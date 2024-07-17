package scalers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

func TestValidateTrafficAutowire(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		err      bool
	}{
		{"", "", false},
		{"false", "false", false},
		{"httproute", "httproute", false},
		{"ingress", "ingress", false},
		{"virtualservice", "virtualservice", false},
		{"httproute,ingress", "httproute,ingress", false},
		{"httproute,ingress,virtualservice", "httproute,ingress,virtualservice", false},
		{"httproute,ingress,virtualservice,false", "", true},
		{"invalid", "", true},
		{"invalid;", "", true},
		{"httproute,invalid", "", true},
		{" Httproute , Ingress ", "httproute,ingress", false},
		{"   ", "", false},
		{"FALSE ", "false", false},
	}

	for _, test := range tests {
		result, err := validateTrafficAutowire(test.input)
		if (err != nil) != test.err {
			t.Errorf("expected error: %v, got: %v", test.err, err)
		}
		if result != test.expected {
			t.Errorf("expected: %s, got: %s", test.expected, result)
		}
	}
}

func TestValidateHealthcheckResponse(t *testing.T) {
	tests := []struct {
		input string
		err   bool
	}{
		{"", false},
		{"passthrough", false},
		{"static", false},
		{"invalid", true},
		{"invalid;", true},
		{"passthrough,static", true},
		{"passthrough,invalid", true},
		{"passthrough;", true},
		{"passthrough,static;", true},
		{"passthrough,static,false", true},
	}

	for _, test := range tests {
		err := validateHealthcheckResponse(test.input)
		if (err != nil) != test.err {
			t.Errorf("expected error: %v, got: %v", test.err, err)
		}
	}
}

type kedifyHttpMetadataTestData struct {
	metadata  map[string]string
	namespace string
	isError   bool
}

var parseKedifyHttpMetadataTestDataset = []kedifyHttpMetadataTestData{
	{map[string]string{"service": "example-service", "port": "8080", "hosts": "example.com", "pathPrefixes": "/api", "scalingMetric": "requestRate", "targetValue": "100", "window": "30s", "granularity": "5s"}, "default", false},
	{map[string]string{"service": "example-service", "port": "8080", "hosts": "example.com", "pathPrefixes": "/api", "scalingMetric": "concurrency", "targetValue": "10"}, "default", false},
	{map[string]string{"service": "example-service", "port": "8080", "hosts": "example.com", "scalingMetric": "requestRate", "targetValue": "100"}, "default", false},
	{map[string]string{"service": "example-service", "port": "8080", "hosts": "example.com", "pathPrefixes": "/api", "scalingMetric": "requestRate, concurrency", "targetValue": "10"}, "default", true},
	{map[string]string{"pathPrefixes": "/api", "scalingMetric": "requestRate", "targetValue": "100"}, "default", true},
	{map[string]string{"service": "example-service", "port": "8080", "hosts": "example.com", "scalingMetric": "unknownMetric"}, "default", true},
	{map[string]string{"service": "example-service", "port": "8080", "hosts": "example.com", "scalingMetric": "requestRate", "targetValue": "100", "window": "invalidDuration", "granularity": "5s"}, "default", true},
	{map[string]string{"service": "example-service", "port": "8080", "hosts": "example.com", "scalingMetric": "requestRate", "targetValue": "100", "window": "30s", "granularity": "invalidDuration"}, "default", true},
	{map[string]string{"port": "8080", "hosts": "example.com", "pathPrefixes": "/api", "scalingMetric": "requestRate", "targetValue": "100"}, "default", true},
	{map[string]string{"service": "example-service", "hosts": "example.com", "pathPrefixes": "/api", "scalingMetric": "requestRate", "targetValue": "100"}, "default", true},
	{map[string]string{"service": "example-service", "port": "invalidPort", "hosts": "example.com", "pathPrefixes": "/api", "scalingMetric": "requestRate", "targetValue": "100"}, "default", true},
}

func TestParseKedifyHttpMetadata(t *testing.T) {
	for _, testData := range parseKedifyHttpMetadataTestDataset {
		_, err := parseKedifyHTTPScalerMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata}, logr.Discard())
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}

func TestKedifyHttpScaler(t *testing.T) {
	s := scheme.Scheme
	s.AddKnownTypes(kedav1alpha1.SchemeGroupVersion, &kedav1alpha1.ScaledObject{})

	client := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(createKedaScaledObject()).Build()
	config := &scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"service":       "example-service",
			"port":          "8080",
			"hosts":         "example.com",
			"pathPrefixes":  "/api",
			"scalingMetric": "requestRate",
			"targetValue":   "100",
			"window":        "30s",
			"granularity":   "5s",
		},
		ScalableObjectName:      "test-scaledobject",
		ScalableObjectNamespace: "default",
		ScalableObjectType:      "ScaledObject",
		TriggerIndex:            0,
	}

	scaler, err := NewKedifyHTTPScaler(context.Background(), client, config)
	if err != nil {
		t.Fatalf("Failed to create kedifyHttpScaler: %v", err)
	}

	if scaler == nil {
		t.Fatalf("Expected scaler to be created, but got nil")
	}

	// Check if HTTPScaledObject is created
	httpScaledObject := &unstructured.Unstructured{}
	httpScaledObject.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "http.keda.sh",
		Version: "v1alpha1",
		Kind:    "HTTPScaledObject",
	})

	// Ensure the HTTPScaledObject is deeply copied correctly
	copyHttpScaledObject := httpScaledObject.DeepCopy()

	err = client.Get(context.Background(), types.NamespacedName{Name: config.ScalableObjectName, Namespace: config.ScalableObjectNamespace}, copyHttpScaledObject)
	if err != nil {
		t.Fatalf("Failed to get HTTPScaledObject: %v", err)
	}

	// Verify HTTPScaledObject fields
	spec, ok := copyHttpScaledObject.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatalf("Failed to get spec from HTTPScaledObject")
	}

	scaleTargetRef, ok := spec["scaleTargetRef"].(map[string]interface{})
	if !ok {
		t.Fatalf("Failed to get scaleTargetRef from HTTPScaledObject spec")
	}

	if scaleTargetRef["name"] != "test-deployment" {
		t.Errorf("Expected ScaleTargetRef.Name to be 'test-deployment', but got '%s'", scaleTargetRef["name"])
	}

	if scaleTargetRef["service"] != "example-service" {
		t.Errorf("Expected ScaleTargetRef.Service to be 'example-service', but got '%s'", scaleTargetRef["service"])
	}

	port, ok := scaleTargetRef["port"].(int64)
	if !ok || port != 8080 {
		t.Errorf("Expected ScaleTargetRef.Port to be 8080, but got %v", scaleTargetRef["port"])
	}

	hosts, ok := spec["hosts"].([]interface{})
	if !ok || len(hosts) != 1 || hosts[0] != "example.com" {
		t.Errorf("Expected Hosts to be ['example.com'], but got %v", hosts)
	}

	scalingMetric, ok := spec["scalingMetric"].(map[string]interface{})
	if !ok {
		t.Fatalf("Failed to get scalingMetric from HTTPScaledObject spec")
	}

	rate, ok := scalingMetric["requestRate"].(map[string]interface{})
	if !ok {
		t.Fatalf("Failed to get rate from scalingMetric")
	}

	targetValue, ok := rate["targetValue"].(int64)
	if !ok || targetValue != 100 {
		t.Errorf("Expected ScalingMetric.Rate.TargetValue to be 100, but got %v", rate["targetValue"])
	}

	window, err := time.ParseDuration(rate["window"].(string))
	if err != nil || window != 30*time.Second {
		t.Errorf("Expected ScalingMetric.Rate.Window to be 30s, but got %v", window)
	}

	granularity, err := time.ParseDuration(rate["granularity"].(string))
	if err != nil || granularity != 5*time.Second {
		t.Errorf("Expected ScalingMetric.Rate.Granularity to be 5s, but got %v", granularity)
	}

	// Verify the annotation is present
	annotations := copyHttpScaledObject.GetAnnotations()
	if val, ok := annotations["httpscaledobject.keda.sh/skip-scaledobject-creation"]; !ok || val != "true" {
		t.Errorf("Expected annotation 'httpscaledobject.keda.sh/skip-scaledobject-creation' to be 'true', but got '%v'", val)
	}

	// Verify the owner reference is set correctly
	ownerReferences := copyHttpScaledObject.GetOwnerReferences()
	if len(ownerReferences) != 1 {
		t.Fatalf("Expected 1 OwnerReference, but got %d", len(ownerReferences))
	}

	ownerRef := ownerReferences[0]
	if ownerRef.Kind != "ScaledObject" {
		t.Errorf("Expected OwnerReference.Kind to be 'ScaledObject', but got '%s'", ownerRef.Kind)
	}

	if ownerRef.Name != "test-scaledobject" {
		t.Errorf("Expected OwnerReference.Name to be 'test-scaledobject', but got '%s'", ownerRef.Name)
	}
}

func createKedaScaledObject() *kedav1alpha1.ScaledObject {
	return &kedav1alpha1.ScaledObject{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-scaledobject",
			Namespace: "default",
		},
		Spec: kedav1alpha1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1alpha1.ScaleTarget{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "test-deployment",
			},
		},
	}
}
