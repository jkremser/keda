package scalers

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

func TestKedifyEnvoyHttpScaler(t *testing.T) {
	s := scheme.Scheme
	s.AddKnownTypes(kedav1alpha1.SchemeGroupVersion, &kedav1alpha1.ScaledObject{})

	client := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(createKedaScaledObjectWithMinReplicas(1)).Build()
	config := &scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"scalingMetric":          "requestRate",
			"targetValue":            "100",
			"window":                 "30s",
			"granularity":            "5s",
			"externalProxyMetricKey": "example-metric-key",
		},
		ScalableObjectName:      "test-scaledobject",
		ScalableObjectNamespace: "default",
		ScalableObjectType:      "ScaledObject",
		TriggerIndex:            0,
	}

	scaler, err := NewKedifyEnvoyHTTPScaler(context.Background(), client, config)
	if err != nil {
		t.Fatalf("Failed to create kedifyEnvoyHttpScaler: %v", err)
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

	err = client.Get(context.Background(), types.NamespacedName{Name: config.ScalableObjectName, Namespace: config.ScalableObjectNamespace}, httpScaledObject)
	if err != nil {
		t.Fatalf("Failed to get HTTPScaledObject: %v", err)
	}

	// Verify HTTPScaledObject fields
	spec, ok := httpScaledObject.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatalf("Failed to get spec from HTTPScaledObject")
	}

	scaleTargetRef, ok := spec["scaleTargetRef"].(map[string]interface{})
	if !ok {
		t.Fatalf("Failed to get scaleTargetRef from HTTPScaledObject spec")
	}

	if scaleTargetRef["service"] != "example-metric-key" {
		t.Errorf("Expected ScaleTargetRef.Service to be 'example-metric-key', but got '%s'", scaleTargetRef["service"])
	}

	hosts, ok := spec["hosts"].([]interface{})
	if !ok || len(hosts) != 1 || hosts[0] != "example-metric-key" {
		t.Errorf("Expected Hosts to be ['example-metric-key'], but got %v", hosts)
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
	annotations := httpScaledObject.GetAnnotations()
	if val, ok := annotations["httpscaledobject.keda.sh/skip-scaledobject-creation"]; !ok || val != "true" {
		t.Errorf("Expected annotation 'httpscaledobject.keda.sh/skip-scaledobject-creation' to be 'true', but got '%v'", val)
	}

	// Verify the owner reference is set correctly
	ownerReferences := httpScaledObject.GetOwnerReferences()
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

func createKedaScaledObjectWithMinReplicas(minReplicas int32) *kedav1alpha1.ScaledObject {
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
			MinReplicaCount: &minReplicas,
		},
	}
}
