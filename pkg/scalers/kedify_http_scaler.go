package scalers

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

// This scaler is responsible for creating and managing the HTTPScaledObject based on the input metadata and ScaledObject configuration
// Internally it uses ExternalPushScaler to connect with HTTP Addon and get the metrics

type kedifyHttpScalerScalingMetric string

const (
	requestRate kedifyHttpScalerScalingMetric = "requestRate"
	concurrency kedifyHttpScalerScalingMetric = "concurrency"
)

type kedifyHttpScaler struct {
	externalPushScaler

	kubeClient client.Client
	metadata   kedifyHttpScalerMetadata
}

type kedifyHttpScalerMetadata struct {
	service                string
	port                   int32
	hosts                  []string
	pathPrefixes           []string
	scalingMetric          kedifyHttpScalerScalingMetric
	targetValue            *int
	granularity            *v1.Duration
	window                 *v1.Duration
	trafficAutowire        string
	externalProxyMetricKey string

	// healthcheck related fields
	healthcheckPath     string
	healthcheckResponse string
}

// externalProxyMetricKeyAnnotation is the annotation for pairing external metrics in the interceptor
const externalProxyMetricKeyAnnotation = "http.kedify.io/external-proxy-metric-key"

// TODO - this should be more smart and dynamic, now it tries to connect to the same namespace and hardcoded port & name
var httpScalerAddress = fmt.Sprintf("keda-add-ons-http-external-scaler.%s:9090", kedautil.GetPodNamespace())

// NewKedifyHTTPScaler creates ExternalPushScaler for connecting with HTTP Addon, it also creates HTTPScaledObject based on the input metadata
func NewKedifyHTTPScaler(ctx context.Context, kubeClient client.Client, config *scalersconfig.ScalerConfig) (PushScaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting external scaler metric type: %w", err)
	}

	logger := InitializeLogger(config, "kedify_http_scaler")

	if config.ScalableObjectType != "ScaledObject" {
		e := fmt.Errorf("'kedify-http' scaler only support ScaledObject, invalid scalable object type: %s", config.ScalableObjectType)
		return nil, e
	}

	meta, err := parseKedifyHTTPScalerMetadata(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing external scaler metadata: %w", err)
	}

	scaledObject := &kedav1alpha1.ScaledObject{}
	err = kubeClient.Get(ctx, types.NamespacedName{Name: config.ScalableObjectName, Namespace: config.ScalableObjectNamespace}, scaledObject)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Error(err, "ScaledObject not found", "scaledObjectName", config.ScalableObjectName, "scaledObjectNamespace", config.ScalableObjectNamespace)
		}
		return nil, err
	}

	err = ensureHTTPScaledObjectExists(ctx, kubeClient, config, scaledObject, meta, logger)
	if err != nil {
		return nil, err
	}

	origMetadata := map[string]string{
		"scalerAddress":    httpScalerAddress,
		"httpScaledObject": config.ScalableObjectName,
	}

	return &kedifyHttpScaler{
		metadata: meta,
		externalPushScaler: externalPushScaler{
			externalScaler{
				metricType: metricType,
				metadata: externalScalerMetadata{
					scalerAddress:    httpScalerAddress,
					originalMetadata: origMetadata,
					triggerIndex:     config.TriggerIndex,
				},
				scaledObjectRef: pb.ScaledObjectRef{
					Name:           config.ScalableObjectName,
					Namespace:      config.ScalableObjectNamespace,
					ScalerMetadata: origMetadata,
				},
				logger: logger,
			},
		},
		kubeClient: kubeClient,
	}, nil
}

func (s *kedifyHttpScaler) Close(context.Context) error {
	return nil
}

// GetMetricSpecForScaling returns the metric spec for the HPA
func (s *kedifyHttpScaler) GetMetricSpecForScaling(ctx context.Context) []v2.MetricSpec {
	return s.externalPushScaler.GetMetricSpecForScaling(ctx)
}

func (s *kedifyHttpScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	return s.externalPushScaler.GetMetricsAndActivity(ctx, metricName)
}

func (s *kedifyHttpScaler) Run(ctx context.Context, active chan<- bool) {
	s.externalPushScaler.Run(ctx, active)
}

// parseKedifyHTTPScalerMetadata parses the input metadata and returns the kedifyHttpScalerMetadata
func parseKedifyHTTPScalerMetadata(config *scalersconfig.ScalerConfig, logger logr.Logger) (kedifyHttpScalerMetadata, error) {
	meta := kedifyHttpScalerMetadata{}

	if val, ok := config.TriggerMetadata["service"]; ok && val != "" {
		meta.service = val
	} else {
		return meta, fmt.Errorf("service is a required field")
	}

	if val, ok := config.TriggerMetadata["port"]; ok {
		tv, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return meta, fmt.Errorf("invalid port - must be an integer")
		}
		meta.port = int32(tv)
	} else {
		return meta, fmt.Errorf("port is a required field")
	}

	if val, ok := config.TriggerMetadata["hosts"]; ok && val != "" {
		meta.hosts = strings.Split(config.TriggerMetadata["hosts"], ",")
	} else {
		return meta, fmt.Errorf("hosts is a required field")
	}

	// Set default pathPrefixes to "/" to prevent problems during an update of SO.
	// If the value of pathPrefixes is not set in the SO, an update on the SO
	// causes the pathPrefixes be added to HSO as [] which is not allowed.
	// This is probably a bug in the Unstructured resources handling.
	meta.pathPrefixes = []string{"/"}
	if val, ok := config.TriggerMetadata["pathPrefixes"]; ok && val != "" {
		meta.pathPrefixes = strings.Split(config.TriggerMetadata["pathPrefixes"], ",")
	}

	if config.TriggerMetadata["scalingMetric"] != "" {
		mode := kedifyHttpScalerScalingMetric(config.TriggerMetadata["scalingMetric"])
		if mode != requestRate && mode != concurrency {
			return meta, fmt.Errorf("err unkonwn scalingMetric type %q given", mode)
		}
		meta.scalingMetric = mode
	} else {
		return meta, fmt.Errorf("scalingMetric is a required field")
	}

	if val, ok := config.TriggerMetadata["targetValue"]; ok {
		tv, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return meta, fmt.Errorf("invalid targetValue - must be an integer")
		}
		targeValue := int(tv)
		meta.targetValue = &targeValue
	}

	if val, ok := config.TriggerMetadata["window"]; ok {
		if meta.scalingMetric == requestRate {
			duration, err := time.ParseDuration(val)
			if err != nil {
				return meta, fmt.Errorf("invalid window - must be a duration string")
			}
			meta.window = &v1.Duration{Duration: duration}
		} else {
			logger.Info("window is not supported for concurrency scaling, ignoring the value")
		}
	}

	if val, ok := config.TriggerMetadata["granularity"]; ok {
		if meta.scalingMetric == requestRate {
			duration, err := time.ParseDuration(val)
			if err != nil {
				return meta, fmt.Errorf("invalid granularity - must be a duration string")
			}
			meta.granularity = &v1.Duration{Duration: duration}
		} else {
			logger.Info("granularity is not supported for concurrency scaling, ignoring the value")
		}
	}
	meta.externalProxyMetricKey = config.TriggerMetadata["externalProxyMetricKey"]

	if val, ok := config.TriggerMetadata["trafficAutowire"]; ok {
		autowiring, err := validateTrafficAutowire(val)
		if err != nil {
			return meta, err
		}
		meta.trafficAutowire = autowiring
	}

	if val, ok := config.TriggerMetadata["healthcheckPath"]; ok {
		meta.healthcheckPath = val
	}

	if val, ok := config.TriggerMetadata["healthcheckResponse"]; ok {
		if meta.healthcheckPath != "" {
			err := validateHealthcheckResponse(val)
			if err != nil {
				return meta, err
			}
			meta.healthcheckResponse = val
		} else {
			return meta, fmt.Errorf("healthcheckResponse is a required field when healthcheckPath is set")
		}
	}

	return meta, nil
}

// ensureHTTPScaledObjectExists creates or updates HTTPScaledObject based on the input metadata and ScaledObject configuration
func ensureHTTPScaledObjectExists(ctx context.Context, kubeClient client.Client, config *scalersconfig.ScalerConfig, scaledObject *kedav1alpha1.ScaledObject, meta kedifyHttpScalerMetadata, logger logr.Logger) error {
	scalingMetric := map[string]interface{}{}
	if meta.scalingMetric == requestRate {
		scalingMetric["requestRate"] = map[string]interface{}{
			"targetValue": int64(getIntOrDefault(meta.targetValue, 100)),
			"window":      getDurationOrDefault(meta.window, 1*time.Minute).ToUnstructured(),
			"granularity": getDurationOrDefault(meta.granularity, 1*time.Second).ToUnstructured(),
		}
	} else if meta.scalingMetric == concurrency {
		scalingMetric["concurrency"] = map[string]interface{}{
			"targetValue": int64(getIntOrDefault(meta.targetValue, 0)),
		}
	} else {
		return fmt.Errorf("unknown scalingMetric type %q given", meta.scalingMetric)
	}

	ann := scaledObject.GetAnnotations()
	if ann == nil {
		ann = make(map[string]string)
	}
	ann["httpscaledobject.keda.sh/skip-scaledobject-creation"] = "true"
	if meta.externalProxyMetricKey != "" {
		ann[externalProxyMetricKeyAnnotation] = meta.externalProxyMetricKey
	} else {
		delete(ann, externalProxyMetricKeyAnnotation)
	}
	if meta.trafficAutowire != "" {
		ann["http.kedify.io/traffic-autowire"] = meta.trafficAutowire
	} else {
		delete(ann, "http.kedify.io/traffic-autowire")
	}

	if meta.healthcheckPath != "" {
		ann["http.kedify.io/healthcheck-path"] = meta.healthcheckPath
	} else {
		delete(ann, "http.kedify.io/healthcheck-path")
	}
	if meta.healthcheckResponse != "" {
		ann["http.kedify.io/healthcheck-response"] = meta.healthcheckResponse
	} else {
		delete(ann, "http.kedify.io/healthcheck-response")
	}

	httpScaledObject := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "http.keda.sh/v1alpha1",
			"kind":       "HTTPScaledObject",
			"metadata": map[string]interface{}{
				"name":      config.ScalableObjectName,
				"namespace": config.ScalableObjectNamespace,
			},
			"spec": map[string]interface{}{
				"scaleTargetRef": map[string]interface{}{
					"apiVersion": scaledObject.Spec.ScaleTargetRef.APIVersion,
					"kind":       scaledObject.Spec.ScaleTargetRef.Kind,
					"name":       scaledObject.Spec.ScaleTargetRef.Name,
					"service":    meta.service,
					"port":       int64(meta.port),
				},
				"scalingMetric": scalingMetric,
			},
		},
	}
	if scaledObject.Spec.MinReplicaCount != nil {
		unstructured.SetNestedField(httpScaledObject.Object, int64(*scaledObject.Spec.MinReplicaCount), "spec", "replicas", "min")
	}
	if scaledObject.Spec.MaxReplicaCount != nil {
		unstructured.SetNestedField(httpScaledObject.Object, int64(*scaledObject.Spec.MaxReplicaCount), "spec", "replicas", "max")
	}
	unstructured.SetNestedStringMap(httpScaledObject.Object, scaledObject.GetLabels(), "metadata", "labels")
	unstructured.SetNestedStringMap(httpScaledObject.Object, ann, "metadata", "annotations")
	unstructured.SetNestedStringSlice(httpScaledObject.Object, meta.hosts, "spec", "hosts")
	unstructured.SetNestedStringSlice(httpScaledObject.Object, meta.pathPrefixes, "spec", "pathPrefixes")

	err := controllerutil.SetControllerReference(scaledObject, httpScaledObject, kubeClient.Scheme())
	if err != nil {
		return fmt.Errorf("error setting controller reference: %w", err)
	}

	err = kubeClient.Create(ctx, httpScaledObject)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			existingHttpScaledObject := &unstructured.Unstructured{}
			existingHttpScaledObject.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "http.keda.sh",
				Version: "v1alpha1",
				Kind:    "HTTPScaledObject",
			})
			err = kubeClient.Get(ctx, types.NamespacedName{Name: config.ScalableObjectName, Namespace: config.ScalableObjectNamespace}, existingHttpScaledObject)
			if err != nil {
				return fmt.Errorf("error getting existing HTTPScaledObject: %w", err)
			}

			existingHttpScaledObject.Object["spec"] = httpScaledObject.Object["spec"]
			existingHttpScaledObject.SetAnnotations(ann)
			existingHttpScaledObject.SetLabels(httpScaledObject.GetLabels())
			existingHttpScaledObject.SetOwnerReferences(httpScaledObject.GetOwnerReferences())

			err = kubeClient.Update(ctx, existingHttpScaledObject)
			if err != nil {
				return fmt.Errorf("error updating existing HTTPScaledObject: %w", err)
			}
		} else {
			logger.Error(err, "error creating HTTPScaledObject", "httpScaledObjectName", config.ScalableObjectName, "httpScaledObjectNamespace", config.ScalableObjectNamespace)
			return err
		}
	}

	return nil
}

// getIntOrDefault returns the default value if the input value is nil
func getIntOrDefault(val *int, defaultVal int) int {
	if val == nil {
		return defaultVal
	}
	return *val
}

// getDurationOrDefault returns the default value if the input value is nil
func getDurationOrDefault(val *v1.Duration, defaultVal time.Duration) v1.Duration {
	if val == nil {
		return v1.Duration{Duration: defaultVal}
	}
	return *val
}

// validateTrafficAutowire validates the trafficAutowire value
// allowed values are "", "false" or any combination of following values "httproute", "ingress", "virtualservice"
func validateTrafficAutowire(value string) (string, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" || value == "false" {
		return value, nil
	}
	validValues := map[string]bool{"httproute": true, "ingress": true, "virtualservice": true}
	values := strings.Split(value, ",")
	for i, v := range values {
		v = strings.TrimSpace(v)
		if !validValues[v] {
			return "", fmt.Errorf("invalid autowiring value " + v + " given, valid values are httproute,ingress,virtualservice")
		}
		values[i] = v
	}
	return strings.Join(values, ","), nil
}

// validateHealthcheckResponse validates the healthcheck response
// the only valid values are "", "passthrough" and "static"
func validateHealthcheckResponse(value string) error {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" || value == "passthrough" || value == "static" {
		return nil
	}
	return fmt.Errorf("invalid healthcheckResponse value %q given, valid values are passthrough, static", value)
}
