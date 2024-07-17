package scalers

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

// This scaler is responsible for creating and managing the HTTPScaledObject based on the input metadata and ScaledObject configuration
// Internally it uses ExternalPushScaler to connect with HTTP Addon and get the metrics
//
// The difference between kedify-envoy-http and kedify-http is that kedify-envoy-http uses custom Envoy proxy to expose the metrics.
//
// !!!!!
// Because this scaler just gets metrics from Envoy to drive the scaling, we cannot scale to zero with this scaler - Envoy can't hold the traffic during the scale to zero.
// !!!!!

type kedifyEnvoyHttpScaler struct {
	externalPushScaler

	kubeClient client.Client
	metadata   kedifyHttpScalerMetadata
}

// NewKedifyHTTPScaler creates ExternalPushScaler for connecting with HTTP Addon, it also creates HTTPScaledObject based on the input metadata
func NewKedifyEnvoyHTTPScaler(ctx context.Context, kubeClient client.Client, config *scalersconfig.ScalerConfig) (PushScaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting external scaler metric type: %w", err)
	}

	logger := InitializeLogger(config, "kedify_envoy_http_scaler")

	if config.ScalableObjectType != "ScaledObject" {
		e := fmt.Errorf("'kedify-envoy-http' scaler only support ScaledObject, invalid scalable object type: %s", config.ScalableObjectType)
		return nil, e
	}

	meta, err := parseKedifyEnvoyHTTPScalerMetadata(config, logger)
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

	// scale to zero will not work for this trigger type, let's check if minReplicaCount is > 0
	if scaledObject.Spec.MinReplicaCount == nil || *scaledObject.Spec.MinReplicaCount < 1 {
		err := fmt.Errorf("minReplicaCount is required and must be greater than 0 for trigger type 'kedify-envoy-http'")
		logger.Error(err, "ScaledObject is not properly configured for trigger type 'kedify-envoy-http'")
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

	return &kedifyEnvoyHttpScaler{
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

func (s *kedifyEnvoyHttpScaler) Close(context.Context) error {
	return nil
}

// GetMetricSpecForScaling returns the metric spec for the HPA
func (s *kedifyEnvoyHttpScaler) GetMetricSpecForScaling(ctx context.Context) []v2.MetricSpec {
	return s.externalPushScaler.GetMetricSpecForScaling(ctx)
}

func (s *kedifyEnvoyHttpScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	return s.externalPushScaler.GetMetricsAndActivity(ctx, metricName)
}

func (s *kedifyEnvoyHttpScaler) Run(ctx context.Context, active chan<- bool) {
	s.externalPushScaler.Run(ctx, active)
}

// parseKedifyEnvoyHTTPScalerMetadata parses the input metadata and returns the kedifyHttpScalerMetadata
func parseKedifyEnvoyHTTPScalerMetadata(config *scalersconfig.ScalerConfig, logger logr.Logger) (kedifyHttpScalerMetadata, error) {
	meta := kedifyHttpScalerMetadata{}

	// we disable autowiring by default for Kedify Envoy HTTP scaler
	meta.trafficAutowire = "false"

	if val, ok := config.TriggerMetadata["externalProxyMetricKey"]; ok {
		meta.externalProxyMetricKey = val
	} else {
		return meta, fmt.Errorf("externalProxyMetricKey is a required field")
	}

	// we need to set mandatory fields on HTTPScaledObject, these are not used so we can set them to envoy_"externalProxyMetricKey"
	key := fmt.Sprintf("envoy_%s", meta.externalProxyMetricKey)
	meta.hosts = []string{key}
	meta.service = key
	meta.port = 0

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

	return meta, nil
}
