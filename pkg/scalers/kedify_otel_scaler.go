package scalers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// This scaler uses ExternalPushScaler to connect with OTEL Addon to get the metrics

type operationOverTime string

const (
	opLastOne operationOverTime = "last_one"
	opRate    operationOverTime = "rate"
	opCount   operationOverTime = "count"
	opAvg     operationOverTime = "avg"
	opMin     operationOverTime = "min"
	opMax     operationOverTime = "max"
)

type kedifyOTELScaler struct {
	externalPushScaler

	kubeClient client.Client
	metadata   kedifyOTELScalerMetadata
}

type kedifyOTELScalerMetadata struct {
	scalerAddress     string `keda:"name=scalerAddress,     order=triggerMetadata, optional"`
	metricQuery       string `keda:"name=metricQuery,       order=triggerMetadata"`
	targetValue       int    `keda:"name=targetValue,       order=triggerMetadata"`
	clampMin          string `keda:"name=clampMin,          order=triggerMetadata, optional"`
	clampMax          string `keda:"name=clampMax,          order=triggerMetadata, optional"`
	operationOverTime string `keda:"name=operationOverTime, order=triggerMetadata, optional, enum=last_one;rate;count;avg;min;max"`
}

// NewKedifyOTELScaler creates ExternalPushScaler for connecting with OTEL Addon
func NewKedifyOTELScaler(ctx context.Context, kubeClient client.Client, config *scalersconfig.ScalerConfig) (PushScaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting external scaler metric type: %w", err)
	}

	logger := InitializeLogger(config, "kedify_otel_scaler")

	if config.ScalableObjectType != "ScaledObject" {
		e := fmt.Errorf("'kedify-otel' scaler only support ScaledObject, invalid scalable object type: %s", config.ScalableObjectType)
		return nil, e
	}

	// otelScalerAddress is a fallback address that will be used if no scalerAddress is specified in trigger metadata
	var otelScalerAddress = fmt.Sprintf("keda-otel-scaler.%s.svc:4318", kedautil.GetPodNamespace())
	meta, err := parseKedifyOTELScalerMetadata(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing external scaler metadata: %w", err)
	}
	if len(meta.scalerAddress) != 0 {
		otelScalerAddress = meta.scalerAddress
	}

	scaledObject := &kedav1alpha1.ScaledObject{}
	err = kubeClient.Get(ctx, types.NamespacedName{Name: config.ScalableObjectName, Namespace: config.ScalableObjectNamespace}, scaledObject)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Error(err, "ScaledObject not found", "scaledObjectName", config.ScalableObjectName, "scaledObjectNamespace", config.ScalableObjectNamespace)
		}
		return nil, err
	}

	origMetadata := map[string]string{
		"scalerAddress": otelScalerAddress,
	}

	return &kedifyOTELScaler{
		metadata: *meta,
		externalPushScaler: externalPushScaler{
			externalScaler{
				metricType: metricType,
				metadata: externalScalerMetadata{
					scalerAddress:    otelScalerAddress,
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

func (s *kedifyOTELScaler) Close(context.Context) error {
	return nil
}

// GetMetricSpecForScaling returns the metric spec for the HPA
func (s *kedifyOTELScaler) GetMetricSpecForScaling(ctx context.Context) []v2.MetricSpec {
	return s.externalPushScaler.GetMetricSpecForScaling(ctx)
}

func (s *kedifyOTELScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	metrics, active, err := s.externalPushScaler.GetMetricsAndActivity(ctx, metricName)
	for _, m := range metrics {
		if m.Value.Value() < 0 {
			return metrics, false, fmt.Errorf("OTEL add-on is not ok")
		}
	}
	return metrics, active, err
}

func (s *kedifyOTELScaler) Run(ctx context.Context, active chan<- bool) {
	s.externalPushScaler.Run(ctx, active)
}

// parseKedifyOTELScalerMetadata parses the input metadata and returns the kedifyOTELScalerMetadata
func parseKedifyOTELScalerMetadata(config *scalersconfig.ScalerConfig, logger logr.Logger) (*kedifyOTELScalerMetadata, error) {
	meta := &kedifyOTELScalerMetadata{}
	if err := config.TypedConfig(meta); err != nil {
		return meta, fmt.Errorf("error parsing SO's metadata: %w", err)
	}

	//if val, ok := config.TriggerMetadata["clampMin"]; ok && val != "" {
	//	valParsed, err := strconv.ParseInt(val, 10, 64)
	//	if err != nil {
	//		return meta, fmt.Errorf("invalid clampMin value - must be an integer")
	//	}
	//	meta.clampMin = string(valParsed)
	//}
	//if val, ok := config.TriggerMetadata["clampMax"]; ok && val != "" {
	//	valParsed, err := strconv.ParseInt(val, 10, 64)
	//	if err != nil {
	//		return meta, fmt.Errorf("invalid clampMax value - must be an integer")
	//	}
	//	meta.clampMax = string(valParsed)
	//}
	//if val, ok := config.TriggerMetadata["operationOverTime"]; ok && val != "" {
	//	if err := CheckTimeOp(operationOverTime(val)); err != nil {
	//		return meta, err
	//	}
	//	meta.metricQuery = val
	//}

	//if val, ok := config.TriggerMetadata["targetValue"]; ok {
	//	tv, err := strconv.ParseInt(val, 10, 64)
	//	if err != nil {
	//		return meta, fmt.Errorf("invalid targetValue - must be an integer")
	//	}
	//	targeValue := int(tv)
	//	meta.targetValue = &targeValue
	//}

	return meta, nil
}

//func CheckTimeOp(op operationOverTime) error {
//	switch op {
//	case opLastOne, opRate, opCount, opAvg, opMin, opMax:
//		return nil
//	default:
//		return fmt.Errorf("unknown operationOverTime:%s", op)
//	}
//}
