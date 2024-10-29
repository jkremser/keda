package scalers

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type kedifyOTELMetadataTestData struct {
	metadata  map[string]string
	namespace string
	isError   bool
}

var parseKedifyOTELMetadataTestDataset = []kedifyOTELMetadataTestData{
	// ok
	{map[string]string{"scalerAddress": "keda-otel-scaler.default.svc:4318", "metricQuery": "avg(app_frontend_requests{target=/api/recommendations, method=GET, status=200})", "operationOverTime": "rate", "targetValue": "1", "clampMax": "600", "clampMin": "0"}, "default", false},
	{map[string]string{"scalerAddress": "keda-otel-scaler.default.svc:4318", "metricQuery": "avg(app_frontend_requests{target=\"/api/recommendations\"})", "targetValue": "3", "clampMax": "200", "clampMin": "0"}, "default", false},
	{map[string]string{"metricQuery": "max(app_frontend_requests{target=/api/recommendations, method=GET, status=200})", "operationOverTime": "min", "targetValue": "1", "clampMax": "600", "clampMin": "0"}, "default", false},
	{map[string]string{"metricQuery": "min(app_frontend_requests{target=/api/recommendations, method=GET, status=302})", "targetValue": "1", "clampMax": "10"}, "default", false},
	{map[string]string{"metricQuery": "sum(app_frontend_requests)", "operationOverTime": "last_one", "targetValue": "1", "clampMin": "0"}, "default", false},
	{map[string]string{"metricQuery": "sum(app_frontend_requests)", "operationOverTime": "count", "targetValue": "1"}, "default", false},
	{map[string]string{"metricQuery": "app_frontend_requests", "targetValue": "5"}, "default", false},

	// not ok
	{map[string]string{"scalerAddress": "keda-otel-scaler.default.svc:4318", "metricQuery": "avg(app_frontend_requests{target=/api/recommendations, method=GET, status=200})", "operationOverTime": "rate", "clampMax": "600", "clampMin": "0"}, "default", true},
	{map[string]string{"scalerAddress": "keda-otel-scaler.default.svc:4318", "targetValue": "3", "clampMax": "200", "clampMin": "0"}, "default", true},
	{map[string]string{"metricQuery": "max(app_frontend_requests{target=/api/recommendations, method=GET, status=200})", "operationOverTime": "minimum", "targetValue": "1", "clampMax": "600", "clampMin": "0"}, "default", true},
	{map[string]string{"metricQuery": "min(app_frontend_requests{target=/api/recommendations, method=GET, status=302})", "operationOverTime": "Rate", "targetValue": "1"}, "default", true},
	{map[string]string{"metricQuery": "", "operationOverTime": "last_one", "targetValue": "1", "clampMin": "0"}, "default", true},
	{map[string]string{"metricQuery": "sum(app_frontend_requests)", "operationOverTime": "count", "targetValue": ""}, "default", true},
	{map[string]string{"metricQuery": "app_frontend_requests", "targetValue": "5", "operationOverTime": ""}, "default", true},
}

func TestParseKedifyOTELMetadata(t *testing.T) {
	for _, testData := range parseKedifyHttpMetadataTestDataset {
		_, err := parseKedifyOTELScalerMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata}, logr.Discard())
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}
