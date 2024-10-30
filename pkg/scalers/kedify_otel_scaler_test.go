package scalers

import (
	"fmt"
	"testing"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type kedifyOTELMetadataTestData struct {
	metadata map[string]string
	isError  bool
}

var parseKedifyOTELMetadataTestDataset = []kedifyOTELMetadataTestData{
	// ok
	{map[string]string{"scalerAddress": "keda-otel-scaler.default.svc:4318", "metricQuery": "avg(app_frontend_requests1{target=/api/recommendations, method=GET, status=200})", "operationOverTime": "rate", "targetValue": "1", "clampMax": "600", "clampMin": "0"}, false},
	{map[string]string{"scalerAddress": "keda-otel-scaler.default.svc:4318", "metricQuery": "avg(app_frontend_requests2{target=\"/api/recommendations\"})", "targetValue": "3", "clampMax": "200", "clampMin": "0"}, false},
	{map[string]string{"metricQuery": "max(app_frontend_requests3{target=/api/recommendations, method=GET, status=200})", "operationOverTime": "min", "targetValue": "1", "clampMax": "600", "clampMin": "0"}, false},
	{map[string]string{"metricQuery": "min(app_frontend_requests4{target=/api/recommendations, method=GET, status=302})", "targetValue": "1", "clampMax": "10"}, false},
	{map[string]string{"metricQuery": "sum(app_frontend_requests5)", "operationOverTime": "last_one", "targetValue": "1", "clampMin": "0"}, false},
	{map[string]string{"metricQuery": "sum(app_frontend_requests6)", "operationOverTime": "count", "targetValue": "1"}, false},
	{map[string]string{"metricQuery": "app_frontend_requests7", "targetValue": "5"}, false},

	// not ok
	// missing targetValue
	{map[string]string{"scalerAddress": "keda-otel-scaler.default.svc:4318", "metricQuery": "avg(app_frontend_requests8{target=/api/recommendations, method=GET, status=200})", "operationOverTime": "rate", "clampMax": "600", "clampMin": "0"}, true},
	// missing metricQuery
	{map[string]string{"scalerAddress": "keda-otel-scaler.default.svc:4318", "targetValue": "3", "clampMax": "200", "clampMin": "0"}, true},
	// unknown operationOverTime
	{map[string]string{"metricQuery": "max(app_frontend_requests9{target=/api/recommendations, method=GET, status=200})", "operationOverTime": "minimum", "targetValue": "1", "clampMax": "600", "clampMin": "0"}, true},
	// typo in operationOverTime (case sensitive)
	{map[string]string{"metricQuery": "min(app_frontend_requests41{target=/api/recommendations, method=GET, status=302})", "operationOverTime": "Rate", "targetValue": "1"}, true},
	// empty metricQuery
	{map[string]string{"metricQuery": "", "operationOverTime": "last_one", "targetValue": "1", "clampMin": "0"}, true},
	// empty targetValue
	{map[string]string{"metricQuery": "sum(app_frontend_requests11)", "operationOverTime": "count", "targetValue": ""}, true},
	// targetValue NaN
	{map[string]string{"metricQuery": "sum(app_frontend_requests12)", "operationOverTime": "count", "targetValue": "hello"}, true},
	// empty operationOverTime
	{map[string]string{"metricQuery": "app_frontend_requests13", "targetValue": "5", "operationOverTime": ""}, true},
	// typo in targetValue
	{map[string]string{"metricQuery": "app_frontend_requests14", "targetValue": "a5"}, true},
	// typo in clampMin
	{map[string]string{"metricQuery": "hello", "targetValue": "1", "clampMin": "0a"}, true},
	// typo in clampMax
	{map[string]string{"metricQuery": "42", "targetValue": "1", "clampMax": "0a"}, true},
	// empty
	{map[string]string{}, true},
}

func TestParseKedifyOTELMetadataErrors(t *testing.T) {
	for _, testData := range parseKedifyOTELMetadataTestDataset {
		_, err := parseKedifyOTELScalerMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata})
		if err != nil && !testData.isError {
			t.Errorf("Expected success but got error: %v, testData: %+v", err, testData.metadata)
		}
		if testData.isError && err == nil {
			t.Errorf("Expected error but got success, testData: %+v", testData.metadata)
		}
	}
}

func TestParseKedifyOTELMetadata(t *testing.T) {
	for _, testData := range parseKedifyOTELMetadataTestDataset {
		if !testData.isError {
			parsedMeta, err := parseKedifyOTELScalerMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata})
			if err == nil {
				assertHelper(t, testData.metadata, parsedMeta)
			}
		}
	}
}

func assertHelper(t *testing.T, expected map[string]string, actual kedifyOTELScalerMetadata) {
	if expected["targetValue"] != fmt.Sprint(actual.targetValue) {
		t.Errorf("      got: %v", actual.targetValue)
		t.Errorf(" expected: %v", expected["targetValue"])
		t.Errorf("test data: %+v", expected)
	}
	if expected["scalerAddress"] != actual.scalerAddress {
		t.Errorf("      got: %v", actual.scalerAddress)
		t.Errorf(" expected: %v", expected["scalerAddress"])
		t.Errorf("test data: %+v", expected)
	}
	if expected["metricQuery"] != actual.metricQuery {
		t.Errorf("      got: %v", actual.metricQuery)
		t.Errorf(" expected: %v", expected["metricQuery"])
		t.Errorf("test data: %+v", expected)
	}
	if expected["operationOverTime"] != actual.operationOverTime {
		t.Errorf("      got: %v", actual.operationOverTime)
		t.Errorf(" expected: %v", expected["operationOverTime"])
		t.Errorf("test data: %+v", expected)
	}
	if expected["scalerAddress"] != actual.scalerAddress {
		t.Errorf("      got: %v", actual.scalerAddress)
		t.Errorf(" expected: %v", expected["scalerAddress"])
		t.Errorf("test data: %+v", expected)
	}

	if expected["clampMin"] != actual.clampMin {
		t.Errorf("      got: %v", actual.clampMin)
		t.Errorf(" expected: %v", expected["clampMin"])
		t.Errorf("test data: %+v", expected)
	}
	if expected["clampMax"] != actual.clampMax {
		t.Errorf("      got: %v", actual.clampMax)
		t.Errorf(" expected: %v", expected["clampMax"])
		t.Errorf("test data: %+v", expected)
	}
}
