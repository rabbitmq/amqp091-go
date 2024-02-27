package amqp091

import (
	"testing"
	"time"
)

func TestNewError(t *testing.T) {
	testCases := []struct {
		code           uint16
		text           string
		expectedServer bool
	}{
		// Just three basics samples
		{404, "Not Found", true},
		{500, "Internal Server Error", true},
		{403, "Forbidden", true},
	}

	for _, tc := range testCases {
		err := newError(tc.code, tc.text)
		if err.Code != int(tc.code) {
			t.Errorf("expected Code %d, got %d", tc.code, err.Code)
		}
		if err.Reason != tc.text {
			t.Errorf("expected Reason %s, got %s", tc.text, err.Reason)
		}
		if err.Server != tc.expectedServer {
			t.Errorf("expected Server to be %v", tc.expectedServer)
		}
	}
}

func TestValidateField(t *testing.T) {
	// Test case for simple types
	simpleTypes := []interface{}{
		nil, true, byte(1), int8(1), 10, int16(10), int32(10), int64(10),
		float32(1.0), float64(1.0), "string", []byte("byte slice"),
		Decimal{Scale: 2, Value: 12345}, time.Now(),
	}
	for _, v := range simpleTypes {
		if err := validateField(v); err != nil {
			t.Errorf("validateField failed for simple type %T: %s", v, err)
		}
	}

	// Test case for []interface{}
	sliceTypes := []interface{}{
		"string", 10, float64(1.0), Decimal{Scale: 2, Value: 12345},
	}
	if err := validateField(sliceTypes); err != nil {
		t.Errorf("validateField failed for []interface{}: %s", err)
	}

	// Test case for Table
	tableType := Table{
		"key1": "value1",
		"key2": 10,
		"key3": []interface{}{"nested string", 20},
	}
	if err := validateField(tableType); err != nil {
		t.Errorf("validateField failed for Table: %s", err)
	}

	// Test case for unsupported type
	unsupportedType := struct{}{}
	if err := validateField(unsupportedType); err == nil {
		t.Error("validateField should fail for unsupported type but it didn't")
	}
}
