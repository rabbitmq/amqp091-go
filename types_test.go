package amqp091

import (
	"fmt"
	"testing"
	"time"
)

func TestNewError(t *testing.T) {
	testCases := []struct {
		code           uint16
		text           string
		expectedServer bool
		recoverable    bool
		retriable      bool
	}{
		// Just three basics samples
		{404, "Not Found", true, true, false},
		{500, "Internal Server Error", true, false, false},
		{403, "Forbidden", true, true, false},
		{311, "Content Too Large", true, true, true},
	}

	for _, tc := range testCases {
		aerr := newError(tc.code, tc.text)
		if aerr.Code != int(tc.code) {
			t.Errorf("expected Code %d, got %d", tc.code, aerr.Code)
		}
		if aerr.Reason != tc.text {
			t.Errorf("expected Reason %s, got %s", tc.text, aerr.Reason)
		}
		if aerr.Server != tc.expectedServer {
			t.Errorf("expected Server to be %v", tc.expectedServer)
		}
		if aerr.Recover != tc.recoverable {
			t.Errorf("expected Recover to be %v", tc.recoverable)
		}

		if isTemporaryError := aerr.Temporary(); isTemporaryError != tc.recoverable {
			t.Errorf("expected err to be temporary %v", tc.recoverable)
		}

		if isRetriable := aerr.Retryable(); isRetriable != tc.retriable {
			t.Errorf("expected err to be retriable %v", tc.recoverable)
		}
	}
}

func TestNewErrorMessage(t *testing.T) {
	var err error = newError(404, "Not Found")

	expected := `Exception (404) Reason: "Not Found"`

	if got := err.Error(); expected != got {
		t.Errorf("expected Error %q, got %q", expected, got)
	}

	expected = `Exception=404, Reason="Not Found", Recover=true, Server=true`

	if got := fmt.Sprintf("%#v", err); expected != got {
		t.Errorf("expected go string %q, got %q", expected, got)
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
