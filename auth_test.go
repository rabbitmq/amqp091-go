package amqp091

import "testing"

func TestPlainAuth(t *testing.T) {
	auth := &PlainAuth{
		Username: "user",
		Password: "pass",
	}

	if auth.Mechanism() != "PLAIN" {
		t.Errorf("Expected PLAIN, got %s", auth.Mechanism())
	}

	expectedResponse := "\000user\000pass"
	if auth.Response() != expectedResponse {
		t.Errorf("Expected %s, got %s", expectedResponse, auth.Response())
	}
}

func TestExternalAuth(t *testing.T) {
	auth := &ExternalAuth{}

	if auth.Mechanism() != "EXTERNAL" {
		t.Errorf("Expected EXTERNAL, got %s", auth.Mechanism())
	}

	if auth.Response() != "\000*\000*" {
		t.Errorf("Expected \000*\000*, got %s", auth.Response())
	}
}
