package logger

import (
	"bytes"
	"fmt"
	"log"
	"testing"
)

func TestLogger(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	endpoint := "127.0.0.1:8888"
	logger := NewBasicLogger(DEBUG, endpoint)
	endpointStr := fmt.Sprintf("[%s]:  ", endpoint)
	// Test Debug
	logger.Debug("Debug message should be printed.")
	assertLogOutputContains(t, &buf, endpointStr+"[DEBUG] Debug message should be printed.")

	// Test Debugf
	logger.Debug("Debug message with format: %s", "formatted")
	assertLogOutputContains(t, &buf, endpointStr+"[DEBUG] Debug message with format: formatted")

	// Set log level to INFO
	logger.SetLevel(INFO)

	// Test Info
	logger.Info("Info message should be printed.")
	assertLogOutputContains(t, &buf, endpointStr+"[INFO] Info message should be printed.")

	// Test Infof
	logger.Info("Info message with format: %s", "formatted")
	assertLogOutputContains(t, &buf, endpointStr+"[INFO] Info message with format: formatted")

	// Test Warn
	logger.Warn("Warn message should be printed.")
	assertLogOutputContains(t, &buf, endpointStr+"[WARN] Warn message should be printed.")

	// Test Warnf
	logger.Warn("Warn message with format: %s", "formatted")
	assertLogOutputContains(t, &buf, endpointStr+"[WARN] Warn message with format: formatted")

	// Test Error
	logger.Error("Error message should be printed.")
	assertLogOutputContains(t, &buf, endpointStr+"[ERROR] Error message should be printed.")

	// Test Errorf
	logger.Error("Error message with format: %s", "formatted")
	assertLogOutputContains(t, &buf, endpointStr+"[ERROR] Error message with format: formatted")
}

func assertLogOutputContains(t *testing.T, buf *bytes.Buffer, expected string) {
	t.Helper()
	actual := buf.String()
	if !contains(actual, expected) {
		t.Errorf("Log output does not contain expected string. Expected: %s, Actual: %s", expected, actual)
	}
}

func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}
