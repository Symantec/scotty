package kafka

// TODO: If optiopay/kafka accepts pull request, remove this file.

import (
	"errors"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	err := errors.New("An error")
	backoff := newBackOffType(10*time.Second, 1.5)
	if out := backoff.WaitTime(nil); out != 0 {
		t.Fatalf("Expected 0 wait time, got %v", out)
	}
	if out := backoff.WaitTime(err); out != 10*time.Second {
		t.Fatalf("Expeccted %v, got %v", 10*time.Second, out)
	}
	if out := backoff.WaitTime(err); out != 15*time.Second {
		t.Fatalf("Expeccted %v, got %v", 15*time.Second, out)
	}
	if out := backoff.WaitTime(err); out != 22*time.Second+500*time.Millisecond {
		t.Fatalf("Expeccted %v, got %v", 22*time.Second+500*time.Millisecond, out)
	}
	if out := backoff.WaitTime(nil); out != 0 {
		t.Fatalf("Expected 0 wait time, got %v", out)
	}
	if out := backoff.WaitTime(err); out != 10*time.Second {
		t.Fatalf("Expeccted %v, got %v", 10*time.Second, out)
	}
}
