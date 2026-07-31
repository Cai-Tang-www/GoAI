package redis

import "testing"

func TestNewRejectsNilConfig(t *testing.T) {
	if _, err := New(nil, nil); err == nil {
		t.Fatal("expected nil config error")
	}
}

func TestCloseAllowsNilClient(t *testing.T) {
	if err := Close(nil); err != nil {
		t.Fatalf("close nil client failed: %v", err)
	}
}
