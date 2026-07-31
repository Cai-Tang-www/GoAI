package worker

import "testing"

func TestNewRunWorkerRejectsNilService(t *testing.T) {
	if _, err := NewRunWorker(nil); err == nil {
		t.Fatal("expected nil service error")
	}
}
