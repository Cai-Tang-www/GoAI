package db

import "testing"

func TestNewRejectsNilConfig(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("expected nil config error")
	}
}

func TestMigrateRejectsNilDatabase(t *testing.T) {
	if err := Migrate(nil); err == nil {
		t.Fatal("expected nil database error")
	}
}

func TestCloseAllowsNilDatabase(t *testing.T) {
	if err := Close(nil); err != nil {
		t.Fatalf("close nil database failed: %v", err)
	}
}
