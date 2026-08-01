package main

import (
	"reflect"
	"testing"
	"time"
)

func TestHeaderFlagsRejectInvalidValue(t *testing.T) {
	var headers headerFlags
	if err := headers.Set("Authorization"); err == nil {
		t.Fatal("expected invalid header format error")
	}
	if err := headers.Set("X-Trace-ID=trace-test"); err != nil {
		t.Fatalf("set header failed: %v", err)
	}
	if got, want := headers.String(), "X-Trace-ID=trace-test"; got != want {
		t.Fatalf("headers got=%q want=%q", got, want)
	}
}

func TestPercentileUsesSortedNearestIndex(t *testing.T) {
	values := []time.Duration{10, 20, 30, 40}
	for _, test := range []struct {
		ratio float64
		want  time.Duration
	}{
		{ratio: 0, want: 10},
		{ratio: 0.5, want: 20},
		{ratio: 0.95, want: 40},
		{ratio: 1, want: 40},
	} {
		if got := percentile(values, test.ratio); got != test.want {
			t.Errorf("percentile(%v)=%v want=%v", test.ratio, got, test.want)
		}
	}
}

func TestSummarizeCountsSuccessfulStatusesAndFailures(t *testing.T) {
	got := summarize([]result{
		{statusCode: 200, duration: 10},
		{statusCode: 204, duration: 20},
		{statusCode: 500, duration: 30, err: errBenchmarkTest},
	}, 3, 2, 500*time.Millisecond)
	if got.Successes != 2 || got.Failures != 1 {
		t.Fatalf("summary counts got=%+v", got)
	}
	wantStatuses := map[string]int{"200": 1, "204": 1}
	if !reflect.DeepEqual(got.StatusCounts, wantStatuses) {
		t.Fatalf("status counts got=%v want=%v", got.StatusCounts, wantStatuses)
	}
	if got.P50 != 10 || got.P99 != 20 {
		t.Fatalf("latency percentiles got p50=%v p99=%v", got.P50, got.P99)
	}
	if got.Elapsed != 500*time.Millisecond {
		t.Fatalf("elapsed got=%v want=%v", got.Elapsed, 500*time.Millisecond)
	}
	if got.RequestsPerS != 6 {
		t.Fatalf("requests per second got=%v want=6", got.RequestsPerS)
	}
}

var errBenchmarkTest = testingError("request failed")

type testingError string

func (e testingError) Error() string { return string(e) }
