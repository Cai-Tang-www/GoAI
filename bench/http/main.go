package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// headerFlags collects repeatable -header name=value flags for the benchmark client.
type headerFlags []string

func (h *headerFlags) String() string { return strings.Join(*h, ",") }

func (h *headerFlags) Set(value string) error {
	name, _, ok := strings.Cut(value, "=")
	if !ok || strings.TrimSpace(name) == "" {
		return errors.New("header must use name=value format")
	}
	*h = append(*h, value)
	return nil
}

type result struct {
	statusCode int
	duration   time.Duration
	err        error
}

type summary struct {
	Requests     int            `json:"requests"`
	Concurrency  int            `json:"concurrency"`
	Successes    int            `json:"successes"`
	Failures     int            `json:"failures"`
	StatusCounts map[string]int `json:"status_counts"`
	Elapsed      time.Duration  `json:"elapsed_ns"`
	RequestsPerS float64        `json:"requests_per_second"`
	P50          time.Duration  `json:"p50_ns"`
	P95          time.Duration  `json:"p95_ns"`
	P99          time.Duration  `json:"p99_ns"`
}

func main() {
	var headers headerFlags
	url := flag.String("url", "http://127.0.0.1:8080/ping", "HTTP URL to benchmark")
	method := flag.String("method", http.MethodGet, "HTTP method")
	body := flag.String("body", "", "request body; use JSON for API requests")
	requests := flag.Int("requests", 1000, "total requests")
	concurrency := flag.Int("concurrency", 10, "number of concurrent workers")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	flag.Var(&headers, "header", "repeatable request header in name=value form")
	flag.Parse()

	if *requests <= 0 || *concurrency <= 0 {
		fail("requests and concurrency must be greater than zero")
	}
	if *concurrency > *requests {
		*concurrency = *requests
	}

	client := &http.Client{Timeout: *timeout}
	results, elapsed := run(client, *url, *method, []byte(*body), headers, *requests, *concurrency)
	output := summarize(results, *requests, *concurrency, elapsed)
	encoded, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		fail(fmt.Sprintf("encode summary: %v", err))
	}
	fmt.Println(string(encoded))
	if output.Failures > 0 {
		fail("benchmark completed with failed requests")
	}
}

func run(client *http.Client, targetURL, method string, body []byte, headers []string, requests, concurrency int) ([]result, time.Duration) {
	started := time.Now()
	results := make([]result, requests)
	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for index := range jobs {
				results[index] = doRequest(client, targetURL, method, body, headers)
			}
		}()
	}
	for index := range requests {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	return results, time.Since(started)
}

func doRequest(client *http.Client, targetURL, method string, body []byte, headers []string) result {
	requestBody := io.Reader(http.NoBody)
	if len(body) > 0 {
		requestBody = bytes.NewReader(body)
	}
	request, err := http.NewRequest(method, targetURL, requestBody)
	if err != nil {
		return result{err: err}
	}
	for _, header := range headers {
		name, value, _ := strings.Cut(header, "=")
		request.Header.Set(strings.TrimSpace(name), value)
	}
	if len(body) > 0 && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/json")
	}
	started := time.Now()
	response, err := client.Do(request)
	elapsed := time.Since(started)
	if err != nil {
		return result{duration: elapsed, err: err}
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		err = readErr
	} else if closeErr != nil {
		err = closeErr
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if err == nil {
			err = fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
		}
	}
	return result{statusCode: response.StatusCode, duration: elapsed, err: err}
}

func summarize(results []result, requests, concurrency int, elapsed time.Duration) summary {
	latencies := make([]time.Duration, 0, len(results))
	statusCounts := make(map[string]int)
	output := summary{Requests: requests, Concurrency: concurrency, StatusCounts: statusCounts}
	for _, item := range results {
		if item.err != nil {
			output.Failures++
			continue
		}
		output.Successes++
		latencies = append(latencies, item.duration)
		statusCounts[fmt.Sprintf("%d", item.statusCode)]++
	}
	output.Elapsed = elapsed
	if elapsed > 0 {
		output.RequestsPerS = float64(requests) / elapsed.Seconds()
	}
	if len(latencies) == 0 {
		return output
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	output.P50 = percentile(latencies, 0.50)
	output.P95 = percentile(latencies, 0.95)
	output.P99 = percentile(latencies, 0.99)
	return output
}

func percentile(values []time.Duration, ratio float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	if ratio <= 0 {
		return values[0]
	}
	if ratio >= 1 {
		return values[len(values)-1]
	}
	index := int(math.Ceil(float64(len(values))*ratio)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
