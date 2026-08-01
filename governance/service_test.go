package governance

import (
	"math"
	"testing"
	"time"
)

func TestValidateConfigAllowsDisabledMode(t *testing.T) {
	if err := ValidateConfig(Config{}); err != nil {
		t.Fatalf("disabled governance should not require settings: %v", err)
	}
	service, err := New(Config{})
	if err != nil {
		t.Fatalf("create disabled governance service: %v", err)
	}
	if service.Enabled || service.Limiter != nil || service.Transport != nil {
		t.Fatalf("unexpected disabled service: %+v", service)
	}
}

func TestValidateConfigRejectsEnabledInvalidSettings(t *testing.T) {
	config := Config{
		Enabled:                    true,
		RateLimitRequestsPerSecond: 1,
		RateLimitBurst:             1,
		RateLimitMaxKeys:           1,
		DownstreamRequestTimeout:   time.Second,
		CircuitFailureThreshold:    1,
		CircuitOpenTimeout:         0,
	}
	if err := ValidateConfig(config); err == nil {
		t.Fatal("expected invalid enabled configuration error")
	}
}

func TestValidateConfigRejectsNonFiniteRate(t *testing.T) {
	for _, rate := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		config := Config{
			Enabled:                    true,
			RateLimitRequestsPerSecond: rate,
			RateLimitBurst:             1,
			RateLimitMaxKeys:           1,
			DownstreamRequestTimeout:   time.Second,
			CircuitFailureThreshold:    1,
			CircuitOpenTimeout:         time.Second,
			CircuitMaxTargets:          1,
		}
		if err := ValidateConfig(config); err == nil {
			t.Fatalf("expected non-finite rate %v to be rejected", rate)
		}
	}
}
