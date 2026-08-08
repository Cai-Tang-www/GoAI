package middlewares

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"GoAI/ai"
	"GoAI/governance"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

// TestWrapErrorMapsKnownSentinels 验证已知 sentinel error 会映射为稳定错误码。
func TestWrapErrorMapsKnownSentinels(t *testing.T) {
	testCases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "run not found", err: services.ErrRunNotFound(), wantStatus: http.StatusNotFound, wantCode: CodeRunNotFound},
		{name: "loop not found", err: services.ErrLoopNotFound(), wantStatus: http.StatusNotFound, wantCode: CodeLoopNotFound},
		{name: "run forbidden", err: services.ErrRunForbidden(), wantStatus: http.StatusForbidden, wantCode: CodeAuthForbidden},
		{name: "agent not found", err: services.ErrAgentNotFound(), wantStatus: http.StatusNotFound, wantCode: CodeAgentNotFound},
		{name: "agent forbidden", err: services.ErrAgentForbidden(), wantStatus: http.StatusForbidden, wantCode: CodeAuthForbidden},
		{name: "agent already exists", err: services.ErrAgentAlreadyExists(), wantStatus: http.StatusConflict, wantCode: CodeAgentAlreadyExists},
		{name: "agent invalid state", err: services.ErrAgentInvalidState(), wantStatus: http.StatusConflict, wantCode: CodeAgentInvalidState},
		{name: "agent publish validation", err: services.ErrAgentPublishValidation(), wantStatus: http.StatusUnprocessableEntity, wantCode: CodeAgentPublishValidation},
		{name: "agent registry validation", err: services.ErrAgentRegistryValidation(), wantStatus: http.StatusBadRequest, wantCode: CodeValidationFailed},
		{name: "capability not found", err: services.ErrCapabilityNotFound(), wantStatus: http.StatusNotFound, wantCode: CodeCapabilityNotFound},
		{name: "capability exists", err: services.ErrCapabilityAlreadyExists(), wantStatus: http.StatusConflict, wantCode: CodeCapabilityAlreadyExists},
		{name: "endpoint not found", err: services.ErrEndpointNotFound(), wantStatus: http.StatusNotFound, wantCode: CodeEndpointNotFound},
		{name: "endpoint exists", err: services.ErrEndpointAlreadyExists(), wantStatus: http.StatusConflict, wantCode: CodeEndpointAlreadyExists},
		{name: "endpoint health", err: services.ErrEndpointHealthCheckFailed(), wantStatus: http.StatusBadGateway, wantCode: CodeEndpointHealthCheckFailed},
		{name: "agent route missing", err: services.ErrAgentRouteNotFound(), wantStatus: http.StatusNotFound, wantCode: CodeAgentRouteNotFound},
		{name: "agent route unavailable", err: services.ErrAgentRouteUnavailable(), wantStatus: http.StatusServiceUnavailable, wantCode: CodeAgentRouteUnavailable},
		{name: "agent route invalid", err: services.ErrAgentRouteInvalid(), wantStatus: http.StatusBadRequest, wantCode: CodeAgentRouteInvalid},
		{name: "workflow not found", err: services.ErrWorkflowNotFound(), wantStatus: http.StatusNotFound, wantCode: CodeWorkflowNotFound},
		{name: "provider missing", err: ai.ErrProviderNotFound, wantStatus: http.StatusBadRequest, wantCode: CodeProviderNotFound},
		{name: "stream interrupted", err: ai.ErrStreamInterrupted, wantStatus: http.StatusInternalServerError, wantCode: CodeStreamInterrupted},
		{name: "circuit open", err: governance.ErrCircuitOpen, wantStatus: http.StatusServiceUnavailable, wantCode: CodeServiceUnavailable},
		{name: "downstream timeout", err: governance.ErrDownstreamTimeout, wantStatus: http.StatusGatewayTimeout, wantCode: CodeDownstreamTimeout},
		{name: "dispatch failed", err: fmt.Errorf("%w: kafka down", services.ErrRunDispatchFailed()), wantStatus: http.StatusInternalServerError, wantCode: CodeKafkaPublishFailed},
	}

	for _, tc := range testCases {
		got := WrapError(tc.err)
		if got.HTTPStatus != tc.wantStatus || got.Code != tc.wantCode {
			t.Fatalf("%s: got status=%d code=%s", tc.name, got.HTTPStatus, got.Code)
		}
	}
}

// TestErrorHandlingMiddlewareRecoversPanic 验证 panic 会转成统一内部错误响应。
func TestErrorHandlingMiddlewareRecoversPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(TraceMiddleware(), ErrorHandlingMiddleware())
	router.GET("/panic", func(c *gin.Context) {
		panic("boom")
	})

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d body=%s", w.Code, w.Body.String())
	}
}
