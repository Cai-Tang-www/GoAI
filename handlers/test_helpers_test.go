package handlers_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

type apiEnvelope struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	TraceID string          `json:"trace_id"`
}

// decodeEnvelope 解析统一响应 envelope，并校验 trace_id 已返回。
func decodeEnvelope(t *testing.T, w *httptest.ResponseRecorder) apiEnvelope {
	t.Helper()
	var env apiEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope failed: %v body=%s", err, w.Body.String())
	}
	if strings.TrimSpace(env.TraceID) == "" {
		t.Fatalf("trace_id should not be empty: body=%s", w.Body.String())
	}
	return env
}

// uniqueSQLiteDSN 为每个测试生成独立的内存库 DSN，避免整包执行时相互污染。
func uniqueSQLiteDSN(t *testing.T) string {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_", ":", "_").Replace(t.Name())
	return "file:" + name + "?mode=memory&cache=shared"
}
