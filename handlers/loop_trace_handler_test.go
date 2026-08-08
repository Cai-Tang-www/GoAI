package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"GoAI/config"
	"GoAI/db"
	"GoAI/middlewares"
	"GoAI/models"
)

func TestLoopTraceRoutesReturnOwnedObservabilitySnapshot(t *testing.T) {
	database := setupRBACIntegrationDB(t)
	member := models.User{Username: "trace-member", Email: "trace-member@example.com", Password: "x"}
	outsider := models.User{Username: "trace-outsider", Email: "trace-outsider@example.com", Password: "x"}
	admin := models.User{Username: "trace-admin", Email: "trace-admin@example.com", Password: "x"}
	if err := database.Create(&member).Error; err != nil {
		t.Fatalf("create member failed: %v", err)
	}
	if err := database.Create(&outsider).Error; err != nil {
		t.Fatalf("create outsider failed: %v", err)
	}
	if err := database.Create(&admin).Error; err != nil {
		t.Fatalf("create admin failed: %v", err)
	}
	config.AppConfig = &config.Config{
		JWTSecret:                  "loop-trace-handler-secret",
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: admin.Username,
		ModelProviders:             map[string]config.ModelProviderConfig{},
	}
	if err := db.SeedRBAC(database, config.AppConfig); err != nil {
		t.Fatalf("seed RBAC failed: %v", err)
	}
	memberToken, err := middlewares.GenerateToken(member.ID)
	if err != nil {
		t.Fatalf("generate member token failed: %v", err)
	}
	outsiderToken, err := middlewares.GenerateToken(outsider.ID)
	if err != nil {
		t.Fatalf("generate outsider token failed: %v", err)
	}
	adminToken, err := middlewares.GenerateToken(admin.ID)
	if err != nil {
		t.Fatalf("generate admin token failed: %v", err)
	}
	run := models.Run{RunID: "run-http-trace", TraceID: "trace-http", AgentID: 1, WorkflowID: 1, UserID: uint64(member.ID), TriggerType: "api", InputJSON: `{}`, Status: models.RunStatusSuccess}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create trace run failed: %v", err)
	}
	loop := models.LoopRecord{LoopID: "loop-http-trace", TraceID: run.TraceID, RunID: run.RunID, AgentID: run.AgentID, LoopType: models.LoopTypeRun, Status: models.LoopStatusSuccess, InputSnapshotJSON: `{}`, OutputSnapshotJSON: `{}`}
	if err := database.Create(&loop).Error; err != nil {
		t.Fatalf("create trace loop failed: %v", err)
	}
	if err := database.Create(&models.LoopEvaluation{LoopID: loop.LoopID, EvaluatorCode: "quality-v1", Status: models.EvaluationStatusPending, ResultJSON: `{}`}).Error; err != nil {
		t.Fatalf("create trace evaluation failed: %v", err)
	}
	router := newTestRouter(t, database, nil)

	request := func(path, token string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}

	traceResponse := request("/api/runs/"+run.RunID+"/trace", memberToken)
	traceEnvelope := decodeEnvelope(t, traceResponse)
	if traceResponse.Code != http.StatusOK || traceEnvelope.Code != middlewares.CodeOK {
		t.Fatalf("trace response status/code = %d/%s body=%s", traceResponse.Code, traceEnvelope.Code, traceResponse.Body.String())
	}
	var trace struct {
		RootRun models.Run              `json:"root_run"`
		Loops   []models.LoopRecord     `json:"loops"`
		Evals   []models.LoopEvaluation `json:"evaluations"`
	}
	if err := json.Unmarshal(traceEnvelope.Data, &trace); err != nil {
		t.Fatalf("decode trace data failed: %v", err)
	}
	if trace.RootRun.RunID != run.RunID || len(trace.Loops) != 1 || len(trace.Evals) != 1 {
		t.Fatalf("unexpected trace data: %+v", trace)
	}

	loopDetailResponse := request("/api/loops/"+loop.LoopID, memberToken)
	loopDetailEnvelope := decodeEnvelope(t, loopDetailResponse)
	var loopDetail struct {
		LoopID      string `json:"loop_id"`
		Evaluations []struct {
			EvaluatorCode string `json:"evaluator_code"`
		} `json:"evaluations"`
	}
	if err := json.Unmarshal(loopDetailEnvelope.Data, &loopDetail); err != nil {
		t.Fatalf("decode loop detail data failed: %v", err)
	}
	if loopDetailResponse.Code != http.StatusOK || loopDetail.LoopID != loop.LoopID || len(loopDetail.Evaluations) != 1 || loopDetail.Evaluations[0].EvaluatorCode != "quality-v1" {
		t.Fatalf("unexpected loop detail response: %d %+v", loopDetailResponse.Code, loopDetail)
	}

	for _, path := range []string{
		"/api/runs/" + run.RunID + "/loops",
		"/api/loops/" + loop.LoopID + "/evaluations",
	} {
		response := request(path, memberToken)
		if response.Code != http.StatusOK || decodeEnvelope(t, response).Code != middlewares.CodeOK {
			t.Fatalf("observability route %s failed: %d %s", path, response.Code, response.Body.String())
		}
	}

	for _, path := range []string{
		"/api/runs/" + run.RunID + "/trace",
		"/api/runs/" + run.RunID + "/loops",
		"/api/loops/" + loop.LoopID,
		"/api/loops/" + loop.LoopID + "/evaluations",
	} {
		response := request(path, outsiderToken)
		envelope := decodeEnvelope(t, response)
		if response.Code != http.StatusForbidden || envelope.Code != middlewares.CodeAuthForbidden {
			t.Fatalf("outsider observability route %s = %d/%s, want 403/AUTH_FORBIDDEN", path, response.Code, envelope.Code)
		}
	}

	adminResponse := request("/api/loops/"+loop.LoopID, adminToken)
	if adminResponse.Code != http.StatusOK || decodeEnvelope(t, adminResponse).Code != middlewares.CodeOK {
		t.Fatalf("admin should read another owner's loop: %d %s", adminResponse.Code, adminResponse.Body.String())
	}

	missing := request("/api/loops/missing-loop", memberToken)
	missingEnvelope := decodeEnvelope(t, missing)
	if missing.Code != http.StatusNotFound || missingEnvelope.Code != middlewares.CodeLoopNotFound {
		t.Fatalf("missing loop response = %d/%s, want 404/LOOP_NOT_FOUND", missing.Code, missingEnvelope.Code)
	}
}
