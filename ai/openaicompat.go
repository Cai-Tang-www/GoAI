package ai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type OpenAICompatProvider struct {
	profile ProviderProfile
	client  *http.Client
}

func NewOpenAICompatProvider(profile ProviderProfile) (AIProvider, error) {
	client := profile.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	return &OpenAICompatProvider{
		profile: profile,
		client:  client,
	}, nil
}

func (p *OpenAICompatProvider) Name() string {
	return p.profile.Name
}

func (p *OpenAICompatProvider) Chat(ctx context.Context, req ChatRequest) (*ChatStream, error) {
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = p.profile.DefaultModel
	}
	if model == "" {
		return nil, ErrModelNotConfigured
	}

	endpoint := strings.TrimRight(strings.TrimSpace(p.profile.BaseURL), "/") + normalizeEndpointPath(p.profile.EndpointPath)

	payload := map[string]any{
		"model":    model,
		"messages": req.Messages,
		"stream":   true,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.profile.APIKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		bs, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("provider status %d: %s", resp.StatusCode, strings.TrimSpace(string(bs)))
	}

	chunks := make(chan string)
	errs := make(chan error, 1)
	go func() {
		defer close(chunks)
		defer close(errs)
		defer resp.Body.Close()
		if parseErr := ParseOpenAICompatibleSSEStream(ctx, resp.Body, chunks); parseErr != nil {
			errs <- parseErr
		}
	}()

	return &ChatStream{
		Chunks: chunks,
		Errs:   errs,
	}, nil
}

// ParseOpenAICompatibleSSEStream 解析 OpenAI-compatible SSE 流。
func ParseOpenAICompatibleSSEStream(ctx context.Context, reader io.Reader, out chan<- string) error {
	scanner := bufio.NewScanner(reader)
	// 允许较大的单行 chunk
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var dataLines []string
	done := false

	flushEvent := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		payload := strings.Join(dataLines, "\n")
		dataLines = dataLines[:0]
		payload = strings.TrimSpace(payload)
		if payload == "" {
			return nil
		}
		if payload == "[DONE]" {
			done = true
			return nil
		}
		return emitChunkFromPayload(ctx, payload, out)
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			if err := flushEvent(); err != nil {
				return err
			}
			if done {
				return nil
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			dataLines = append(dataLines, data)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	if err := flushEvent(); err != nil {
		return err
	}
	if !done {
		return ErrStreamInterrupted
	}
	return nil
}

func emitChunkFromPayload(ctx context.Context, payload string, out chan<- string) error {
	var decoded struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		return fmt.Errorf("decode stream payload: %w", err)
	}
	for _, choice := range decoded.Choices {
		if choice.Delta.Content == "" {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- choice.Delta.Content:
		}
	}
	return nil
}
