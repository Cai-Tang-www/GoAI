package kafka

import (
	"GoAI/requestctx"
	"context"
	"encoding/json"
	"errors"
	"testing"

	kgo "github.com/segmentio/kafka-go"
)

type fakeMessageWriter struct {
	messages   []kgo.Message
	writeErr   error
	closeErr   error
	closeCalls int
}

func (w *fakeMessageWriter) WriteMessages(_ context.Context, messages ...kgo.Message) error {
	w.messages = append(w.messages, messages...)
	return w.writeErr
}

func (w *fakeMessageWriter) Close() error {
	w.closeCalls++
	return w.closeErr
}

func TestNewRunExecuteMessageCarriesTraceID(t *testing.T) {
	ctx := requestctx.WithTraceID(context.Background(), "trace-kafka-test")
	msg := newRunExecuteMessage(ctx, "run_123")
	if msg.RunID != "run_123" {
		t.Fatalf("unexpected run id: %s", msg.RunID)
	}
	if msg.TraceID != "trace-kafka-test" {
		t.Fatalf("unexpected trace id: %s", msg.TraceID)
	}
}

func TestProducerPublishRunExecuteWritesStableMessage(t *testing.T) {
	writer := &fakeMessageWriter{}
	producer := &Producer{writer: writer}
	ctx := requestctx.WithTraceID(context.Background(), "trace-producer")

	if err := producer.PublishRunExecute(ctx, "run_456"); err != nil {
		t.Fatalf("publish run execute failed: %v", err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("expected one message, got %d", len(writer.messages))
	}
	message := writer.messages[0]
	if string(message.Key) != "run_456" {
		t.Fatalf("unexpected message key: %s", message.Key)
	}
	var payload RunExecuteMessage
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		t.Fatalf("decode message failed: %v", err)
	}
	if payload.RunID != "run_456" || payload.TraceID != "trace-producer" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestProducerWrapsWriterAndCloseErrors(t *testing.T) {
	writeErr := errors.New("write failed")
	producer := &Producer{writer: &fakeMessageWriter{writeErr: writeErr}}
	if err := producer.Send(context.Background(), nil, nil); !errors.Is(err, writeErr) {
		t.Fatalf("expected wrapped write error, got %v", err)
	}

	closeErr := errors.New("close failed")
	producer = &Producer{writer: &fakeMessageWriter{closeErr: closeErr}}
	if err := producer.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("expected wrapped close error, got %v", err)
	}
}

func TestProducerNilReceiverIsSafe(t *testing.T) {
	var producer *Producer
	if err := producer.Send(context.Background(), nil, nil); err == nil {
		t.Fatal("expected nil producer send error")
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("close nil producer failed: %v", err)
	}
}

func TestProducerCloseIsIdempotent(t *testing.T) {
	closeErr := errors.New("close failed")
	writer := &fakeMessageWriter{closeErr: closeErr}
	producer := &Producer{writer: writer}

	for range 2 {
		if err := producer.Close(); !errors.Is(err, closeErr) {
			t.Fatalf("expected original close error, got %v", err)
		}
	}
	if writer.closeCalls != 1 {
		t.Fatalf("expected writer close once, got %d", writer.closeCalls)
	}
}
