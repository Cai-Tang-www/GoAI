package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"GoAI/config"
	"GoAI/requestctx"

	kgo "github.com/segmentio/kafka-go"
)

type fakeMessageReader struct {
	messages []kgo.Message
	index    int
	closeErr error
}

type closeAwareMessageReader struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func (r *closeAwareMessageReader) ReadMessage(ctx context.Context) (kgo.Message, error) {
	select {
	case <-ctx.Done():
		return kgo.Message{}, ctx.Err()
	case <-r.closed:
		return kgo.Message{}, errors.New("reader closed")
	}
}

func (r *closeAwareMessageReader) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func (r *fakeMessageReader) ReadMessage(ctx context.Context) (kgo.Message, error) {
	if r.index < len(r.messages) {
		message := r.messages[r.index]
		r.index++
		return message, nil
	}
	<-ctx.Done()
	return kgo.Message{}, ctx.Err()
}

func (r *fakeMessageReader) Close() error {
	return r.closeErr
}

func TestNewConsumerRejectsMissingDependencies(t *testing.T) {
	if _, err := NewConsumer(nil, func(context.Context, RunExecuteMessage) error { return nil }); err == nil {
		t.Fatal("expected nil config error")
	}
	if _, err := NewConsumer(&config.Config{}, nil); err == nil {
		t.Fatal("expected nil handler error")
	}
}

func TestConsumerSkipsInvalidMessagesAndPropagatesTraceID(t *testing.T) {
	validPayload, err := json.Marshal(RunExecuteMessage{RunID: "run_consume", TraceID: "trace-consume"})
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}
	reader := &fakeMessageReader{messages: []kgo.Message{
		{Value: []byte("not-json")},
		{Value: []byte(`{"trace_id":"trace-empty"}`)},
		{Topic: "run_execute", Partition: 1, Offset: 2, Value: validPayload},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var handled RunExecuteMessage
	var traceID string
	consumer := &Consumer{
		reader: reader,
		handler: func(ctx context.Context, message RunExecuteMessage) error {
			handled = message
			traceID = requestctx.TraceIDFromContext(ctx)
			cancel()
			return nil
		},
		logger: log.New(io.Discard, "", 0),
	}

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}
	if handled.RunID != "run_consume" {
		t.Fatalf("unexpected handled message: %+v", handled)
	}
	if traceID != "trace-consume" {
		t.Fatalf("unexpected handler trace id: %s", traceID)
	}
	if reader.index != 3 {
		t.Fatalf("expected all three messages to be read, got %d", reader.index)
	}
}

func TestConsumerCloseWrapsReaderError(t *testing.T) {
	closeErr := errors.New("reader close failed")
	consumer := &Consumer{reader: &fakeMessageReader{closeErr: closeErr}}
	if err := consumer.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("expected wrapped close error, got %v", err)
	}
	if err := consumer.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("expected repeated close to return the original error, got %v", err)
	}
	var nilConsumer *Consumer
	if err := nilConsumer.Close(); err != nil {
		t.Fatalf("close nil consumer failed: %v", err)
	}
}

func TestConsumerStopsWhenReaderIsClosed(t *testing.T) {
	reader := &closeAwareMessageReader{closed: make(chan struct{})}
	consumer := &Consumer{
		reader:  reader,
		handler: func(context.Context, RunExecuteMessage) error { return nil },
		logger:  log.New(io.Discard, "", 0),
	}

	done := make(chan struct{})
	go func() {
		consumer.Start(context.Background())
		close(done)
	}()

	if err := consumer.Close(); err != nil {
		t.Fatalf("close consumer failed: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop after reader close")
	}
}
