package consumer

import (
	"KafkaProducerConsumer/dto"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MockConsumer struct {
	ReadMessageFunc  func(timeout time.Duration) (*kafka.Message, error)
	StoreMessageFunc func(msg *kafka.Message) ([]kafka.TopicPartition, error)
}

func (m *MockConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	return m.ReadMessageFunc(timeout)
}

func (m *MockConsumer) StoreMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return m.StoreMessageFunc(msg)
}

func (m *MockConsumer) Close() error { return nil }

func TestKafkaService_GetHandler_Success(t *testing.T) {
	order := dto.OrderDto{ID: "order-999", Amount: 450}
	orderBytes, _ := json.Marshal(order)
	topic := "sandbox"

	mock := &MockConsumer{
		ReadMessageFunc: func(timeout time.Duration) (*kafka.Message, error) {
			return &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
				Value:          orderBytes,
			}, nil
		},
		StoreMessageFunc: func(msg *kafka.Message) ([]kafka.TopicPartition, error) {
			return nil, nil
		},
	}

	ks := &KafkaService{Consumer: mock}

	req := httptest.NewRequest("GET", "/pull", nil)
	rr := httptest.NewRecorder()

	ks.GetHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rr.Code)
	}

	var response dto.OrderDto
	json.Unmarshal(rr.Body.Bytes(), &response)
	if response.ID != "order-999" {
		t.Errorf("Expected order ID order-999, got %s", response.ID)
	}
}

func TestGetHandler_Timeout(t *testing.T) {
	mock := &MockConsumer{
		ReadMessageFunc: func(timeout time.Duration) (*kafka.Message, error) {
			// Simulate Kafka timeout error
			return nil, kafka.NewError(kafka.ErrTimedOut, "Local: Timed out", false)
		},
	}

	ks := &KafkaService{Consumer: mock}

	req := httptest.NewRequest("GET", "/pull", nil)
	rr := httptest.NewRecorder()

	ks.GetHandler(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("Expected status 204 for timeout, got %d", rr.Code)
	}
}
