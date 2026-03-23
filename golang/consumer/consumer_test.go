package consumer

import (
	"KafkaProducerConsumer/dto"
	"encoding/json"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MockConsumer struct {
	ReadMessageFunc   func(timeout time.Duration) (*kafka.Message, error)
	StoreMessageFunc  func(msg *kafka.Message) ([]kafka.TopicPartition, error)
	CommitMessageFunc func(msg *kafka.Message) ([]kafka.TopicPartition, error)
}

func (m *MockConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	return m.ReadMessageFunc(timeout)
}

func (m *MockConsumer) StoreMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return m.StoreMessageFunc(msg)
}

func (m *MockConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return m.CommitMessageFunc(msg)
}

func (m *MockConsumer) Close() error { return nil }

func TestStartWorker_Success(t *testing.T) {
	done := make(chan bool) // Signal to stop the test
	topic := "sandbox"
	order := dto.OrderDto{ID: "test-1", Amount: 100}
	val, _ := json.Marshal(order)

	mock := &MockConsumer{
		ReadMessageFunc: func(timeout time.Duration) (*kafka.Message, error) {
			return &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
				Value:          val,
			}, nil
		},
		StoreMessageFunc: func(msg *kafka.Message) ([]kafka.TopicPartition, error) {
			return nil, nil
		},
		CommitMessageFunc: func(msg *kafka.Message) ([]kafka.TopicPartition, error) {
			done <- true
			return nil, nil
		},
	}

	ks := &KafkaService{Consumer: mock}

	go ks.StartWorker()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Test timed out: Worker never committed the message")
	}
}
