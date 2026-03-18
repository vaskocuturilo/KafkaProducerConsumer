package producer

import (
	"KafkaProducerConsumer/dto"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MockProducer struct {
	ProduceFunc func(msg *kafka.Message, deliveryChan chan kafka.Event) error
}

func (m *MockProducer) Close() {}

func (m *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return m.ProduceFunc(msg, deliveryChan)
}

func TestKafkaService_SendMessage_Success(t *testing.T) {
	mock := &MockProducer{
		ProduceFunc: func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
			go func() {
				topic := "sandbox"
				deliveryChan <- &kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &topic,
						Partition: 0,
						Offset:    1,
					},
				}
			}()
			return nil
		},
	}

	ks := &KafkaService{Producer: mock}

	orderDto := dto.OrderDto{ID: "order-123", Amount: 100}
	err := ks.sendMessage("sandbox", orderDto)
	if err != nil {
		t.Errorf("Expected success, got err: %v", err)
	}
}

func TestSendHandler_InvalidJSON(t *testing.T) {
	ks := &KafkaService{Producer: &MockProducer{}}

	req := httptest.NewRequest("POST", "/send", strings.NewReader("not-json"))
	rr := httptest.NewRecorder()

	ks.SendHandler(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("Handler returned wrong status: got %v want %v", status, http.StatusBadRequest)
	}
}
