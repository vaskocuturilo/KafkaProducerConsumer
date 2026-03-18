package producer

import (
	"KafkaProducerConsumer/dto"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const KafkaTopic = "sandbox"

type KafkaClient interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

type KafkaService struct {
	Producer KafkaClient
}

func NewKafKaProducer(servers string) (*KafkaService, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": servers, "enable.idempotence": true, "acks": "all"})

	if err != nil {
		return nil, err
	}

	return &KafkaService{Producer: p}, nil
}

func (ks *KafkaService) sendMessage(topic string, order dto.OrderDto) error {
	value, _ := json.Marshal(order)

	deliveryChan := make(chan kafka.Event)

	err := ks.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("produced failed: %w", err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivered failed %w", m.TopicPartition.Error)
	}

	log.Printf("Success produced record: Metadata: %v, Topic: %v, Partiotion: %v, Offset: %v",
		m.TopicPartition.Metadata,
		m.TopicPartition.Topic,
		m.TopicPartition.Partition,
		m.TopicPartition.Offset)

	close(deliveryChan)

	return nil
}

func (ks *KafkaService) SendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	var order dto.OrderDto

	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		log.Printf("Decode payload error: %v", err)
		http.Error(w, "Failed to Decode payload", http.StatusBadRequest)
		return
	}

	if order.ID == "" || order.Amount <= 0 {
		http.Error(w, "Missing required order fields", http.StatusBadRequest)
		return
	}

	log.Printf("Processing order: %s for product: %s", order.ID, order.ProductId)

	topic := KafkaTopic

	err := ks.sendMessage(topic, order)

	if err != nil {
		log.Printf("Kafka error: %v", err)
		http.Error(w, "Failed to send message", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	response := struct {
		Status  string `json:"status"`
		OrderID string `json:"order_id"`
	}{
		Status:  "Success. The order sent and confirmed by Kafka",
		OrderID: order.ID,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode response: %v", err)
	}
}
