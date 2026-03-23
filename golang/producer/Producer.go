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
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":   servers,
		"enable.idempotence":  true,
		"retries":             5,
		"retry.backoff.ms":    100,
		"delivery.timeout.ms": 30000,
		"acks":                "all",
	})

	if err != nil {
		return nil, err
	}

	ks := &KafkaService{Producer: p}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed for Order %s: %v", ev.Value, ev.TopicPartition.Error)
				} else {
					log.Printf("Delivered to %v at offset %v", ev.TopicPartition.Topic, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	return ks, nil
}

func (ks *KafkaService) sendMessage(topic string, order dto.OrderDto) error {
	value, err := json.Marshal(order)

	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	log.Println("1. Starting Produce")
	deliveryChan := make(chan kafka.Event, 1)

	err = ks.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(order.ID),
		Value:          value,
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("produced failed: %w", err)
	}

	log.Println("2. Waiting for deliveryChan")
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

	log.Println("3. Received event")
	return nil
}

func (ks *KafkaService) SendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()

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
