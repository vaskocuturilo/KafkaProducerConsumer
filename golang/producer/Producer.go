package producer

import (
	"KafkaProducerConsumer/dto"
	"KafkaProducerConsumer/utils"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

const KafkaTopic = "sandbox"

type KafkaService struct {
	Producer *kafka.Producer
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

	topic := KafkaTopic

	order := dto.OrderDto{

		ID: uuid.New().String(),

		ProductId: uuid.New().String(),

		Amount: utils.RandRange(1, 456000),
	}

	err := ks.sendMessage(topic, order)

	if err != nil {
		log.Printf("Kafka error: %v", err)
		http.Error(w, "Failed to send message", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "The OrderDto sent and confirmed by Kafka")
}
