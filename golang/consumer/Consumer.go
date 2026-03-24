package consumer

import (
	"KafkaProducerConsumer/dto"
	"encoding/json"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer interface {
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	StoreMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
	CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

type KafkaService struct {
	Consumer KafkaConsumer
}

func NewKafKaConsumer(servers string, topics []string) (*KafkaService, error) {
	c, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers":        servers,
			"group.id":                 "order-service",
			"auto.offset.reset":        "earliest",
			"session.timeout.ms":       6000,
			"enable.auto.offset.store": false})

	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics(topics, nil)

	return &KafkaService{Consumer: c}, err
}

func (ks *KafkaService) StartWorker() {
	log.Println("Kafka Worker started. Listening for messages...")

	for {
		msg, err := ks.Consumer.ReadMessage(1 * time.Second)

		if err != nil {
			if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
				continue
			}
			log.Printf("Consumer error: %v", err)
			time.Sleep(500 * time.Millisecond)
		}

		var order dto.OrderDto

		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("Failed to decode message: %v", err)
			continue
		}

		log.Printf("📥 PROCESSED: OrderID=%s, Amount=%d [Partition: %d]",
			order.ID, order.Amount, msg.TopicPartition.Partition)

		_, err = ks.Consumer.CommitMessage(msg)

		if err != nil {
			log.Printf("Failed to store offset: %v", err)
			continue
		}

	}
}
