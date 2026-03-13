package consumer

import (
	"KafkaProducerConsumer/dto"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaService struct {
	Consumer *kafka.Consumer
}

func NewKafKaConsumer(servers string, topics []string) (*KafkaService, error) {
	c, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers":        servers,
			"group.id":                 "foo",
			"auto.offset.reset":        "earliest",
			"session.timeout.ms":       6000,
			"enable.auto.offset.store": false})

	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics(topics, nil)

	return &KafkaService{Consumer: c}, err
}

func (ks *KafkaService) GetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	msg, err := ks.Consumer.ReadMessage(2 * time.Second)

	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrTimedOut {
			http.Error(w, "No message available", http.StatusNoContent)
		} else {
			http.Error(w, "Kafka Error: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	var order dto.OrderDto

	if err := json.Unmarshal(msg.Value, &order); err != nil {
		http.Error(w, "Malformed JSON in Kafka", http.StatusInternalServerError)
		return
	}

	log.Printf("Success consumer message: "+
		"Metadata: %v, Topic: %v, Partiotion: %v, Offset: %v, Key: %v, Value: %v",
		msg.TopicPartition.Metadata,
		msg.TopicPartition.Topic,
		msg.TopicPartition.Partition,
		msg.TopicPartition.Offset,
		msg.Key,
		msg.Value)

	_, err = ks.Consumer.StoreMessage(msg)

	if err != nil {
		log.Printf("Failed to store offset: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(order)
	if err != nil {
		return
	}
}
