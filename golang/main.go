package main

import (
	"KafkaProducerConsumer/consumer"
	"KafkaProducerConsumer/producer"
	"errors"
	"fmt"
	"log"
	"net/http"
)

const (
	KafkaServer = "localhost:9092,localhost:9093,localhost:9094"
	KafkaTopic  = "sandbox"
)

func main() {

	kp, err := producer.NewKafKaProducer(KafkaServer)

	if err != nil {
		log.Fatalf("Failed to init Kafka: %v", err)
	}

	defer kp.Producer.Close()

	kc, err := consumer.NewKafKaConsumer(KafkaServer, []string{KafkaTopic})

	if err != nil {
		log.Fatalf("Failed to init Kafka: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/send", kp.SendHandler)
	mux.HandleFunc("/pull", kc.GetHandler)

	srv := http.Server{Addr: "localhost:8080", Handler: mux}

	fmt.Printf("The Server running at http://localhost:8080 \n")

	err = srv.ListenAndServe()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Critical error: %v", err)
	}
}
