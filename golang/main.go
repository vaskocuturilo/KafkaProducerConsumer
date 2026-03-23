package main

import (
	"KafkaProducerConsumer/consumer"
	"KafkaProducerConsumer/producer"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
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

	go kc.StartWorker()

	mux := http.NewServeMux()

	mux.HandleFunc("/send", kp.SendHandler)

	srv := http.Server{Addr: "localhost:8080", Handler: mux}

	done := make(chan os.Signal, 1)

	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		fmt.Printf("Server running at http://localhost:8080\n")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-done
	fmt.Println("\n Shutting down gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kc.Consumer.Close()
	srv.Shutdown(ctx)
}
