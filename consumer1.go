package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Kafka cluster configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     "YA25TQI6C5RXKVKQ",
		"sasl.password":     "bs/dtnh0yLiU5myChazsSjX96iZWyQXW7iPrnhtmCnyvBXo6C8wPYBci4F7Tk9zd",
		"group.id":          "test-user",
		"auto.offset.reset": "earliest",
	}

	// Create a Kafka consumer
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to the topic
	topic := "user"
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %v", err)
	}

	// Handle OS signals for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming messages
	run := true
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Received signal: %v", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					log.Printf("Error receiving message: %v", e.TopicPartition.Error)
				} else {
					var data map[string]interface{}
					err := json.Unmarshal(e.Value, &data)
					if err != nil {
						log.Printf("Failed to unmarshal JSON: %v", err)
						continue
					}

					// Process the received message
					log.Printf("Received message: %v", data)
				}
			case kafka.Error:
				log.Printf("Kafka consumer error: %v", e)
				run = false
			}
		}
	}

	log.Println("Consumer stopped")
}
