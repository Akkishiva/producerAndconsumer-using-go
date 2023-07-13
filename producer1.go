package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Student struct {
	Name       string
	Section    string
	English    int
	Maths      int
	Physics    int
	Chemistry  int
	Biology    int
	TotalMarks int
	Status     string
}

func main() {
	// Kafka cluster configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     "YA25TQI6C5RXKVKQ",
		"sasl.password":     "bs/dtnh0yLiU5myChazsSjX96iZWyQXW7iPrnhtmCnyvBXo6C8wPYBci4F7Tk9zd",
	}

	// Create a Kafka producer
	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Example usage
	topic := "user"

	// Example mark sheet data
	sections := []string{"A", "B", "C"} // List of sections

	// Generate mark sheets for each section
	for _, section := range sections {
		passCount := 0
		totalCount := 0

		for studentID := 1; studentID <= 100; studentID++ {
			studentName := fmt.Sprintf("Student_%s_%d", section, studentID)
			english := rand.Intn(61) + 40   // Random marks between 40 and 100
			maths := rand.Intn(61) + 40     // Random marks between 40 and 100
			physics := rand.Intn(61) + 40   // Random marks between 40 and 100
			chemistry := rand.Intn(61) + 40 // Random marks between 40 and 100
			biology := rand.Intn(61) + 40   // Random marks between 40 and 100

			// Calculate total marks and determine pass/fail status
			totalMarks := english + maths + physics + chemistry + biology
			status := "Failed"
			if totalMarks > 300 {
				status = "Pass"
				passCount++
			}
			totalCount++

			// Create the student object
			student := Student{
				Name:       studentName,
				Section:    section,
				English:    english,
				Maths:      maths,
				Physics:    physics,
				Chemistry:  chemistry,
				Biology:    biology,
				TotalMarks: totalMarks,
				Status:     status,
			}

			// Convert the student object to JSON bytes
			jsonData, err := json.Marshal(student)
			if err != nil {
				log.Println("Failed to marshal JSON:", err)
				continue
			}

			// Create a Kafka message with the JSON data
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: selectPartition(section),
				},
				Key:   []byte(studentName),
				Value: jsonData,
			}

			// Produce the message to Kafka
			err = producer.Produce(msg, nil)
			if err != nil {
				log.Println("Failed to produce message to Kafka:", err)
			}
		}

		// Calculate pass percentage
		passPercentage := float64(passCount) / float64(totalCount) * 100

		// Print pass percentage for the section
		log.Printf("Section %s: Pass Percentage - %.2f%%", section, passPercentage)
	}

	// Wait for messages to be delivered
	producer.Flush(15 * 1000)

	log.Println("All messages published successfully")
}

// selectPartition maps the section to a Kafka partition
func selectPartition(section string) int32 {
	switch section {
	case "A":
		return 0
	case "B":
		return 1
	case "C":
		return 2
	default:
		// Use random partition if section is not A, B, or C
		return kafka.PartitionAny
	}
}
