package main

import (
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"

	"fmt"
	"os"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func createProducer(address string) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer([]string{address}, nil)
	check(err)
	return producer
}

func closeProducer(producer sarama.SyncProducer) {
	err := producer.Close()
	check(err)
}

func sendMessage(producer sarama.SyncProducer, topic string, text string) {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(text)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Printf("Failed to send message: %s\n", err)
	} else {
		fmt.Printf("Message sent \"%s\" to partition %d at offset %d\n", text, partition, offset)

	}
}

func main() {
	addressKafka := os.Args[1]
	producer := createProducer(addressKafka)

	defer closeProducer(producer)

	topicKafka := os.Args[2]

	t, err := tail.TailFile("log/output.log", tail.Config{Follow: true})
	check(err)

	for line := range t.Lines {
		sendMessage(producer, topicKafka, line.Text)
	}
}
