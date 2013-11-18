package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

const (
	defaultBrokerAddress = "127.0.0.1:9092"
	defaultTopicName     = "benchmark"
	defaultEventCount    = 10000
	clientId             = string(iota)
)

var (
	brokerAddress = flag.String("address", defaultBrokerAddress, "broker address to connect to")
	eventCount    = flag.Int("events", defaultEventCount, "number of events to produce")
	topicName     = flag.String("topic", defaultTopicName, "kafka topic to use")
	clientConfig  = sarama.ClientConfig{
		WaitForElection: time.Second,
	}
	producerConfig = sarama.ProducerConfig{
		Timeout: 1000,
	}
	consumerConfig = sarama.ConsumerConfig{
		MaxWaitTime: 100, // milliseconds
	}
)

func getClient(addr string) (client *sarama.Client) {
	addrs := make([]string, 1)
	addrs[0] = addr
	client, err := sarama.NewClient(clientId, addrs, &clientConfig)

	log.Print("Connecting to brokers: ", addrs)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func produce(client *sarama.Client, topic string, events int) {
	producer, err := sarama.NewProducer(client, topic, &producerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	log.Printf("Sending %d messages to topic '%s'\n", events, topic)

	for i := 0; i < events; i++ {
		producer.SendMessage(nil, sarama.StringEncoder("hello, world"))
	}
}

func consume(client *sarama.Client, topic string, events int) {
	consumer, err := sarama.NewConsumer(client, topic, 0, "bench-group", &consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	log.Printf("Reading %d events from '%s' topic", events, topic)
	count := 0
	for event := range consumer.Events() {
		count++
		if count == events {
			log.Printf("Received final (%d) event: %s", count, string(event.Value))
			return
		}
	}
}

func usage() {
	flag.Usage()
	fmt.Println()
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		return
	}
	client := getClient(*brokerAddress)
	defer client.Close()

	if flag.Args()[0] == "produce" {
		produce(client, *topicName, *eventCount)
	} else {
		consume(client, *topicName, *eventCount)
	}
}
