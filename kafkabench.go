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
	}
}
