package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

const (
	defaultBrokerAddress = "127.0.0.1:9092"
	defaultTopicName     = "benchmark"
	defaultEventCount    = 100000
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

type InnerMessage struct {
	previousTimestamp   int64
	timestamp           int64
	metric              string
	cityId              uint32
	cityName            string
	dummyId             uint32
	dummyRating         float32
	dummyStatus         string
	maxDispatchDistance uint32
	timeOpen            uint32
	timeNotOnTrip       uint32
	longitude           float32
	latitude            float32
	vehicleViewId       uint32
	vehicleUUID         string
}

type DummyMessage struct {
	ts   float64
	host string
	msg  InnerMessage
}

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

	message := DummyMessage{
		ts:   1384205271.861,
		host: "dummy.host",
		msg: InnerMessage{
			previousTimestamp: 1384205268645,
			timestamp:         1384205271861,
			metric:            "location",
			cityId:            42,
			cityName:          "foo",
			vehicleViewId:     442,
			vehicleUUID:       "aoeu-aoeu-aoeu-aoeu-aoeu",
			timeNotOnTrip:     3430946,
			dummyStatus:       "something",
		},
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		log.Fatal("Unable to JSON-encode dummy message", err)
	}

	for i := 0; i < events; i++ {
		producer.SendMessage(nil, sarama.ByteEncoder(encoded))
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
	var message DummyMessage
	for event := range consumer.Events() {
		if err := json.Unmarshal(event.Value, &message); err != nil {
			log.Fatal("Unable to decode JSON message", err)
		}

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

	now := time.Now()
	if flag.Args()[0] == "produce" {
		produce(client, *topicName, *eventCount)
	} else {
		consume(client, *topicName, *eventCount)
	}
	ellapsed := time.Now().Sub(now)
	log.Printf("Finished in %s", ellapsed)
}
