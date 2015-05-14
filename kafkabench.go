package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"gopkg.in/shopify/sarama.v1"
)

const (
	defaultBrokerAddress = "127.0.0.1:9092"
	defaultTopicName     = "benchmark"
	defaultEventCount    = 100000
	clientID             = string(iota)
)

var (
	brokerAddress = flag.String("address", defaultBrokerAddress, "broker address to connect to")
	eventCount    = flag.Int("events", defaultEventCount, "number of events to produce")
	topicName     = flag.String("topic", defaultTopicName, "kafka topic to use")
	config        = sarama.NewConfig()
)

func init() {
	config.Producer.Timeout = time.Millisecond * 1000
	config.Consumer.MaxWaitTime = time.Millisecond * 100
}

type innerMessage struct {
	previousTimestamp   int64
	timestamp           int64
	metric              string
	cityID              uint32
	cityName            string
	dummyID             uint32
	dummyRating         float32
	dummyStatus         string
	maxDispatchDistance uint32
	timeOpen            uint32
	timeNotOnTrip       uint32
	longitude           float32
	latitude            float32
	vehicleViewID       uint32
	vehicleUUID         string
}

type dummyMessage struct {
	ts   float64
	host string
	msg  innerMessage
}

func getClient(addr string) *sarama.Client {
	brokers := []string{*brokerAddress}
	log.Println("Connecting to brokers: ", brokers)

	client, err := sarama.NewClient(brokers, config)

	if err != nil {
		log.Fatal(err)
	}
	return &client
}

func produce(client *sarama.Client, topic string, events int) {
	producer, err := sarama.NewSyncProducer([]string{*brokerAddress}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	log.Printf("Sending %d messages to topic '%s'\n", events, topic)

	message := dummyMessage{
		ts:   1384205271.861,
		host: "dummy.host",
		msg: innerMessage{
			previousTimestamp: 1384205268645,
			timestamp:         1384205271861,
			metric:            "location",
			cityID:            42,
			cityName:          "foo",
			vehicleViewID:     442,
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
		producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(encoded),
		})
	}
}

func consume(client *sarama.Client, topic string, events int) {
	consumer, err := sarama.NewConsumer([]string{*brokerAddress}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()

	log.Printf("Reading %d events from '%s' topic", events, topic)
	count := 0
	for message := range pc.Messages() {
		if err := json.Unmarshal(message.Value, &message); err != nil {
			log.Fatal("Unable to decode JSON message", err)
		}

		count++
		if count == events {
			log.Printf("Received final (%d) event: %s", count, string(message.Value))
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
	defer log.Println(client.Close())

	now := time.Now()
	if flag.Args()[0] == "produce" {
		produce(client, *topicName, *eventCount)
	} else {
		consume(client, *topicName, *eventCount)
	}
	ellapsed := time.Now().Sub(now)
	log.Printf("Finished in %s", ellapsed)
}
