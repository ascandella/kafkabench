package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"runtime"
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
	brokers       []string
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

func getClient(brokers []string) *sarama.Client {
	log.Println("Connecting to brokers: ", brokers)

	client, err := sarama.NewClient(brokers, config)

	if err != nil {
		log.Fatal(err)
	}
	return &client
}

func produce(client *sarama.Client, topic string, events int, done chan<- struct{}) {
	producer, err := sarama.NewSyncProducerFromClient(*client)
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

	close(done)
}

func consume(client *sarama.Client, topic string, done <-chan struct{}) {
	consumer, err := sarama.NewConsumerFromClient(*client)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()

	log.Printf("Reading events from '%s' topic", topic)
	count := 0
	select {
	case message := <-pc.Messages():
		if err := json.Unmarshal(message.Value, &message); err != nil {
			log.Fatal("Unable to decode JSON message", err)
		}

		count++
		if count%10000 == 0 {
			log.Printf("Read %d events, offset: %d\n", count, message.Offset)
		}
	case <-done:
		log.Printf("Received final (%d) events", count)
	}
}

func usage() {
	flag.Usage()
	fmt.Println()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	brokers = []string{*brokerAddress}
	client := getClient(brokers)

	now := time.Now()
	done := make(chan struct{})

	go produce(client, *topicName, *eventCount, done)
	go consume(client, *topicName, done)

	<-done

	ellapsed := time.Now().Sub(now)
	log.Printf("Finished in %s", ellapsed)
}
