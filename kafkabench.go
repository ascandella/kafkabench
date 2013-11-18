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
	clientId             = string(iota)
)

var (
	brokerAddress = flag.String("address", defaultBrokerAddress, "broker address to connect to")
	topicName     = flag.String("topic", defaultTopicName, "kafka topic to use")
	clientConfig  = sarama.ClientConfig{
		WaitForElection: time.Second,
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
	if flag.Args()[0] == "produce" {
		if topics, err := client.Topics(); err != nil {
			log.Fatal("Unable to list topics", err)
		} else {
			log.Println("Available topics", topics)
		}
	}
}
