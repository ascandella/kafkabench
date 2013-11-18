package main

import (
	"flag"
	"fmt"
)

const (
	defaultBrokerAddress = "127.0.0.1:9092"
	defaultTopicName     = "benchmark"
)

var (
	brokerAddress = flag.String("address", defaultBrokerAddress, "broker address to connect to")
	topicName     = flag.String("topic", defaultTopicName, "kafka topic to use")
)

func usage() {
	flag.Usage()
	fmt.Println()
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
	} else {
		fmt.Println("Nargs %d", flag.NArg())
		fmt.Printf("%s", flag.Args())
	}
}
