package kafka

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	kc *kafka.Consumer
}

func New() *Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	return &Consumer{
		kc: consumer,
	}
}

func (c *Consumer) registerConnector() *http.Response {
	plan, _ := ioutil.ReadFile("./debezium-connector.json")
	response, err := http.Post("http://localhost:8083/connectors/", "application/json", bytes.NewBuffer(plan))

	if err != nil {
		panic(err)
	}

	return response
}

func (c *Consumer) CheckConnector(url string) {
	response, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		c.registerConnector()
	}

	_, _ = ioutil.ReadAll(response.Body)
	response.Body.Close()
	// fmt.Println(string(body))
}

func (c *Consumer) SubscribeTopic(topic string) {
	err := c.kc.Subscribe(topic, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Subscribed to topic: %s\n", topic)
}

func (c *Consumer) ReadTopicMessages(cb func(*kafka.Message)) string {
	for {
		msg, err := c.kc.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		} else {
			cb(msg)
		}
	}
}

func (c *Consumer) CloseConsumer() {
	c.kc.Close()
}
