package main

import (
	"bytes"
	"context"
	mykafka "debezium-postgres-redis-demo/app/kafka"
	"debezium-postgres-redis-demo/app/redis"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Message struct {
	Before map[string]interface{} `json:"before"`
	After  struct {
		Id        int     `json:"id"`
		Operation string  `json:"operation"`
		Amount    float64 `json:"amount"`
	} `json:"after"`
}

func prettyPrint(msg []byte) {
	var prettyJSON bytes.Buffer
	_ = json.Indent(&prettyJSON, msg, "", "\t")
	fmt.Println(prettyJSON.String())
}

func updateRedis(msg *kafka.Message) {
	rdb := redis.Client()
	prettyPrint(msg.Value)

	var m Message
	_ = json.Unmarshal(msg.Value, &m)
	key := "user_id:balance"
	err := rdb.IncrByFloat(context.Background(), key, m.After.Amount).Err()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Updated key: %s", key)
}

func main() {
	consumer := mykafka.New()
	consumer.CheckConnector("http://localhost:8083/connectors/payment_cashflow_connector")
	consumer.SubscribeTopic("postgres.public.CashFlow")
	consumer.ReadTopicMessages(updateRedis)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	fmt.Println("quitting.")
}
