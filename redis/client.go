package redis

import (
	"sync"

	"github.com/go-redis/redis/v8"
)

var once sync.Once
var rdb *redis.Client

func Client() *redis.Client {
	once.Do(func() {
		rdb = redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		})
	})
	return rdb
}
