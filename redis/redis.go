package redis

import (
	"context"
	"fmt"
	"log"

	"GoAI/config"

	"github.com/go-redis/redis/v8"
)

var Rdb *redis.Client
var ctx = context.Background()

// InitRedis 初始化 Redis 客户端 提供Redis实例
func InitRedis() {
	Rdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("localhost:%d", config.AppConfig.RedisPort),
		Password: config.AppConfig.RedisPassword,
		DB:       0, // use default DB
	})

	_, err := Rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	log.Println("Redis connected successfully!")
}
