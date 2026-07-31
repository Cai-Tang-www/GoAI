package redis

import (
	"context"
	"fmt"

	"GoAI/config"

	goredis "github.com/go-redis/redis/v8"
)

// New 创建 Redis 客户端并完成启动连通性探测。
func New(ctx context.Context, cfg *config.Config) (*goredis.Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("creating Redis client: config is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	client := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       0,
	})
	if _, err := client.Ping(ctx).Result(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("pinging Redis: %w", err)
	}
	return client, nil
}

// Close 关闭指定 Redis 客户端。
func Close(client *goredis.Client) error {
	if client == nil {
		return nil
	}
	if err := client.Close(); err != nil {
		return fmt.Errorf("closing Redis client: %w", err)
	}
	return nil
}
