package redis

import (
	"log"

	"context"

	"github.com/go-redis/redis/v8"
)

// Redis wraps the Redis client to provide custom methods.
type Redis struct {
	Client *redis.Client
	Ctx    context.Context
}

// NewRedis initializes and returns a Redis client.
func NewRedis(redisURL string) *Redis {
	log.Printf("RedisURL: %s", redisURL)
	rdb := redis.NewClient(&redis.Options{
		Addr: redisURL,
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	return &Redis{Client: rdb, Ctx: ctx}
}

// Delete deletes a key from Redis.
func (r *Redis) Delete(key string) error {
	return r.Client.Del(r.Ctx, key).Err()
}

// Set sets a key-value pair in Redis.
func (r *Redis) Set(key string, data []byte) error {
	return r.Client.Set(r.Ctx, key, data, 0).Err()
}

// Get retrieves a value by key from Redis.
func (r *Redis) Get(key string) ([]byte, error) {
	return r.Client.Get(r.Ctx, key).Bytes()
}

// Close closes the Redis client.
func (r *Redis) Close() {
	if err := r.Client.Close(); err != nil {
		log.Printf("Error on redis close: %s", err)
	}
}
