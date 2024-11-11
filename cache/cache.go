package cache

import (
	"encoding/json"
	"log"
	"strconv"

	"example.com/m/models"
	"example.com/m/redis"
	"github.com/valyala/gozstd"
)

type Cache interface {
	Get(key string) ([]byte, error)
}

func GetPreviousResult(r Cache, key int) (map[string]models.CacheValue, error) {
	b, err := r.Get(strconv.Itoa(key))
	if err != nil {
		return nil, err
	}

	decompB, err := gozstd.Decompress(nil, b)
	if err != nil {
		return nil, err
	}

	var prevRes map[string]models.CacheValue
	if err = json.Unmarshal(decompB, &prevRes); err != nil {
		return nil, err
	}
	return prevRes, nil
}

func GetUnblock(r *redis.Redis, key int) (map[string]models.CacheValue, error) {
	keyStr := strconv.Itoa(key) + "_unblock"
	b, err := r.Get(keyStr)
	if err != nil {
		return nil, err
	}

	decompB, err := gozstd.Decompress(nil, b)
	if err != nil {
		return nil, err
	}

	var prevRes map[string]models.CacheValue
	if err = json.Unmarshal(decompB, &prevRes); err != nil {
		return nil, err
	}
	return prevRes, nil
}

func CacheData(taskID string, subsCache map[string]models.CacheValue, r *redis.Redis) error {
	b, err := json.Marshal(subsCache)
	if err != nil {
		return err
	}
	compB := gozstd.CompressLevel(nil, b, 1)

	return r.Set(taskID, compB)
}

func UpdateCache(key string, cacheData map[string]models.CacheValue, r *redis.Redis) error {
	if err := CacheData(key, cacheData, r); err != nil {
		log.Printf("Ошибка обновления кэша: %v", err)
		return err
	}
	return nil
}
