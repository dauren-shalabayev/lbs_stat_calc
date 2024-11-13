package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"example.com/m/cache"
	"example.com/m/models"
	"example.com/m/redis"
	"example.com/m/utils"
	"github.com/streadway/amqp"
)

const (
	rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	queueName   = "abons_log"
)

var (
	mu             sync.Mutex
	taskData       = make(map[int][]models.Abons)
	taskTimers     = make(map[int]*time.Timer) // таймеры для каждой задачи
	TASK_COMPLETED = "task_completed"
)

func generate(r *redis.Redis) {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Ошибка при открытии канала: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Ошибка при объявлении очереди: %v", err)
	}

	// abonsList := []models.Abons{
	// 	{
	// 		TaskID:     1,
	// 		SectorID:   99,
	// 		Command:    "task_completed",
	// 		Msisdn:     "1234567890",
	// 		Imsi:       "9876543210",
	// 		Lac:        1,
	// 		CellID:     1,
	// 		Service:    "Test Service",
	// 		Timestamp:  time.Now(),
	// 		ReturnCode: 0,
	// 	},
	// 	{
	// 		TaskID:     1,
	// 		SectorID:   100,
	// 		Command:    "block",
	// 		Msisdn:     "1234567890",
	// 		Imsi:       "9876543210",
	// 		Lac:        1,
	// 		CellID:     1,
	// 		Service:    "Test Service",
	// 		Timestamp:  time.Now(),
	// 		ReturnCode: 0,
	// 	},
	// 	{
	// 		TaskID:     1,
	// 		SectorID:   101,
	// 		Command:    "block",
	// 		Msisdn:     "1234567890",
	// 		Imsi:       "9876543210",
	// 		Lac:        1,
	// 		CellID:     1,
	// 		Service:    "Test Service",
	// 		Timestamp:  time.Now(),
	// 		ReturnCode: 0,
	// 	},
	// 	{
	// 		TaskID:     1,
	// 		SectorID:   102,
	// 		Command:    "block",
	// 		Msisdn:     "1234567890",
	// 		Imsi:       "9876543210",
	// 		Lac:        1,
	// 		CellID:     1,
	// 		Service:    "Test Service",
	// 		Timestamp:  time.Now(),
	// 		ReturnCode: 0,
	// 	},
	// 	{
	// 		TaskID:     2,
	// 		SectorID:   103,
	// 		Command:    "block",
	// 		Msisdn:     "1234567890",
	// 		Imsi:       "9876543210",
	// 		Lac:        1,
	// 		CellID:     1,
	// 		Service:    "Test Service",
	// 		Timestamp:  time.Now(),
	// 		ReturnCode: 0,
	// 	},
	// 	{
	// 		TaskID:     2,
	// 		SectorID:   104,
	// 		Command:    "block",
	// 		Msisdn:     "1234567890",
	// 		Imsi:       "9876543210",
	// 		Lac:        1,
	// 		CellID:     1,
	// 		Service:    "Test Service",
	// 		Timestamp:  time.Now(),
	// 		ReturnCode: 0,
	// 	},
	// 	{
	// 		TaskID:     2,
	// 		SectorID:   105,
	// 		Command:    "task_completed",
	// 		Msisdn:     "1234567890",
	// 		Imsi:       "9876543210",
	// 		Lac:        1,
	// 		CellID:     1,
	// 		Service:    "Test Service",
	// 		Timestamp:  time.Now(),
	// 		ReturnCode: 0,
	// 	},
	// }

	/* type Stat struct {
		SuccessBlocks   int `json:"success_blocks"`
		WrongBlocks     int `json:"wrong_blocks"`
		SuccessUnblocks int `json:"success_unblocks"`
		WrongUnblocks   int `json:"wrong_unblocks"`
	} */
	abonsList := []models.Abons{
		{
			TaskID:     1,
			SectorID:   100,
			Command:    "unblock",
			Msisdn:     "1234567890444",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "DT",
			Timestamp:  time.Now(),
			ReturnCode: -1,
		},
		{
			TaskID:     1,
			SectorID:   100,
			Command:    "unblock",
			Msisdn:     "1234567890444",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "VS",
			Timestamp:  time.Now(),
			ReturnCode: -1,
		},

		{
			TaskID:     1,
			SectorID:   100,
			Command:    "full_unblock",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "VS",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
		{
			TaskID:     1,
			SectorID:   101,
			Command:    "full_unblock",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "DT",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
		{
			TaskID:     1,
			SectorID:   102,
			Command:    "full_unblock",
			Msisdn:     "123456789222",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "VS",
			Timestamp:  time.Now(),
			ReturnCode: -1,
		},
		{
			TaskID:     1,
			SectorID:   102,
			Command:    "full_unblock",
			Msisdn:     "123456789222",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "DT",
			Timestamp:  time.Now(),
			ReturnCode: -1,
		},

		{
			TaskID:     1,
			SectorID:   99,
			Command:    "task_completed",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "VS",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
	}

	for _, abons := range abonsList {

		body, err := json.Marshal(abons)
		if err != nil {
			log.Printf("Ошибка при сериализации сообщения: %s", err)
			continue
		}

		err = ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			})
		if err != nil {
			log.Printf("Ошибка при отправке сообщения: %s", err)
		} else {
			fmt.Printf("Сообщение отправлено: %s, %s", abons.Command, time.Now())
			fmt.Println()
		}

		// if abons.Command == "task_completed" {
		// 	fmt.Print(time.Now())
		// 	time.Sleep(time.Second * 10)

		// }

		// if abons.Command == "block" && abons.SectorID == 101 {
		// 	fmt.Print(time.Now())
		// 	time.Sleep(time.Second * 30)

		// }
	}
	retryUnblock := make(map[string]models.CacheValue)

	unblockKeyStr := strconv.Itoa(1) + "_retry_unblock"
	retryUnblock["7018882233"] = models.CacheValue{
		LacCell:  "12345-67890",
		SectorID: 11,
		Imsi:     "imsi_test_1",
	}
	retryUnblock["7019992233"] = models.CacheValue{
		LacCell:  "54321-09876",
		SectorID: 1212,
		Imsi:     "imsi_test_2",
	}
	if err := cache.CacheData(unblockKeyStr, retryUnblock, r); err != nil {
		fmt.Println("ERRROROROOR")
	}
}

func main() {
	redisClient := redis.NewRedis("localhost:6379")

	defer redisClient.Close()
	go generate(redisClient)

	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Ошибка при открытии канала: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Ошибка при объявлении очереди: %v", err)
	}

	fmt.Println()

	msgs, err := ch.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Ошибка при получении сообщений: %v", err)
	}
	for d := range msgs {
		var abons models.Abons
		if err := json.Unmarshal(d.Body, &abons); err != nil {
			log.Printf("Ошибка при разборе сообщения: %s", err)
			continue
		}

		taskID := abons.TaskID

		taskData[taskID] = append(taskData[taskID], abons)
		fmt.Println(abons.ReturnCode, abons.Msisdn, abons.Service, abons.Command, time.Now())
		switch abons.Command {
		case TASK_COMPLETED:

			process(taskID, taskData[taskID], "UD", redisClient)
			delete(taskData, taskID)
			delete(taskTimers, taskID)

		}

	}
}

func process(taskID int, abonsData []models.Abons, operation string, r *redis.Redis) {
	wrongBlockStat, wrongUnblockStat, wrongFullUnblockStat, stat := CompareLog(abonsData, taskID, operation)
	taskReportJSON, err := GenerateTaskReportJSON(taskID, operation, stat)
	if err != nil {
		fmt.Println("failed to generate task report JSON:")
	}
	fmt.Println(taskReportJSON)
	GenerateRetryCache(taskID, wrongBlockStat, wrongUnblockStat, wrongFullUnblockStat, r)

}

func CompareLog(abonsData []models.Abons, key int, operation string) ([]models.Abons, []models.Abons, []models.Abons, map[int]map[string]*models.ServiceStats) {
	results := make(map[int]map[string]*models.ServiceStats)
	wrongBlockStat := []models.Abons{}
	wrongUnblockStat := []models.Abons{}
	wrongFullUnblockStat := []models.Abons{}

	log.Printf("Count abons_log: %d", len(abonsData)-1)
	// Проход по данным абонентов и сравнение с кэшем и предыдущими результатами
	for _, item := range abonsData {
		taskID := item.TaskID
		service := item.Service
		msisdn := item.Msisdn
		returnCode := item.ReturnCode
		command := item.Command
		imsi := item.Imsi
		lacCell, err := utils.NormalizeLocation(item.Lac, item.CellID)
		if err != nil {
			log.Fatalf("Failed to update Redis cache: %v", err)
		}
		sectorID := item.SectorID

		// Инициализация ServiceStats для заданного taskID и service, если она еще не существует
		if _, exists := results[taskID]; !exists {
			results[taskID] = make(map[string]*models.ServiceStats)
		}
		if _, exists := results[taskID][service]; !exists {
			results[taskID][service] = &models.ServiceStats{
				Service: service,
				Stat: models.Stat{
					SuccessBlocks:   0,
					WrongBlocks:     0,
					SuccessUnblocks: 0,
					WrongUnblocks:   0,
				},
			}
		}
		currentResult := results[taskID][service]

		if command == utils.BLOCK {
			if returnCode == 0 {
				currentResult.Stat.SuccessBlocks++
			} else {
				currentResult.Stat.WrongBlocks++
				wrongBlockStat = append(wrongBlockStat, item)
				log.Printf("Wrong block for MSISDN: %s, IMSI: %s, LacCell: %s, SectorID: %d, Command: %s", msisdn, imsi, lacCell, sectorID, command)
			}
		} else if command == utils.REFRESH {
			if returnCode == 0 {
				currentResult.Stat.SuccessBlocks++
			} else {
				currentResult.Stat.WrongBlocks++
				wrongBlockStat = append(wrongBlockStat, item)
				log.Printf("Wrong refresh for MSISDN: %s, IMSI: %s, LacCell: %s, SectorID: %d, Command: %s", msisdn, imsi, lacCell, sectorID, command)
			}
		} else if command == utils.UNBLOCK {
			if returnCode == 0 {
				currentResult.Stat.SuccessUnblocks++
			} else {
				currentResult.Stat.SuccessBlocks++
				currentResult.Stat.WrongUnblocks++
				wrongUnblockStat = append(wrongUnblockStat, item)
				log.Printf("Wrong unblock for MSISDN: %s, IMSI: %s, LacCell: %s, SectorID: %d, Command: %s", msisdn, imsi, lacCell, sectorID, command)
			}
		} else if command == utils.MOVE {
			if returnCode == 0 {
				currentResult.Stat.SuccessUnblocks++
			} else {
				currentResult.Stat.SuccessBlocks++
				currentResult.Stat.WrongUnblocks++
				wrongUnblockStat = append(wrongUnblockStat, item)
				log.Printf("Wrong move for MSISDN: %s, IMSI: %s, LacCell: %s, SectorID: %d, Command: %s", msisdn, imsi, lacCell, sectorID, command)
			}
		} else if command == utils.FULL_UNBLOCK {
			if returnCode == 0 {
				currentResult.Stat.SuccessUnblocks++
			} else {
				fmt.Println("ERROR WHILE FULL UNBLOCK")
				currentResult.Stat.SuccessBlocks++
				currentResult.Stat.WrongUnblocks++
				wrongUnblockStat = append(wrongFullUnblockStat, item)
				log.Printf("Wrong fullunblock for MSISDN: %s, IMSI: %s, LacCell: %s, SectorID: %d, Command: %s", msisdn, imsi, lacCell, sectorID, command)

			}
		}

	}

	return wrongBlockStat, wrongUnblockStat, wrongFullUnblockStat, results
}

func GenerateTaskReportJSON(taskID int, operation string, statResults map[int]map[string]*models.ServiceStats) ([]byte, error) {
	taskReport := models.TaskReport{
		TaskID:    taskID,
		Operation: operation,
		Services:  make([]models.ServiceStats, 0),
	}

	for _, serviceResult := range statResults[taskID] {
		if serviceResult.Service != "" {
			serviceReport := models.ServiceStats{
				Service: serviceResult.Service,
				Stat:    serviceResult.Stat,
			}
			taskReport.Services = append(taskReport.Services, serviceReport)
		}
	}
	fmt.Println("task_report", taskReport)
	taskReportJSON, err := json.Marshal(taskReport)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal task report JSON: %w", err)
	}

	return taskReportJSON, nil
}

func GenerateRetryCache(key int, wrongBlockStat []models.Abons, wrongUnblockStat []models.Abons, wrongFullUnblockStat []models.Abons, r *redis.Redis) error {
	// Удаление MSISDN из кэша блокировки при ошибке блокирования
	retryBlock := make(map[string]models.CacheValue)
	retryUnblock := make(map[string]models.CacheValue)

	// Обработка ошибочного блокирования
	if len(wrongBlockStat) > 0 {
		blockKeyStr := strconv.Itoa(key) + "_retry_block"
		for _, abons := range wrongBlockStat {
			lacCell, err := utils.NormalizeLocation(abons.Lac, abons.CellID)
			if err != nil {
				return fmt.Errorf("ошибка нормализации местоположения для абонента %s: %v", abons.Msisdn, err)
			}
			retryBlock[abons.Msisdn] = models.CacheValue{
				LacCell:  lacCell,
				SectorID: abons.SectorID,
				Imsi:     abons.Imsi,
			}
		}
		if err := cache.UpdateCache(blockKeyStr, retryBlock, r); err != nil {
			return fmt.Errorf("Ошибка обновления кэша блокировки: %v", err)
		}
	}

	// Обработка ошибочного разблокирования
	if len(wrongUnblockStat) > 0 {
		unblockKeyStr := strconv.Itoa(key) + "_retry_unblock"
		for _, abons := range wrongUnblockStat {
			lacCell, err := utils.NormalizeLocation(abons.Lac, abons.CellID)
			if err != nil {
				return fmt.Errorf("ошибка нормализации местоположения для абонента %s: %v", abons.Msisdn, err)
			}
			retryUnblock[abons.Msisdn] = models.CacheValue{
				LacCell:  lacCell,
				SectorID: abons.SectorID,
				Imsi:     abons.Imsi,
			}
			log.Printf("Не разблокированный абонент: %s", abons.Msisdn)
		}
		if err := cache.UpdateCache(unblockKeyStr, retryUnblock, r); err != nil {
			return fmt.Errorf("Ошибка обновления кэша разблокировки: %v", err)
		}
	} else {
		log.Printf("Нет данных по неразблокированным абонентам...")
	}

	if len(wrongFullUnblockStat) > 0 {
		unblockKeyStr := strconv.Itoa(key) + "_retry_fullunblock"
		for _, abons := range wrongFullUnblockStat {
			lacCell, err := utils.NormalizeLocation(abons.Lac, abons.CellID)
			if err != nil {
				return fmt.Errorf("ошибка нормализации местоположения для абонента %s: %v", abons.Msisdn, err)
			}
			retryUnblock[abons.Msisdn] = models.CacheValue{
				LacCell:  lacCell,
				SectorID: abons.SectorID,
				Imsi:     abons.Imsi,
			}
			log.Printf("Не разблокированный абонент: %s", abons.Msisdn)
		}
		if err := cache.UpdateCache(unblockKeyStr, retryUnblock, r); err != nil {
			return fmt.Errorf("Ошибка обновления кэша разблокировки: %v", err)
		}
	} else {
		log.Printf("Нет данных по неразблокированным абонентам...")
	}

	return nil
}
