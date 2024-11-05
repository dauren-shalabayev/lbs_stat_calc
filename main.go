package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Abons struct {
	TaskID        int       `json:"task_id"`
	SectorID      int       `json:"sector_id"`
	SectorIDPrev  int       `json:"sector_id_prev"`
	Command       string    `json:"command"`
	Msisdn        string    `json:"msisdn"`
	Imsi          string    `json:"imsi"`
	Lac           int       `json:"lac"`
	CellID        int       `json:"cell_id"`
	LacPrev       int       `json:"lac_prev"`
	CellIDPrev    int       `json:"cell_id_prev"`
	Service       string    `json:"service"`
	Timestamp     time.Time `json:"timestamp"`
	ReturnCode    int       `json:"return_code"`
	Result        string    `json:"result"`
	Reason        string    `json:"reason"`
	ActionResults []string  `json:"action_results"`
}

const (
	rabbitMQURL = "amqp://guest:guest@localhost:5672/" // Замените на ваши данные подключения
	queueName   = "abons_log"                          // Название очереди
)

var (
	mu             sync.Mutex
	taskData       = make(map[int][]Abons)
	taskTimers     = make(map[int]*time.Timer) // таймеры для каждой задачи
	TASK_COMPLETED = "task_completed"
)

func generate() {
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

	abonsList := []Abons{
		{
			TaskID:     1,
			SectorID:   101,
			Command:    "task_completed",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "Test Service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
		{
			TaskID:     1,
			SectorID:   101,
			Command:    "block",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "Test Service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
		{
			TaskID:     1,
			SectorID:   101,
			Command:    "block",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "Test Service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
		{
			TaskID:     1,
			SectorID:   101,
			Command:    "block",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "Test Service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
		{
			TaskID:     2,
			SectorID:   101,
			Command:    "block",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "Test Service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
		{
			TaskID:     2,
			SectorID:   101,
			Command:    "block",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "Test Service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
		},
		{
			TaskID:     2,
			SectorID:   101,
			Command:    "task_completed",
			Msisdn:     "1234567890",
			Imsi:       "9876543210",
			Lac:        1,
			CellID:     1,
			Service:    "Test Service",
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

		if abons.Command == "task_completed" {
			fmt.Print(time.Now())
			time.Sleep(time.Second * 30)

		}
	}
}

func main() {

	go generate()

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
		var abons Abons
		if err := json.Unmarshal(d.Body, &abons); err != nil {
			log.Printf("Ошибка при разборе сообщения: %s", err)
			continue
		}

		taskID := abons.TaskID

		taskData[taskID] = append(taskData[taskID], abons)
		fmt.Println(abons.ReturnCode, abons.Msisdn, abons.Service, abons.Command, time.Now())
		// switch abons.Command {
		// case TASK_COMPLETED:
		if timer, exists := taskTimers[taskID]; exists {
			timer.Stop()
		}
		taskTimers[taskID] = time.AfterFunc(5*time.Second, func() {
			mu.Lock()
			defer mu.Unlock()

			process(taskID, taskData[taskID])
			delete(taskData, taskID)
			delete(taskTimers, taskID)

		})
		// }

	}
}

func process(taskID int, abonsList []Abons) {
	fmt.Println(time.Now())
	fmt.Printf("Обработка taskID %d: process start...\n", taskID)
	result := make(map[int]int)

	for _, abons := range abonsList {
		if abons.ReturnCode == 0 && abons.Command == "block" {

			result[abons.TaskID] += 1
		}
	}

	fmt.Println("отправляем результаты", result)

}
