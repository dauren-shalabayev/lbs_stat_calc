package models

import "time"

type ServiceStats struct {
	Service string `json:"service"`
	Stat    Stat   `json:"stat"`
}

type Stat struct {
	SuccessBlocks   int `json:"success_blocks"`
	WrongBlocks     int `json:"wrong_blocks"`
	SuccessUnblocks int `json:"success_unblocks"`
	WrongUnblocks   int `json:"wrong_unblocks"`
}

type TaskReport struct {
	TaskID    int            `json:"task_id"`
	Operation string         `json:"operation"`
	Services  []ServiceStats `json:"services"`
}

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

type CacheValue struct {
	Imsi     string
	LacCell  string
	SectorID int
}
