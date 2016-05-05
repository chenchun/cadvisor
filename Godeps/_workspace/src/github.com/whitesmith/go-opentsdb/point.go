package opentsdb

import (
	"encoding/json"
)

type Point struct {
	// Required
	// Opentsdb metric e.g.: "sys.info.cpu"
	Metric string `json:"metric"`

	// Required
	// Timestamp unix time e.g.: time.Now().Unix()
	Timestamp int64 `json:"timestamp"`

	// Required
	// Value to save, this can be either integer(8,16,32,64), float32
	// Different types will have unexpected behaviour
	Value interface{} `json:"value"`

	// Required
	// Map of tags, example: {"host": "deskop"}
	Tags map[string]string `json:"tags"`
}

func NewPoint(metric string, timestamp int64, value interface{}, tags map[string]string) *Point {
	return &Point{
		Metric:    metric,
		Timestamp: timestamp,
		Value:     value,
		Tags:      tags,
	}
}

type BatchPoints struct {
	Points []*Point `json:""`
}

func NewBatchPoints() *BatchPoints {
	return &BatchPoints{}
}

func (bp *BatchPoints) AddPoint(p *Point) {
	bp.Points = append(bp.Points, p)
}

func (bp *BatchPoints) ToJson() ([]byte, error) {
	return json.Marshal(bp.Points)
}
