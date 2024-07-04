package client

import (
	"time"
)

// Define a struct for DAG run information
type DagRunInfo struct {
	DagRunId  string // Could use airflow.NullableString but think processing to string will make it easier
	StartDate time.Time
	EndDate   time.Time
	Status    string
}
