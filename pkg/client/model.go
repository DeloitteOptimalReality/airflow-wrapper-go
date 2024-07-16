package client

import (
	"time"

	"github.com/apache/airflow-client-go/airflow"
)

// Define a struct for DAG run information
type DagRunInfo struct {
	DagId     string
	DagRunId  string // Could use airflow.NullableString but think processing to string will make it easier
	StartDate time.Time
	EndDate   time.Time
	Status    airflow.DagState
}
