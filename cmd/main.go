package main

import (
	"github.com/DeloitteOptimalReality/airflow-wrapper-go/pkg/client"
)

func main() {
	client.NewAirflowClient("localhost:8080", "http", "airflow", "airflow")
	// Test functions below
}
