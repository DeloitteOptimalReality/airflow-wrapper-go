package client

import (
	"fmt"

	"github.com/apache/airflow-client-go/airflow"
	"golang.org/x/net/context"
)

func (c *AirflowClient) GetDag(dagId string) (*airflow.DAG, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getDagRequest := c.airflowClient.DAGApi.GetDag(ctx, dagId)

	dagDetails, resp, err := getDagRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get DAG details: %s", resp.Status)
	}

	return &dagDetails, nil
}
