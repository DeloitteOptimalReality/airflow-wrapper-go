package client

import (
	"fmt"

	"github.com/apache/airflow-client-go/airflow"
	"golang.org/x/net/context"
)

func (c *AirflowClient) ExecuteDagRun(dagId string) (*airflow.DAGRun, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	executeDagRunRequest := c.airflowClient.DAGRunApi.PostDagRun(ctx, dagId)
	executeDagRunRequest = executeDagRunRequest.DAGRun(*airflow.NewDAGRunWithDefaults())

	dagRun, resp, err := executeDagRunRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to execute DAG: %s", resp.Status)
	}

	return &dagRun, nil
}
