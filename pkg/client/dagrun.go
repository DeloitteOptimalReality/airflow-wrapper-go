package client

import (
	"fmt"

	"github.com/apache/airflow-client-go/airflow"
	"golang.org/x/net/context"
)

func (c *AirflowClient) GetDagRun(dagId string, dagRunId string) (*airflow.DAGRun, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getDagRunRequest := c.airflowClient.DAGRunApi.GetDagRun(ctx, dagId, dagRunId)

	dagRun, resp, err := getDagRunRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get DAG details: %s", resp.Status)
	}

	return &dagRun, nil

}

func (c *AirflowClient) GetDagRunList(dagId string) (*airflow.DAGRunCollection, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getDagRunsRequest := c.airflowClient.DAGRunApi.GetDagRuns(ctx, dagId)

	dagRuns, resp, err := getDagRunsRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get DAG details: %s", resp.Status)
	}

	return &dagRuns, nil
}

func (c *AirflowClient) GetLatestDagRun(dagId string) (*airflow.DAGRun, error) {
	dagRuns, err := c.GetDagRunList(dagId)
	if err != nil {
		return nil, err
	}

	nRuns := len(*dagRuns.DagRuns)
	if len(*dagRuns.DagRuns) == 0 {
		return nil, fmt.Errorf("no dag runs found for %s", dagId)
	}

	latestDagRun := (*dagRuns.DagRuns)[nRuns-1]

	return &latestDagRun, nil
}
