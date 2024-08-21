package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

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

func (c *AirflowClient) GetAllDags() (*airflow.DAGCollection, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getDagsRequest := c.airflowClient.DAGApi.GetDags(ctx)

	dagDetails, resp, err := getDagsRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get all DAGs: %s", resp.Status)
	}

	return &dagDetails, nil
}

func (c *AirflowClient) UnpauseDag(dagId string) error {
	dag, err := c.GetDag(dagId)
	if err != nil {
		return err
	}

	isPaused := dag.IsPaused.Get()

	if isPaused == nil || *isPaused {
		err := c.unpauseDagRequest(dagId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *AirflowClient) unpauseDagRequest(dagId string) error {
	body := map[string]bool{"is_paused": false}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s://%s/api/v1/dags/%s?update_mask=is_paused", c.airflowConfig.Scheme, c.airflowConfig.Host, dagId)

	req, err := http.NewRequest("PATCH", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}

	req.SetBasicAuth(c.airflowCredentials.UserName, c.airflowCredentials.Password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("request failed")
	}

	return nil
}
