package client

import (
	"context"
	"fmt"

	"github.com/apache/airflow-client-go/airflow"
)

func (c *AirflowClient) GetDagRunTaskOutput(dagId string, dagRunId string, taskId string, xcomKey string) (airflow.XCom, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getXcomEntriesRequest := c.airflowClient.XComApi.GetXcomEntry(ctx, dagId, dagRunId, taskId, xcomKey)

	xcomEntry, resp, err := getXcomEntriesRequest.Execute()

	if err != nil {
		return xcomEntry, err
	}

	if resp.StatusCode != 200 {
		return xcomEntry, fmt.Errorf("failed to get logs: %s", resp.Status)
	}

	return xcomEntry, nil
}

func (c *AirflowClient) GetDagRunTaskOutputList(dagId string, dagRunId string, taskId string) ([]airflow.XCom, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getXcomEntriesRequest := c.airflowClient.XComApi.GetXcomEntries(ctx, dagId, dagRunId, taskId)

	xcomEntries, resp, err := getXcomEntriesRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get logs: %s", resp.Status)
	}

	outputList := []airflow.XCom{}
	for _, xcomEntry := range *xcomEntries.XcomEntries {
		if xcomEntry.Key == nil {
			return nil, fmt.Errorf("xcom entry key is nil")
		}

		output, err := c.GetDagRunTaskOutput(dagId, dagRunId, taskId, *xcomEntry.Key)
		if err != nil {
			return nil, err
		}

		outputList = append(outputList, output)
	}

	return outputList, nil
}

func (c *AirflowClient) GetLatestDagRunTaskOutputList(dagId string, taskId string) ([]airflow.XCom, error) {
	dagRun, err := c.GetLatestDagRun(dagId)
	if err != nil {
		return nil, err
	}

	return c.GetDagRunTaskOutputList(dagId, *dagRun.DagRunId.Get(), taskId)
}

func (c *AirflowClient) GetLatestDagRunTaskOutputMap(dagId string, taskId string) (map[string]interface{}, error) {
	outputList, err := c.GetLatestDagRunTaskOutputList(dagId, taskId)
	if err != nil {
		return nil, err
	}

	outputMap := map[string]interface{}{}
	for _, output := range outputList {
		outputMap[*output.Key] = output.Value
	}

	return outputMap, nil
}

func (c *AirflowClient) GetDagRunTaskOutputMap(dagId string, dagRunId string, taskId string) (map[string]interface{}, error) {
	outputList, err := c.GetDagRunTaskOutputList(dagId, dagRunId, taskId)
	if err != nil {
		return nil, err
	}

	outputMap := map[string]interface{}{}
	for _, output := range outputList {
		outputMap[*output.Key] = output.Value
	}

	return outputMap, nil
}

func (c *AirflowClient) GetAllDagRunTaskOutputMaps(dagId string, taskId string) ([]map[string]interface{}, error) {
	dagRuns, err := c.GetDagRunList(dagId)
	if err != nil {
		return nil, err
	}

	outputList := []map[string]interface{}{}
	for _, dagRun := range *dagRuns.DagRuns {
		outputMap, err := c.GetDagRunTaskOutputMap(dagId, *dagRun.DagRunId.Get(), taskId)
		if err != nil {
			return nil, err
		}

		outputList = append(outputList, outputMap)
	}

	return outputList, nil
}
