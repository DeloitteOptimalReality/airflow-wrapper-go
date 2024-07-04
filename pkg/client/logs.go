package client

import (
	"context"
	"fmt"

	"github.com/apache/airflow-client-go/airflow"
)

func (c *AirflowClient) GetLog(dagId string, dagRunId string, taskId string, taskTryNumber int32) (airflow.InlineResponse200, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getLogsRequest := c.airflowClient.TaskInstanceApi.GetLog(ctx, dagId, dagRunId, taskId, taskTryNumber)

	logs, resp, err := getLogsRequest.Execute()

	if err != nil {
		return logs, err
	}

	if resp.StatusCode != 200 {
		return logs, fmt.Errorf("failed to get logs: %s", resp.Status)
	}

	return logs, nil
}

func (c *AirflowClient) GetTaskLogs(dagId string, dagRunId string) ([]airflow.InlineResponse200, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getTaskInstancesRequest := c.airflowClient.TaskInstanceApi.GetTaskInstances(ctx, dagId, dagRunId)

	tasks, resp, err := getTaskInstancesRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get task: %s", resp.Status)
	}

	var taskLogList []airflow.InlineResponse200
	for _, taskInstance := range *tasks.TaskInstances {
		getTaskLog, err := c.GetLog(dagId, dagRunId, *taskInstance.TaskId, *taskInstance.TryNumber)
		if err != nil {
			return nil, err
		}
		taskLogList = append(taskLogList, getTaskLog)
	}

	return taskLogList, nil
}

func (c *AirflowClient) GetTaskLog(dagId string, dagRunId string, taskId string) (*airflow.InlineResponse200, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getTaskInstanceRequest := c.airflowClient.TaskInstanceApi.GetTaskInstance(ctx, dagId, dagRunId, taskId)

	task, resp, err := getTaskInstanceRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get task: %s", resp.Status)
	}

	taskLog, err := c.GetLog(dagId, dagRunId, taskId, *task.TryNumber)
	if err != nil {
		return nil, err
	}
	return &taskLog, nil
}
