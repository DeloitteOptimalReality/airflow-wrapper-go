package client

import (
	"fmt"
	"time"

	"github.com/apache/airflow-client-go/airflow"
	"golang.org/x/net/context"
)

func (c *AirflowClient) GetTaskInstance(dagId string, dagRunId string, taskId string) (airflow.TaskInstance, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getTaskInstanceRequest := c.airflowClient.TaskInstanceApi.GetTaskInstance(ctx, dagId, dagRunId, taskId)

	task, resp, err := getTaskInstanceRequest.Execute()

	if err != nil {
		return task, err
	}

	if resp.StatusCode != 200 {
		return task, fmt.Errorf("failed to get task: %s", resp.Status)
	}

	return task, nil
}

func (c *AirflowClient) GetTaskInstances(dagId string, dagRunId string) (airflow.TaskInstanceCollection, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getTaskInstancesRequest := c.airflowClient.TaskInstanceApi.GetTaskInstances(ctx, dagId, dagRunId)

	tasks, resp, err := getTaskInstancesRequest.Execute()

	if err != nil {
		return tasks, err
	}

	if resp.StatusCode != 200 {
		return tasks, fmt.Errorf("failed to get task: %s", resp.Status)
	}

	return tasks, nil
}

func (c *AirflowClient) GetTaskInstanceForAllDagRuns(dagId string, taskId string) ([]airflow.TaskInstance, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getDagRunsRequest := c.airflowClient.DAGRunApi.GetDagRuns(ctx, dagId)

	dagRuns, resp, err := getDagRunsRequest.Execute()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get DAG details: %s", resp.Status)
	}

	var tasksForAllDagRuns []airflow.TaskInstance
	for _, dagRun := range *dagRuns.DagRuns {
		taskInstance, err := c.GetTaskInstance(dagId, *dagRun.DagRunId.Get(), taskId)
		if err != nil {
			return nil, err
		}
		tasksForAllDagRuns = append(tasksForAllDagRuns, taskInstance)
	}

	return tasksForAllDagRuns, nil
}

func (c *AirflowClient) GetTaskInstancesForLatestDagRun(dagId string) (airflow.TaskInstanceCollection, error) {

	latestRun, err := c.GetLatestDagRun(dagId)
	if err != nil {
		return *airflow.NewTaskInstanceCollection(), err
	}

	latestTasks, err := c.GetTaskInstances(dagId, latestRun.GetDagRunId())
	if err != nil {
		return *airflow.NewTaskInstanceCollection(), err
	}

	return latestTasks, nil
}

func (c *AirflowClient) GetAllTaskInstancesAndDagRuns(dagId string) ([]airflow.TaskInstanceCollection, []DagRunInfo, error) {
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, c.airflowCredentials)
	getDagRunsRequest := c.airflowClient.DAGRunApi.GetDagRuns(ctx, dagId)

	dagRuns, resp, err := getDagRunsRequest.Execute()

	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode != 200 {
		return nil, nil, fmt.Errorf("failed to get DAG details: %s", resp.Status)
	}

	var allDagRunsInfo []DagRunInfo
	var allTaskInstances []airflow.TaskInstanceCollection
	for _, dagRun := range *dagRuns.DagRuns {

		// If the dag run hasn't completed, enddate = now
		startdate := *dagRun.StartDate.Get()
		enddate := time.Time{} // EndDate might be null so we initialise with empty time
		if dagRun.EndDate.Get() == nil {
			enddate = time.Now()
		}

		// Insert Dag run info into struct then append to dag list
		dagRunStruct := DagRunInfo{
			DagRunId:  string(*dagRun.DagRunId.Get()),
			StartDate: startdate,
			EndDate:   enddate,
			Status:    string(*dagRun.State),
		}
		allDagRunsInfo = append(allDagRunsInfo, dagRunStruct)

		// Get task instances then append to task list
		taskInstances, err := c.GetTaskInstances(dagId, *dagRun.DagRunId.Get())
		if err != nil {
			return nil, nil, err
		}
		allTaskInstances = append(allTaskInstances, taskInstances)
	}

	return allTaskInstances, allDagRunsInfo, err
}
