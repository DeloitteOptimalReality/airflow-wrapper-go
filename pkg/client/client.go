package client

import (
	"github.com/apache/airflow-client-go/airflow"
)

type AirflowClientInterface interface {
	GetDag(dagId string) (*airflow.DAG, error)
	GetDagRun(dagId string, dagRunId string) (*airflow.DAGRun, error)
	GetDagRunList(dagId string) (*airflow.DAGRunCollection, error)
	GetLatestDagRun(dagId string) (*airflow.DAGRun, error)
	ExecuteDagRun(dagId string) (*airflow.DAGRun, error)
	GetLog(dagId string, dagRunId string, taskId string, taskTryNumber int32) (airflow.InlineResponse200, error)
	GetTaskLogs(dagId string, dagRunId string, taskId string) ([]airflow.InlineResponse200, error)
	GetTaskLog(dagId string, dagRunId string, taskId string) (*airflow.InlineResponse200, error)
	GetDagRunTaskOutput(dagId string, dagRunId string, taskId string, xcomKey string) (airflow.XCom, error)
	GetDagRunTaskOutputList(dagId string, dagRunId string, taskId string) ([]airflow.XCom, error)
	GetLatestDagRunTaskOutputList(dagId string, taskId string) ([]airflow.XCom, error)
	GetLatestDagRunTaskOutputMap(dagId string, taskId string) (map[string]interface{}, error)
	GetDagRunTaskOutputMap(dagId string, dagRunId string, taskId string) (map[string]interface{}, error)
	GetAllDagRunTaskOutputMaps(dagId string, taskId string) ([]map[string]interface{}, error)
	GetTaskInstance(dagId string, dagRunId string, taskId string) (airflow.TaskInstance, error)
	GetTaskInstances(dagId string, dagRunId string) (airflow.TaskInstanceCollection, error)
	GetTaskInstanceForAllDagRuns(dagId string, taskId string) ([]airflow.TaskInstance, error)
	GetTaskInstancesForLatestDagRun(dagId string) (airflow.TaskInstanceCollection, error)
	GetAllTaskInstancesAndDagRuns(dagId string) ([]airflow.TaskInstanceCollection, []DagRunInfo, error)
}

type AirflowClient struct {
	airflowClient      *airflow.APIClient
	airflowConfig      *airflow.Configuration
	airflowCredentials airflow.BasicAuth
}

func newAirflowConfiguration(host string, scheme string) *airflow.Configuration {
	conf := airflow.NewConfiguration()
	conf.Host = host
	conf.Scheme = scheme
	return conf
}

func NewAirflowClient(host string, scheme string, username string, password string) *AirflowClient {
	config := newAirflowConfiguration(host, scheme)
	cli := &AirflowClient{airflowClient: airflow.NewAPIClient(config)}
	cred := airflow.BasicAuth{UserName: username, Password: password}

	return &AirflowClient{
		airflowClient:      cli.airflowClient,
		airflowConfig:      config,
		airflowCredentials: cred,
	}
}
