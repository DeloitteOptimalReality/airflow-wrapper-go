package codegen

import (
	"fmt"
	"testing"

	"github.com/DeloitteOptimalReality/airflow-wrapper-go/pkg/codegen"
	"github.com/stretchr/testify/assert"
)

func TestCreateDagObject(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"test1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := codegen.GenData{DagDef: codegen.Dag{ID: ""}}
			if got, err := codegen.CreateDagGen(data, ""); false {
				if err != nil {
					t.Error()
				}
				fmt.Println(got)
				t.Errorf("CreateDagObject() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToMapValid(t *testing.T) {
	testStruct := codegen.HttpTask{
		TaskID:       "test_task_id",
		ConnectionId: "test_connection_id",
		Name:         "test_name",
		Endpoint:     "test_endpoint",
		Data:         "test_data",
		Downstream:   []string{"downstream_task_1", "downstream_task_2"},
	}
	res, err := codegen.ToMap(testStruct)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, res["Name"], "test_name")
	assert.Equal(t, res["ConnectionId"], "test_connection_id")
	assert.Equal(t, res["Name"], "test_name")
	assert.Equal(t, res["Endpoint"], "test_endpoint")
	assert.Equal(t, res["Data"], "test_data")
	assert.ElementsMatch(t, res["Downstream"], []string{"downstream_task_1", "downstream_task_2"})
}

// TODO: Test the invalid case for ToMap function. I'm not sure how to hit the edge case yet.

func TestToMapValidWithPythonTask(t *testing.T) {
	testStruct := codegen.PythonTask{
		TaskID:     "test_python_task_id",
		Name:       "test_name",
		Data:       map[string]interface{}{"key": "value"},
		Downstream: []string{"downstream_task_1", "downstream_task_2"},
		Upstream:   []string{"upstream_task_1"},
	}
	res, err := codegen.ToMap(testStruct)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, res["Name"], "test_name")
	assert.Equal(t, res["TaskID"], "test_python_task_id")
	assert.Equal(t, res["Data"], map[string]interface{}{"key": "value"})
	assert.ElementsMatch(t, res["Downstream"], []interface{}{"downstream_task_1", "downstream_task_2"})
	assert.ElementsMatch(t, res["Upstream"], []interface{}{"upstream_task_1"})
}

func TestMapToPythonDict(t *testing.T) {
	expectedPythonDict := `
	{
		"key1": {
			"ID": "dag_id",
			"Description": "dag_description",
			"StartDate": {
				"Day": 1,
				"Month": 1,
				"Year": 2024
			},
			"Tags": [],
			"DefaultArgs": {
				"Email": "test_email",
				"Retries": 1,
				"RetryDelay": 999
			}
		}
	}
	`

	testMap := map[string]interface{}{
		"key1": map[string]interface{}{
			"ID":          "dag_id",
			"Description": "dag_description",
			"StartDate": map[string]interface{}{
				"Day":   1,
				"Month": 1,
				"Year":  2024,
			},
			"Tags": []string{},
			"DefaultArgs": map[string]interface{}{
				"Email":      "test_email",
				"Retries":    1,
				"RetryDelay": 999,
			},
		},
	}
	res, err := codegen.MapToPythonDict(testMap)
	if err != nil {
		t.Error(err)
	}
	assert.JSONEq(t, res, expectedPythonDict)
}

func TestToJSONString(t *testing.T) {
	expectedJSONString := `
	{
		"key1": "value1"
	}
	`

	testMap := map[string]interface{}{
		"key1": "value1",
	}
	res, err := codegen.ToJSONString(testMap)
	if err != nil {
		t.Error(err)
	}
	assert.JSONEq(t, res, expectedJSONString)
}

func TestCheckDeps(t *testing.T) {
	assert.True(t, codegen.CheckDeps([]string{"dep_1"}))
	assert.True(t, codegen.CheckDeps([]string{"dep_1", "dep_2"}))
	assert.False(t, codegen.CheckDeps([]string{}))
}
