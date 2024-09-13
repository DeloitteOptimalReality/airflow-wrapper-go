package codegen

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

//go:embed dag.tpl
var tmpl string

type Args struct {
	Email         string
	DependsOnPast bool
	Retries       int
	RetryDelay    int
}

// Function to convert an interface to a JSON string
func ToJSONString(v interface{}) (string, error) {
	jsonData, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

type StartDate struct {
	Day   int
	Month int
	Year  int
}

type Dag struct {
	ID          string
	Description string
	StartDate   StartDate
	Tags        []string
	DefaultArgs Args
}

type Connection struct {
	ConnectionID string
	Host         string
	Port         int
	Downstream   []string
}

type HttpOperator struct {
	TaskID       string
	ConnectionId string
	Name         string
	Endpoint     string
	Data         interface{}
	Downstream   []string
}

type GenData struct {
	DagDef      Dag
	Connections []Connection
	Tasks       []HttpOperator
}

func CheckDeps(deps []string) bool {
	return len(deps) > 0
}

func writeValueToBuffer(v interface{}, buf *bytes.Buffer) bool {
	switch v := v.(type) {
	case bool:
		if v {
			buf.WriteString("True")
		} else {
			buf.WriteString("False")
		}
	case string:
		buf.WriteString("\"")
		buf.WriteString(v)
		buf.WriteString("\"")
	default:
		buf.WriteString(fmt.Sprintf("%v", v))
	}
	return true
}

func ToMap(v interface{}) (map[string]interface{}, error) {
	var res map[string]interface{}
	fmt.Println("MARSHAlling")
	a, err := json.Marshal(v)
	if err != nil {
		return res, err
	}
	json.Unmarshal(a, &res)
	return res, err
}

// Function to convert a Go map to a Python dictionary string
func MapToPythonDict(m map[string]interface{}) (string, error) {
	var buf bytes.Buffer
	buf.WriteString("{")
	first := true
	for k, v := range m {
		if !first {
			buf.WriteString(", ")
		}
		first = false
		buf.WriteString("\"")
		buf.WriteString(k)
		buf.WriteString("\": ")
		switch v := v.(type) {
		case []interface{}:
			buf.WriteString("[")
			for _, k_val := range v {
				writeValueToBuffer(k_val, &buf)
			}
			buf.WriteString("]")
		case map[string]interface{}:
			nested, err := MapToPythonDict(v)
			if err != nil {
				return "", err
			}
			buf.WriteString(nested)
		default:
			writeValueToBuffer(v, &buf)
		}
	}
	buf.WriteString("}")
	return buf.String(), nil
}

func BoolTitle(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

// Function to transform the task ID
func TransformTaskID(taskID string) string {
	return "wt_" + strings.ReplaceAll(taskID, "-", "_")
}

func CreateDagGen(g GenData, directory string) (string, error) {
	data := g
	if data.DagDef.ID == "" {
		return "", fmt.Errorf("DAG ID is required")
	}

	// Prepare a map of original task IDs to transformed task IDs
	taskIDMap := make(map[string]string)
	for _, task := range data.Tasks {
		taskIDMap[task.TaskID] = TransformTaskID(task.TaskID)
	}

	t := template.New("dag").Funcs(
		template.FuncMap{
			"toJSONString":      ToJSONString,
			"toMap":             ToMap,
			"BoolTitle":         BoolTitle,
			"mapToPythonDict":   MapToPythonDict,
			"checkDeps":         CheckDeps,
			"transformTaskID":   TransformTaskID,
			"originalTaskIDMap": func() map[string]string { return taskIDMap },
		})
	tp, err := t.Parse(tmpl)

	if err != nil {
		return "", err
	}

	// Determine the file path
	fileName := data.DagDef.ID + ".py"
	if directory != "" {
		fileName = filepath.Join(directory, fileName)
	} else {
		// If directory is empty, save in current directory
		fileName = filepath.Join(".", fileName)
	}

	file, err := os.Create(fileName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	err = tp.Execute(file, data)
	if err != nil {
		return "", err
	}

	return "Airflow DAG script generated successfully.", nil
}
