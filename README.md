# Introduction 
This is a wrapper library of the airflow-client-go library. The library is written in Go and allows your to create clients with interface methods to interact with the Apache Airflow REST API.

# Getting Started
To use this library, you need to have Go installed on your machine. You can download and install Go from [here](https://golang.org/dl/).

# Usage
To import the library in your project, you can use the following command:
```go
import "github.com/DeloitteOptimalReality/airflow-wrapper-go/pkg/client"
```

To setup a client, you can use the following code:
```go
c := client.NewAirflowClient("localhost:8080", "http", "airflow", "airflow")
```

To get a DAG, you can use the following code:
```go
dag, err := c.GetDag("dag_id")
```
