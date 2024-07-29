# dag.tpl
import json
import os
from datetime import datetime, timedelta
from airflow.models.dag import DAG

from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.db import provide_session


# required so HTTP operator does not crash Python
os.environ["no_proxy"] = "*"
default_args = {
    'owner': 'airflow',
    'depends_on_past': {{.DagDef.DefaultArgs.DependsOnPast | BoolTitle}},
    'email': ['{{.DagDef.DefaultArgs.Email}}'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': {{.DagDef.DefaultArgs.Retries}},
    'retry_delay': timedelta(minutes={{.DagDef.DefaultArgs.RetryDelay}}),
}

dag = DAG(
    '{{.DagDef.ID}}',
    default_args=default_args,
    description='{{.DagDef.Description}}',
    schedule_interval=timedelta(days=1),
    start_date=datetime({{.DagDef.StartDate.Year}}, {{.DagDef.StartDate.Month}}, {{.DagDef.StartDate.Day}}),
    catchup=False,
    tags=[{{range $index, $tag := .DagDef.Tags}}{{if $index}}, {{end}}'{{$tag}}'{{end}}],
)

@provide_session
def create_http_connection(session=None):
    connection = session.query(Connection).filter(
        Connection.conn_id == '{{.ConnectionDef.ConnectionID}}').first()

    if connection:
        connection.conn_id = '{{.ConnectionDef.ConnectionID}}'
        connection.conn_type = 'http'
        connection.host = '{{.ConnectionDef.Host}}'
        connection.port = {{.ConnectionDef.Port}}
        session.commit()
    else:
        # Create a new connection if it doesn't exist
        connection = Connection(
            conn_id='{{.ConnectionDef.ConnectionID}}',
            conn_type='http',
            host='{{.ConnectionDef.Host}}',
            port={{.ConnectionDef.Port}}
        )
        session.add(connection)
        session.commit()

    return '{{.ConnectionDef.ConnectionID}}'

{{ transformTaskID .ConnectionDef.ConnectionID }} = PythonOperator(
    task_id='{{.ConnectionDef.ConnectionID}}',
    python_callable=create_http_connection,
    dag=dag,
)

# Dictionary to map transformed task IDs to original task IDs
task_id_map = {
    {{- range $original, $transformed := originalTaskIDMap }}
    "{{ $transformed }}": "{{ $original }}",
    {{- end }}
}

{{range .Tasks}}
{{ transformTaskID .TaskID }}_data = {{mapToPythonDict .Data}}

{{ transformTaskID .TaskID }} = SimpleHttpOperator(
    task_id='{{.TaskID}}',
    method='POST',
    http_conn_id='{{$.ConnectionDef.ConnectionID}}',
    endpoint='{{.Endpoint}}',
    data=json.dumps({{ transformTaskID .TaskID }}_data),
    headers={"Content-Type": "application/json"},
    log_response=True,
    dag=dag,
)
{{ end }}

{{- if checkDeps .ConnectionDef.Downstream }}
{{- range $dep := .ConnectionDef.Downstream}}
{{ transformTaskID $.ConnectionDef.ConnectionID }}.set_downstream({{ transformTaskID $dep }})
{{- end -}}
{{- end -}}

{{range .Tasks }}
{{if checkDeps .Downstream -}}
{{ $taskId := transformTaskID .TaskID -}}
{{range $dep := .Downstream -}}
{{$taskId}}.set_downstream({{ transformTaskID $dep }})
{{ end -}}
{{ end -}}
{{ end -}}