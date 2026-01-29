from airflow.sdk import dag
from pendulum import datetime
from datetime import timedelta
import json
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import task, get_current_context

default_args = {
    "owner": "Mateus",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)

}

@dag(
    start_date=datetime(2025, 10, 31),
    end_date=datetime(2025, 11, 2),
    catchup=True,
    schedule="@daily",
    tags=["flights-anac"],
    max_active_runs=2,
    default_args=default_args
)
def pipeline_flights_anac():

    extract_data = LambdaInvokeFunctionOperator(
        task_id= "invoke_lambda_extract",
        function_name= "lambda_extract_api",
        invocation_type= "RequestResponse",
        aws_conn_id= "AWS",
        payload=json.dumps({"layer": "staging",
                            "bucket": "mateus-us-east-1-etl-flights",
                            "dt_voo": "{{ logical_date.strftime('%d%m%Y') }}"
                            })
    )

    @task
    def create_payload_dinamico(user_params= None):
        context = get_current_context()
        ti = context['ti']  # Pega o TaskInstance
        data_return = ti.xcom_pull(task_ids='invoke_lambda_extract', key='return_value')
        data = json.loads(data_return)
        payload = {
            "bucket": data.get("bucket"),
            "key": data.get("key")
            }
        if user_params:
            payload.update(user_params)
        return json.dumps(payload)

    create_payload_stg = create_payload_dinamico(user_params= {"table_name": "brazilian_flights_staging",
                                                               "checks_subpath": "staging_bronze_check.yml"
                                                               }
                                                               )
    run_data_quality_staging = LambdaInvokeFunctionOperator(
       task_id = "invoke_lambda_data_quality_staging",
       function_name = "lambda_data_quality_staging_bronze",
       invocation_type="RequestResponse",
       aws_conn_id= "AWS",
       payload=(create_payload_stg))
    
    extract_data >> create_payload_stg >> run_data_quality_staging


pipeline_flights_anac()