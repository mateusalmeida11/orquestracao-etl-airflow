from airflow.sdk import dag
from pendulum import datetime
from datetime import timedelta
import json
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

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


pipeline_flights_anac()