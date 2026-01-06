from airflow.sdk import dag
from pendulum import datetime
from datetime import timedelta
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

default_args = {
    "owner": "Mateus",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)

}

@dag(
    start_date=datetime(2025, 10, 31),
    end_date=datetime(2025, 11, 30),
    schedule="@daily",
    tags=["flights-anac"],
    max_active_runs=2,
    default_args=default_args
)
def pipeline_flights_anac():
    pass