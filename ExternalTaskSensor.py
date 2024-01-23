import os
from datetime import timedelta
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.timezone import datetime
from airflow.models import Variable

from astronomer.providers.core.sensors.external_task import (
    ExternalDeploymentTaskSensorAsync,
)

DEPLOYMENT_CONN_ID = Variable.get("ASTRO_DEPLOYMENT_ID") # "https://clk72u9f3000a01jkq935xb34.astronomer.run/d6re3bz8"

# ASTRONOMER_KEY_ID = Variable.get("ASTRONOMER_KEY_ID")
ASTRONOMER_KEY_SECRET = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcGlUb2tlbklkIjoiY2xyamozZW84MDBhbjAxaWQzNmIzb2ZhMCIsImF1ZCI6ImFzdHJvbm9tZXItZWUiLCJpYXQiOjE3MDU2MDE0OTgsImlzQXN0cm9ub21lckdlbmVyYXRlZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6Ly9hcGkuYXN0cm9ub21lci5pbyIsImtpZCI6ImNsb2Q0aGtqejAya3AwMWozdWNqbzJwOHIiLCJwZXJtaXNzaW9ucyI6WyJhcGlUb2tlbklkOmNscmpqM2VvODAwYW4wMWlkMzZiM29mYTAiLCJkZXBsb3ltZW50SWQ6Y2xwaXJuZHpyNDU4MDY3djN6bXg2cmUzYno4Iiwib3JnYW5pemF0aW9uSWQ6Y2xrNzJ1OWYzMDAwYTAxamtxOTM1eGIzNCIsIm9yZ1Nob3J0TmFtZTpjbGs3MnU5ZjMwMDBhMDFqa3E5MzV4YjM0Iiwid29ya3NwYWNlSWQ6Y2xwaXJqMXA4MDNjZjAxb21zc2g0b2M1dSJdLCJzY29wZSI6ImFwaVRva2VuSWQ6Y2xyamozZW84MDBhbjAxaWQzNmIzb2ZhMCBkZXBsb3ltZW50SWQ6Y2xwaXJuZHpyNDU4MDY3djN6bXg2cmUzYno4IG9yZ2FuaXphdGlvbklkOmNsazcydTlmMzAwMGEwMWprcTkzNXhiMzQgb3JnU2hvcnROYW1lOmNsazcydTlmMzAwMGEwMWprcTkzNXhiMzQiLCJzdWIiOiJjbGx5Ynluam4wMGdoMDFtdmU4ajdycDJrIiwidmVyc2lvbiI6ImNscmpqM2VvODAwYW0wMWlkZHo2anZnaDQifQ.siz_GVm5orkIXe2pBG7-OLTMIV1cvMwAKOLEs0v874Tw8ZU3d1lzhqI723wKPlw8M4FyRwBYxCshCfmSs6DZGtrRhxB_7fY0nl0BYy3G1ZEtxON5lkqaSDSb3kQ5WEQU3VK5oVeMLGGwYd28HR6-coGIBmOwBfTaD-rKcp-3Qv7tid25nwc8NUDIaQGNamY1O79Q2_WRHcCJphicrVs4_n1zbop_WJ_yBhUUJqSNhGV8A1RzywvtxMCKKOA18hCyN3bVAsHjs_2bsIpjKFuV7j1hPVOANqpvXFIktveTq9xnprixFyjgdeIDS6CpUO7CTlk0jzlQc8OaFyhUGiATMQ"

DAG_ID = "dag_domain_recnwpg"

#Variable.get("DAG_ID")#"astronomer_monitoring_dag"#os.getenv("DAG_ID", "")

default_args = {
    "execution_timeout": timedelta(minutes=int(5)),
    "retries": 2 ,
    "retry_delay": timedelta(seconds=int(60)),
}
conn = BaseHook.get_connection(DEPLOYMENT_CONN_ID)

def astro_access_token() -> Dict[str, Any]:

    print(f"DEPLOYMENT_CONN_ID :: {DEPLOYMENT_CONN_ID}")

    """Get the Headers with access token by making post request with client_id and client_secret"""
    # ?limit=1&order_by=-dag_run_id
    print(f"Connection Host :: {conn.host}")
    post_url_val = f"{conn.host}/api/v1/dags/{DAG_ID}/dagRuns"
    print (f"Post URL value : { post_url_val }")

    token_resp = requests.get(
        url=post_url_val,
        headers={
            "Authorization": f"Bearer {ASTRONOMER_KEY_SECRET}",
            "Content-Type": "application/json"
        }
    )

    import json

    data = json.dumps(token_resp["dag_runs"])
    loader = json.loads(data)
    print(f"Dag RUN ID : {loader['dag_run_id'][-1]}")

    print(f"Tokent Response : {token_resp.text}")
    # data = json.load(token_resp.text)

    # latest_dag_run_id = data["dag_run_id"]
    # print (f"dag_run_id : { latest_dag_run_id } ")

    return {
        "cache-control": "no-cache",
        "content-type": "application/json",
        "accept": "application/json",
        "Authorization": "Bearer " + ASTRONOMER_KEY_SECRET,
    }
    

with DAG(
    dag_id="example_async_deployment_task_sensor",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "http"],
) as dag:
    # Task to Generate headers access token
    generate_header_access_token = PythonOperator(
        task_id="generate_header_access_token",
        python_callable=astro_access_token,
    )

    generate_header_access_token
