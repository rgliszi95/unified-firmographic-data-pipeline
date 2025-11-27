from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import boto3
from datetime import datetime
import requests
import pandas as pd
import json
import os
from bs4 import BeautifulSoup

S3_BUCKET = "firmographics-landing-2025"
S3_PREFIX = "firmographics/raw/"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}


def fetch_fortune_500():
    url = "https://fortune.com/api/getRankingSearchYear/fortune500/2025/"
    r = requests.get(url)
    r.raise_for_status()

    data = r.json()

    s3 = S3Hook(aws_conn_id="aws_conn")
    key = f"{S3_PREFIX}fortune500_2025.json"

    s3.load_string(
        string_data=json.dumps(data),
        key=key,
        bucket_name=S3_BUCKET,
        replace=True,
    )



def scrape_wikipedia_sp500():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    html = requests.get(url, headers=headers).text

    tables = pd.read_html(html)
    df = tables[1]  # main S&P 500 table

    # convert rows to list-of-dicts → JSON array
    json_records = df.to_dict(orient="records")

    s3 = S3Hook(aws_conn_id="aws_conn")
    key = f"{S3_PREFIX}sp500.json"

    s3.load_string(
        string_data=json.dumps(json_records),
        key=key,
        bucket_name=S3_BUCKET,
        replace=True,
    )



@task
def upload_dbt_artifacts():
    s3 = S3Hook(aws_conn_id="aws_conn")  

    local_target = "./firmographics_dbt/target"
    run_results_path = os.path.join(local_target, "run_results.json")

    # Read invocation_id from run_results.json
    with open(run_results_path) as f:
        run_results = json.load(f)
        invocation_id = run_results.get("metadata", {}).get("invocation_id")
        if not invocation_id:
            raise ValueError("Could not find invocation_id in run_results.json")

    artifact_files = ["manifest.json", "run_results.json"]

    # Upload artifacts
    for file_name in artifact_files:
        local_path = os.path.join(local_target, file_name)
        s3_key = f"dbt_artifacts/{invocation_id}/{file_name}"
        s3.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True
        )
        print(f"Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")

    # Upload docs site
    for root, dirs, files in os.walk(local_target):
        for f in files:
            if f.endswith((".html", ".json", ".css", ".js")):
                local_path = os.path.join(root, f)
                rel_path = os.path.relpath(local_path, local_target)
                s3_key = f"dbt_docs/{invocation_id}/{rel_path}"
                s3.load_file(
                    filename=local_path,
                    key=s3_key,
                    bucket_name=S3_BUCKET,
                    replace=True
                )
                print(f"Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")


# Snowflake COPY INTO commands
COPY_FORTUNE = """
COPY INTO COMPANY_DATA.RAW.FORTUNE_500 (source, ingested_at, payload)
FROM (
    SELECT
        'fortune500' AS source,
        CURRENT_TIMESTAMP() AS ingested_at,
        $1 AS payload
    FROM @COMPANY_DATA.RAW.S3_STAGE/firmographics/raw/
)
FILE_FORMAT = (TYPE = JSON)
PATTERN = '.*fortune500_2025.json';
"""


COPY_WIKI = """
COPY INTO COMPANY_DATA.RAW.WIKI_SP500 (source, ingested_at, payload)
FROM (
    SELECT
        'wikipedia_sp500' AS source,
        CURRENT_TIMESTAMP() AS ingested_at,
        $1 AS payload
    FROM @COMPANY_DATA.RAW.S3_STAGE/firmographics/raw/
)
FILE_FORMAT = (TYPE = JSON)
PATTERN = '.*sp500.json';
"""



with DAG("firmographics_ingestion",
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    extract_fortune = PythonOperator(
        task_id="extract_fortune",
        python_callable=fetch_fortune_500
    )

    scrape_sp500 = PythonOperator(
        task_id="scrape_sp500",
        python_callable=scrape_wikipedia_sp500
    )

    load_fortune = SnowflakeOperator(
        task_id="load_fortune",
        snowflake_conn_id="snowflake_conn",
        sql=COPY_FORTUNE
    )

    load_sp500 = SnowflakeOperator(
        task_id="load_sp500",
        snowflake_conn_id="snowflake_conn",
        sql=COPY_WIKI
    )

    dbt_run_stage = BashOperator(
        task_id="dbt_run_stage",
        bash_command="cd /opt/airflow/firmographics_dbt && dbt run --select models/staging --profiles-dir /opt/airflow/.dbt"
    )

    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command="cd /opt/airflow/firmographics_dbt && dbt run --select models/core --profiles-dir /opt/airflow/.dbt"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/firmographics_dbt && dbt test --profiles-dir /opt/airflow/.dbt"
    )

    dbt_run_analytics = BashOperator(
        task_id="dbt_run_analytics",
        bash_command="cd /opt/airflow/firmographics_dbt && dbt run --select models/star --profiles-dir /opt/airflow/.dbt"
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command="cd /opt/airflow/firmographics_dbt && dbt docs generate --profiles-dir /opt/airflow/.dbt"
    )

    upload_artifacts = upload_dbt_artifacts()

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/firmographics_dbt && dbt snapshot --profiles-dir /opt/airflow/.dbt"
    )

    extract_fortune >> load_fortune
    scrape_sp500 >> load_sp500
    [load_fortune, load_sp500] >> dbt_run_stage >> dbt_run_core >> dbt_snapshot >> dbt_run_analytics >> dbt_test >> dbt_docs >> upload_artifacts