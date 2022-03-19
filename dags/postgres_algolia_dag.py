from dataclasses import dataclass
import json
import logging
from pathlib import Path

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import jsonlines
from minio import Minio
from psycopg2.extras import Json
from sqlalchemy import create_engine

logger = logging.getLogger("airflow.task")


@dataclass
class MinioConfiguration:
    aws_access_key_id: str
    aws_secret_access_key: str
    host: str


def download_and_insert_into_table(jsonl_file: Path) -> None:
    minio_connection = BaseHook.get_connection('MINIO_S3')
    minio_configuration = MinioConfiguration(**json.loads(minio_connection.get_extra()))
    minio_client = Minio(
        minio_configuration.host,
        access_key=minio_configuration.aws_access_key_id,
        secret_key=minio_configuration.aws_secret_access_key,
        secure=False)
    BUCKET_NAME = "datathon"

    result = minio_client.fget_object(BUCKET_NAME, str(jsonl_file), jsonl_file.name)
    logger.info(result)
    result = Path(jsonl_file.name)

    temp_file = Path('./insert_into_algolia.sql')
    logger.info("Iterate over JSON lines")
    db_connection = BaseHook.get_connection('POSTGRES_DB')
    curs = create_engine(
        f"postgresql+psycopg2://{db_connection.login}:{db_connection.password}@{db_connection.host}:{db_connection.port}/{db_connection.schema}")
    with temp_file.open('w') as fh:
        with jsonlines.open(str(result)) as reader:
            for obj in reader:
                for hit in obj["hits"]:
                    curs.execute("insert into algolia (data) values (%s)", [Json(hit)])


default_args = {
    'owner': 'me',
    'schedule_interval': None,
    'catchup': False,
    'start_date': days_ago(1),
}


@dag(default_args=default_args, tags=['postgres', 'algolia', 'minio'])
def postgres_algolia_pipeline():
    @task()
    def start_db():
        logger.info("Start database")

    @task()
    def end_db():
        logger.info("End database")

    @task()
    def insert_into_table():
        JSON_LINE_FILE = Path('raw/algolia/algolia.jsonl')
        download_and_insert_into_table(JSON_LINE_FILE)

    start_db = start_db()
    CREATE_TABLE = Path('./dags/sql/create_table_algolia.sql')
    create_algolia_table = PostgresOperator(
        task_id="create_algolia_table",
        postgres_conn_id='POSTGRES_DB',
        sql=CREATE_TABLE.read_text(),
    )
    insert_into_table = insert_into_table()
    end_db = end_db()

    start_db >> create_algolia_table >> insert_into_table >> end_db


postgres_algolia_dag = postgres_algolia_pipeline()
