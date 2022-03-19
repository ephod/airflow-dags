import json
import logging
from dataclasses import dataclass
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook

from minio import Minio
from sqlalchemy import create_engine
from sqlalchemy import text

logger = logging.getLogger("airflow.task")


@dataclass
class MinioConfiguration:
    aws_access_key_id: str
    aws_secret_access_key: str
    host: str


def download_and_execute_sql_file(sql_file: Path) -> None:
    minio_connection = BaseHook.get_connection('MINIO_S3')
    minio_configuration = MinioConfiguration(**json.loads(minio_connection.get_extra()))
    minio_client = Minio(
        minio_configuration.host,
        access_key=minio_configuration.aws_access_key_id,
        secret_key=minio_configuration.aws_secret_access_key,
        secure=False)
    BUCKET_NAME = "datathon"

    result = minio_client.fget_object(BUCKET_NAME, str(sql_file), sql_file.name)
    logger.info(result)
    result = Path(sql_file.name)

    logger.info("Fill table from csv file")
    db_connection = BaseHook.get_connection('POSTGRES_DB')
    curs = create_engine(
        f"postgresql+psycopg2://{db_connection.login}:{db_connection.password}@{db_connection.host}:{db_connection.port}/{db_connection.schema}")
    with result.open('r', encoding='utf-8') as fh:
        conn = curs.raw_connection()
        query = text(fh.read())
        conn.execute(query)
        conn.commit()


default_args = {
    'owner': 'me',
    'schedule_interval': None,
    'catchup': False,
    'start_date': days_ago(1),
}


@dag(default_args=default_args, tags=['postgres', 'items_ordered_two_years', 'minio'])
def postgres_items_ordered_two_years_pipeline():
    @task()
    def start_db():
        logger.info("Start database")

    @task()
    def end_db():
        logger.info("End database")

    @task()
    def step_one():
        CSV_FILE = Path('bronze/step_01.sql')
        download_and_execute_sql_file(CSV_FILE)

    @task()
    def step_two():
        CSV_FILE = Path('bronze/step_02.sql')
        download_and_execute_sql_file(CSV_FILE)

    @task()
    def step_three():
        CSV_FILE = Path('bronze/step_03.sql')
        download_and_execute_sql_file(CSV_FILE)

    @task()
    def step_four():
        CSV_FILE = Path('bronze/step_04.sql')
        download_and_execute_sql_file(CSV_FILE)

    @task()
    def step_five():
        CSV_FILE = Path('bronze/step_05.sql')
        download_and_execute_sql_file(CSV_FILE)

    @task()
    def step_six():
        CSV_FILE = Path('bronze/step_06.sql')
        download_and_execute_sql_file(CSV_FILE)

    @task()
    def step_seven():
        CSV_FILE = Path('bronze/step_07.sql')
        download_and_execute_sql_file(CSV_FILE)

    @task()
    def step_eight():
        CSV_FILE = Path('bronze/step_08.sql')
        download_and_execute_sql_file(CSV_FILE)

    start_db = start_db()
    step_one = step_one()
    step_two = step_two()
    step_three = step_three()
    step_four = step_four()
    step_five = step_five()
    step_six = step_six()
    step_seven = step_seven()
    step_eight = step_eight()
    end_db = end_db()

    start_db >> step_one >> step_two >> step_three >> step_four >> step_five >> step_six >> step_seven >> step_eight >> end_db


postgres_items_ordered_two_years_dag = postgres_items_ordered_two_years_pipeline()
