import json
import logging
from dataclasses import dataclass
from pathlib import Path

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook

from minio import Minio
from sqlalchemy import create_engine

logger = logging.getLogger("airflow.task")


@dataclass
class MinioConfiguration:
    aws_access_key_id: str
    aws_secret_access_key: str
    host: str


def download_and_insert_into_table(csv_file: Path) -> None:
    minio_connection = BaseHook.get_connection('MINIO_S3')
    minio_configuration = MinioConfiguration(**json.loads(minio_connection.get_extra()))
    minio_client = Minio(
        minio_configuration.host,
        access_key=minio_configuration.aws_access_key_id,
        secret_key=minio_configuration.aws_secret_access_key,
        secure=False)
    BUCKET_NAME = "datathon"

    result = minio_client.fget_object(BUCKET_NAME, str(csv_file), csv_file.name)
    logger.info(result)
    result = Path(csv_file.name)

    logger.info("Fill table from csv file")
    db_connection = BaseHook.get_connection('POSTGRES_DB')
    curs = create_engine(
        f"postgresql+psycopg2://{db_connection.login}:{db_connection.password}@{db_connection.host}:{db_connection.port}/{db_connection.schema}")
    with result.open('r', encoding='utf-8') as fh:
        next(fh)
        conn = curs.raw_connection()
        cursor = conn.cursor()
        cmd = """COPY items_ordered_two_years(
            num_order,
            item_id,
            created_at,
            product_id,
            qty_ordered,
            base_cost,
            price,
            discount_percent,
            customer_id,
            city,
            zipcode)
        FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"""
        cursor.copy_expert(cmd, fh)
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
    def insert_into_table():
        CSV_FILE = Path('raw/items-ordered-two-years/items_ordered_two_years_v1.csv')
        download_and_insert_into_table(CSV_FILE)

    start_db = start_db()
    CREATE_TABLE = Path('./dags/sql/create_table_items_ordered_two_years.sql')
    create_items_ordered_two_years_table = PostgresOperator(
        task_id="create_items_ordered_two_years_table",
        postgres_conn_id='POSTGRES_DB',
        sql=CREATE_TABLE.read_text(),
    )
    insert_into_table = insert_into_table()
    end_db = end_db()

    start_db >> create_items_ordered_two_years_table >> insert_into_table >> end_db


postgres_items_ordered_two_years_dag = postgres_items_ordered_two_years_pipeline()
