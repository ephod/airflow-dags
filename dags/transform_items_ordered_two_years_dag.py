import csv
from dataclasses import dataclass
import json
import logging
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from minio import Minio
import clevercsv

logger = logging.getLogger("airflow.task")


@dataclass
class MinioConfiguration:
    aws_access_key_id: str
    aws_secret_access_key: str
    host: str


default_args = {
    'owner': 'me',
    'schedule_interval': None,
    'catchup': False,
    'start_date': days_ago(1),
}


def sniff_csv(source_csv_file: Path, output_csv_file: Path) -> None:
    assert source_csv_file.is_file(), f"Wrong CSV file: {source_csv_file}"
    with source_csv_file.open(newline='') as original_fh:
        with output_csv_file.open('w', newline='', encoding='utf-8') as clean_fh:
            dialect = clevercsv.Sniffer().sniff(original_fh.read(), verbose=True)
            original_fh.seek(0)
            reader = clevercsv.reader(original_fh, dialect)
            rows = list(reader)
            writer = csv.writer(clean_fh)
            writer.writerows(rows)


def clean_text_file(source_file: Path):
    minio_connection = BaseHook.get_connection('MINIO_S3')
    minio_configuration = MinioConfiguration(**json.loads(minio_connection.get_extra()))
    minio_client = Minio(
        minio_configuration.host,
        access_key=minio_configuration.aws_access_key_id,
        secret_key=minio_configuration.aws_secret_access_key,
        secure=False)
    BUCKET_NAME = "datathon"
    result = minio_client.fget_object(BUCKET_NAME, str(source_file), source_file.name)
    logger.info(result)
    result_file = Path(source_file.name)
    if not result_file.is_file():
        raise AssertionError("Zip file doesn't exists")

    destination_file = Path('items_ordered_2years.csv')

    logger.info("Start sniffing file")
    sniff_csv(result_file, destination_file)
    logger.info("End sniffing file")

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/items-ordered-two-years/{destination_file.name}",
        str(destination_file),
        content_type="application/csv")


@dag(default_args=default_args, tags=['minio', 'raw', 'items_ordered_two_years'])
def transform_items_ordered_two_years_pipeline():
    @task()
    def start_steps():
        logger.info("Start steps")

    @task()
    def end_steps():
        logger.info("End steps")

    @task()
    def step_one():
        TXT_FILE = Path('raw/items-ordered-two-years/items_ordered_2years.txt')
        clean_text_file(TXT_FILE)

    start_step = start_steps()
    step_1_task = step_one()
    end_steps = end_steps()
    start_step >> step_1_task >> end_steps


transform_items_ordered_two_years_dag = transform_items_ordered_two_years_pipeline()
