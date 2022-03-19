import csv
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from minio import Minio

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


def convert_iso_8559_1_to_utf_8_file(source_file: Path):
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
    destination_file = Path(f"{source_file.stem}_v1.csv")
    if not result_file.is_file():
        raise AssertionError("CSV file doesn't exists")
    text = b''
    with result_file.open("rb") as fh:
        text = fh.read()
    with destination_file.open('wb') as fh:
        fh.write(text.decode('iso-8859-1').encode('utf-8'))
    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products-categories/{destination_file.name}",
        str(destination_file),
        content_type="application/csv")


def transform_csv_file(source_file: Path):
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

    destination_file = Path("./products_categories_v2.csv")
    with result_file.open() as source_fh:
        with destination_file.open('w') as destination_fh:
            new_line = ""
            for line in source_fh.readlines():
                line = line.rstrip(os.linesep)
                line = line.strip("\"")
                line = line.replace("\"\"", "\"")
                if len(line.split(',')) == 4:
                    text = ['"{}"'.format(x) for x in list(csv.reader([line], delimiter=',', quotechar='"'))[0]]
                    destination_fh.write(f"{','.join(text)}{os.linesep}")
                    continue
                text = ['"{}"'.format(x) for x in list(csv.reader([line], delimiter=',', quotechar='"'))[0]]
                if len(text) == 5:
                    text[0] = text[0].strip('"')
                    text[1] = text[1].strip('"')
                    text = [
                        f'"{text[0]}{text[1]}"',
                        text[2],
                        text[3],
                        text[4],
                    ]
                destination_fh.write(f"{','.join(text)}{os.linesep}")

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products-categories/{destination_file.name}",
        str(destination_file),
        content_type="application/csv")


@dag(default_args=default_args, tags=['minio', 'raw', 'products_categories'])
def transform_products_categories_pipeline():
    @task()
    def start_steps():
        logger.info("Start steps")

    @task()
    def end_steps():
        logger.info("End steps")

    @task()
    def step_one():
        CSV_FILE = Path('raw/products-categories/products_categories.csv')
        convert_iso_8559_1_to_utf_8_file(CSV_FILE)

    @task()
    def step_two():
        CSV_FILE = Path('raw/products-categories/products_categories_v1.csv')
        transform_csv_file(CSV_FILE)

    start_step = start_steps()
    step_1_task = step_one()
    step_2_task = step_two()
    end_steps = end_steps()
    start_step >> step_1_task >> step_2_task >> end_steps


transform_products_categories_dag = transform_products_categories_pipeline()
