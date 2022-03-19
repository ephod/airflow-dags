import csv
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import json
import os
from pathlib import Path
import zipfile

import clevercsv
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from minio import Minio
from minio.error import S3Error

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


def split_into_header_and_body(source_file: Path):
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
        raise AssertionError("CSV file doesn't exists")
    header_file = Path(f"{source_file.stem}_header.csv")
    header = ''
    body_file = Path(f"{source_file.stem}_body.csv")
    body = ['']
    with result_file.open("r") as fh:
        header = next(fh)
        body = fh.readlines()

    with header_file.open('w') as fh:
        fh.write(header)
    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products/{header_file.name}",
        str(header_file),
        content_type="application/csv")

    with body_file.open('w') as fh:
        fh.writelines(body)
    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products/{body_file.name}",
        str(body_file),
        content_type="application/csv")


def transform_csv_file_step_two(source_file: Path):
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

    destination_file = Path("./products_body_v2.csv")
    with result_file.open() as source_fh:
        with destination_file.open('w') as destination_fh:
            new_line = ""
            for line in source_fh.readlines():
                if ",https://" in line:
                    destination_fh.write(f"{new_line}{line}")
                    new_line = ""
                else:
                    new_line += line.rstrip(os.linesep)

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products/{destination_file.name}",
        str(destination_file),
        content_type="application/csv")


def transform_csv_file_step_three(source_file: Path):
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

    destination_file = Path("./products_body_v3.csv")
    with result_file.open() as source_fh:
        with destination_file.open('w') as destination_fh:
            haystack = "</p>\",,"
            for i, line in enumerate(source_fh.readlines(), 1):
                if haystack in line:
                    left = line[:line.find(haystack) + 7]
                    right = line[line.find(haystack) + 7:]
                    if not right.startswith("http"):
                        destination_fh.write(f"{left}{os.linesep}")
                        destination_fh.write(right)
                    else:
                        destination_fh.write(line)
                else:
                    destination_fh.write(line)

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products/{destination_file.name}",
        str(destination_file),
        content_type="application/csv")


def transform_csv_file_step_four(source_file: Path):
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

    destination_file = Path("./products_body_v4.csv")
    with result_file.open() as source_fh:
        with destination_file.open('w') as destination_fh:
            for i, line in enumerate(source_fh.readlines(), 1):
                line = line.replace(u"\u00a0", " ")
                line = line.replace(u"\u2013", "-")  # 4910
                line = line.replace(u"\u2019", "'")  # 6511
                line = line.replace(u"\u00ad", "")  # 7204
                line = line.replace(u"\u201c", "\"\"")  # 9412
                line = line.replace(u"\u201d", "\"\"")  # 9412
                line = line.replace(u"\u200b", " ")  # 15728
                destination_fh.write(line)

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products/{destination_file.name}",
        str(destination_file),
        content_type="application/csv")


def transform_csv_file_step_five(source_header_file: Path, source_body_file: Path):
    minio_connection = BaseHook.get_connection('MINIO_S3')
    minio_configuration = MinioConfiguration(**json.loads(minio_connection.get_extra()))
    minio_client = Minio(
        minio_configuration.host,
        access_key=minio_configuration.aws_access_key_id,
        secret_key=minio_configuration.aws_secret_access_key,
        secure=False)
    BUCKET_NAME = "datathon"

    result_header = minio_client.fget_object(BUCKET_NAME, str(source_header_file), source_header_file.name)
    logger.info(result_header)
    result_header = Path(source_header_file.name)

    result_body = minio_client.fget_object(BUCKET_NAME, str(source_body_file), source_body_file.name)
    logger.info(result_body)
    result_body = Path(source_body_file.name)

    destination_file = Path("./products_rejoined_v1.csv")

    with result_header.open() as source_fh:
        with destination_file.open('w') as destination_fh:
            destination_fh.write(source_fh.readline())

    with result_body.open() as source_fh:
        with destination_file.open('a') as destination_fh:
            destination_fh.writelines(source_fh.readlines())

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products/{destination_file.name}",
        str(destination_file),
        content_type="application/csv")


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
        raise AssertionError("File doesn't exists")

    destination_file = Path('products_rejoined_v2.csv')

    logger.info("Start sniffing file")
    sniff_csv(result_file, destination_file)
    logger.info("End sniffing file")

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/products/{destination_file.name}",
        str(destination_file),
        content_type="application/csv")


@dag(default_args=default_args, tags=['minio', 'raw', 'products'])
def transform_products_pipeline():
    @task()
    def start_steps():
        logger.info("Start steps")

    @task()
    def end_steps():
        logger.info("End steps")

    @task()
    def step_one():
        CSV_FILE = Path('raw/products/products.csv')
        split_into_header_and_body(CSV_FILE)

    @task()
    def step_two():
        CSV_FILE = Path('raw/products/products_body.csv')
        transform_csv_file_step_two(CSV_FILE)

    @task()
    def step_three():
        CSV_FILE = Path('raw/products/products_body_v2.csv')
        transform_csv_file_step_three(CSV_FILE)

    @task()
    def step_four():
        CSV_FILE = Path('raw/products/products_body_v3.csv')
        transform_csv_file_step_four(CSV_FILE)

    @task()
    def step_five():
        HEADER_FILE = Path('raw/products/products_header.csv')
        BODY_FILE = Path('raw/products/products_body_v6.csv')
        transform_csv_file_step_five(HEADER_FILE, BODY_FILE)

    @task()
    def step_six():
        CSV_FILE = Path('raw/products/products_rejoined_v1.csv')
        clean_text_file(CSV_FILE)

    start_step = start_steps()
    step_1_task = step_one()
    step_2_task = step_two()
    step_3_task = step_three()
    step_4_task = step_four()
    step_5_task = step_five()
    step_6_task = step_six()
    end_steps = end_steps()
    start_step >> step_1_task >> step_2_task >> step_3_task >> step_4_task >> step_5_task >> step_6_task >> end_steps


transform_products_dag = transform_products_pipeline()
