import logging
from dataclasses import dataclass
import json
from pathlib import Path
import zipfile

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


def unzip_file(zip_file: Path) -> None:
    minio_connection = BaseHook.get_connection('MINIO_S3')
    minio_configuration = MinioConfiguration(**json.loads(minio_connection.get_extra()))
    minio_client = Minio(
        minio_configuration.host,
        access_key=minio_configuration.aws_access_key_id,
        secret_key=minio_configuration.aws_secret_access_key,
        secure=False)
    BUCKET_NAME = "datathon"
    result = minio_client.fget_object(BUCKET_NAME, str(zip_file), zip_file.name)
    logger.info(result)
    zip_file = Path(zip_file.name)
    extract_folder = Path('./extract')
    if not zip_file.is_file():
        raise AssertionError("Zip file doesn't exists")
    csv_file = extract_folder
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_names = zip_ref.namelist()
        for file_name in file_names:
            logger.info(f"File  {file_name} within {zip_file}")
            if file_name.endswith('.csv'):
                csv_file /= file_name
                zip_ref.extract(file_name, extract_folder)
    minio_client.fput_object(
        BUCKET_NAME,
        f"postal-codes/{csv_file.name}",
        str(csv_file),
        content_type="application/csv")


@dag(default_args=default_args, tags=['minio', 'postal-codes', 'unzip'])
def extract_postal_codes_pipeline():
    @task()
    def start_unzip():
        logger.info("Start unzip")

    @task()
    def end_unzip():
        logger.info("End unzip")

    @task()
    def unzip_es():
        ZIP_FILE = Path('postal-codes/ES.zip')
        unzip_file(ZIP_FILE)

    @task()
    def unzip_pt():
        ZIP_FILE = Path('postal-codes/PT.zip')
        unzip_file(ZIP_FILE)

    start_unzip = start_unzip()
    unzip_es_csv_file = unzip_es()
    unzip_pt_csv_file = unzip_pt()
    end_unzip = end_unzip()

    start_unzip >> unzip_es_csv_file >> unzip_pt_csv_file >> end_unzip


extract_postal_codes_dag = extract_postal_codes_pipeline()
