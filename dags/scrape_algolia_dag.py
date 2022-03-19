from dataclasses import dataclass
import json
import logging
from pathlib import Path

from algoliasearch.search_client import SearchClient
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from minio import Minio
import jsonlines

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


def scrape_algolia(jsonl_file: Path):
    ALGOLIA_APP_ID = Variable.get("ALGOLIA_APP_ID")
    ALGOLIA_API_KEY = Variable.get("ALGOLIA_API_KEY")
    client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_API_KEY)
    product_index = client.init_index("product-ecommerce-es-es_es")
    with jsonlines.open(str(jsonl_file), mode='w') as writer:
        results = product_index.search("", {'hitsPerPage': 1_000, 'page': 0})
        writer.write(results)
        number_of_pages = results["nbPages"]
        logger.info(f"Number of pages: {number_of_pages}")
        if number_of_pages == 1:
            logger.info(f"Done with *. 1 page")
        else:
            for page_number in range(1, number_of_pages):
                logger.info(f"Current page number {page_number}")
                results = product_index.search("*", {'hitsPerPage': 1_000, 'page': page_number})
                writer.write(results)
            logger.info(f"Done with *. {number_of_pages} pages")

    minio_connection = BaseHook.get_connection('MINIO_S3')
    minio_configuration = MinioConfiguration(**json.loads(minio_connection.get_extra()))
    minio_client = Minio(
        minio_configuration.host,
        access_key=minio_configuration.aws_access_key_id,
        secret_key=minio_configuration.aws_secret_access_key,
        secure=False)
    BUCKET_NAME = "datathon"

    minio_client.fput_object(
        BUCKET_NAME,
        f"raw/algolia/{jsonl_file.name}",
        str(jsonl_file),
        content_type="application/jsonlines+json")


@dag(default_args=default_args, tags=['minio', 'algolia', 'crawl'])
def scrape_algolia_pipeline():
    @task()
    def start_scraping():
        logger.info("Start scraping")

    @task()
    def end_scraping():
        logger.info("End scraping")

    @task()
    def scrape():
        JSON_LINE_FILE = Path('algolia.jsonl')
        scrape_algolia(JSON_LINE_FILE)

    start_scraping = start_scraping()
    scrape = scrape()
    end_scraping = end_scraping()

    start_scraping >> scrape >> end_scraping


scrape_algolia_dag = scrape_algolia_pipeline()
