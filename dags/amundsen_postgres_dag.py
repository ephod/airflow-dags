# https://github.com/amundsen-io/amundsen/blob/main/databuilder/example/dags/postgres_sample_dag.py
# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0
import logging
from pathlib import Path
import uuid

from airflow.decorators import dag
from airflow import DAG  # noqa
from airflow import macros  # noqa
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator  # noqa
from airflow.utils.dates import days_ago
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'me',
    'schedule_interval': None,
    'catchup': False,
    'start_date': days_ago(1),
}

# NEO4J cluster endpoints
NEO4J_HOST = Variable.get("NEO4J_HOST")
NEO4J_ENDPOINT = f'bolt://{NEO4J_HOST}:7687'

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

ELASTICSEARCH_HOST = Variable.get("ELASTICSEARCH_HOST")
es = Elasticsearch([
    {'host': ELASTICSEARCH_HOST},
], timeout=30, max_retries=3, retry_on_timeout=True)

# TODO: user provides a list of schema for indexing
SUPPORTED_SCHEMAS = ['public']
# String format - ('schema1', schema2', .... 'schemaN')
SUPPORTED_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(schemas="', '".join(SUPPORTED_SCHEMAS))

OPTIONAL_TABLE_NAMES = ''


def connection_string() -> str:
    db_connection = BaseHook.get_connection('POSTGRES_DB')
    user = db_connection.login
    password = db_connection.password
    host = db_connection.host
    port = db_connection.port
    db = db_connection.schema
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def create_table_extract_job() -> None:
    where_clause_suffix = f'st.schemaname in {SUPPORTED_SCHEMA_SQL_IN_CLAUSE}'

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'

    job_config = ConfigFactory.from_dict({
        f'extractor.postgres_metadata.{PostgresMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause_suffix,
        f'extractor.postgres_metadata.{PostgresMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME}': True,
        f'extractor.postgres_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connection_string(),
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j_endpoint,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j_user,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j_password,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=PostgresMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=Neo4jCsvPublisher())
    job.launch()


def create_es_publisher_sample_job() -> None:
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = f'tables{uuid.uuid4()}'
    # related to mapping type from /databuilder/publisher/elasticsearch_publisher.py#L38
    elasticsearch_new_index_key_type = 'table'
    # alias for Elasticsearch used in amundsensearchlibrary/search_service/config.py as an index
    elasticsearch_index_alias = 'table_search_index'

    job_config = ConfigFactory.from_dict({
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.GRAPH_URL_CONFIG_KEY}': neo4j_endpoint,
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.MODEL_CLASS_CONFIG_KEY}':
            'databuilder.models.table_elasticsearch_document.TableESDocument',
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_USER}': neo4j_user,
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_PW}': neo4j_password,
        f'loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY}': extracted_search_data_path,
        f'loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY}': 'w',
        f'publisher.elasticsearch.{ElasticsearchPublisher.FILE_PATH_CONFIG_KEY}': extracted_search_data_path,
        f'publisher.elasticsearch.{ElasticsearchPublisher.FILE_MODE_CONFIG_KEY}': 'r',
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY}':
            elasticsearch_client,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY}':
            elasticsearch_new_index_key,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY}':
            elasticsearch_new_index_key_type,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY}':
            elasticsearch_index_alias
    })

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    job.launch()


@dag(default_args=default_args, tags=['amundsen', 'postgres', 'neo4j', 'elasticsearch'])
def amundsen_databuilder_pipeline():
    try:
        temporal_folder = Path('/var/tmp/amundsen')
        temporal_folder.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        logger.info("Folder is already there")
    else:
        logger.info("Folder was created")

    postgres_table_extract_job = PythonOperator(
        task_id='postgres_table_extract_job',
        python_callable=create_table_extract_job
    )

    postgres_es_index_job = PythonOperator(
        task_id='postgres_es_publisher_sample_job',
        python_callable=create_es_publisher_sample_job
    )

    # elastic search update run after table metadata has been updated
    postgres_table_extract_job >> postgres_es_index_job


amundsen_databuilder_dag = amundsen_databuilder_pipeline()
