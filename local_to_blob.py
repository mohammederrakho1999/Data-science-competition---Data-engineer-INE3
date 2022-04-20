
# authors: Mohammed errakho and kawtar el ayachi.
# done on april 12 2022 at deloitte office.

import os
import configparser
from pathlib import Path
import logging
import json
from azure.storage.blob import ContainerClient

# airflow libaries.
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

# should be seperated in a new python file.
logger = logging.getLogger('Ingestion_to_blob_storage_process')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


config = configparser.ConfigParser()
config.read_file(open("ingestion/config.cfg"))

try:
    with open("ingestion/configuration.json") as json_credentials_file:
        credentials = json.load(json_credentials_file)
except Exception:
    logger.info("Configuration file doesn't exist")


# def get_files(dir):

  #  files = []
 #   for (dirpath, dirnames, filenames) in os.walk(dir):
 #       files.extend(filenames)
#       break
#    return files


def ingest():
    """
    Takes files names and copy them into azure blob storage.
    """
    files = []
    for (dirpath, dirnames, filenames) in os.walk(config.get('LocalDisk', 'localpath')):
        files.extend(filenames)
        break

    cantainer_client = ContainerClient.from_connection_string(
        credentials["azure_credentials"], credentials["cantainer_name"])
    logger.info(
        "Inialiazing a cantainer client for azure blob storage raw bucket...connected")
    num_files = 0
    for file in files:
        blob_client = cantainer_client.get_blob_client(file)
        path = config.get('LocalDisk', 'localpath') + '/' + file
        with open(path, "rb") as data:
            logger.info(f"Start uploading file : {file} ")
            try:
                blob_client.upload_blob(data)
            except Exception as e:
                pass
            logger.info(f"Finished uploading file : {file}")
            num_files += 1

    if len(files) == num_files:
        logger.info("All files uploaded to azure blob storage sucessfully.")

    for file in files:
        os.remove(config.get('LocalDisk', 'localpath') + '/' + file)
        logger.info(f"{file} removed.")

    return True


dag = DAG(
    dag_id="ingestion",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None
)
ingest_file_to_blob_azure = PythonOperator(
    task_id="ingestion",
    python_callable=ingest,
    dag=dag

)
