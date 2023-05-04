from pathlib import Path
import pandas as pd
from prefect import flow, task

from api_to_file import etl_api_to_file_subflow
from web_to_gcs import etl_web_to_gcs_subflow
from gcs_to_bq import etl_gcs_to_bq_subflow

@flow(name="Parent ETL orchestrating subflows")
def etl_parent_flow():
    etl_api_to_file_subflow()
    etl_web_to_gcs_subflow()
    etl_gcs_to_bq_subflow()

if __name__ == "__main__":
    etl_parent_flow()