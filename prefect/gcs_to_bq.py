from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

"""Extract data from cloud storage"""
@task(log_prints=True, retries=3)
def extract_from_gcs() -> Path:
    gcs_path = f"data/avm-data-gcs.csv.parquet"
    gcs_bucket_block_name = "avm-gcs2"
    gcs_block = GcsBucket.load(gcs_bucket_block_name)

    gcs_block.get_directory(
        from_path=gcs_path
    )
    df = pd.read_parquet(gcs_path)
    return df

"""Write data to bigquery"""
@task(log_prints=True, retries=3)
def write_to_bq(df: pd.DataFrame) -> None:
    df.to_gbq(
        destination_table="avm_data.avm_data_table",
        project_id="<ENTER PROJECT NAME>",
        credentials=GcpCredentials.load("avm-gcp-creds").get_credentials_from_service_account(),
        if_exists="replace",
    )

"""create main flow to call all tasks"""
@flow()
def etl_gcs_to_bq() -> None:
    df = extract_from_gcs()
    write_to_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()