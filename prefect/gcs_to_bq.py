from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(name="Extract data from cloud storage", log_prints=True, retries=3)
def extract_from_gcs() -> Path:
    gcs_path = Path(f"data/housing_data.parquet").as_posix()
    gcs_bucket_block_name = "housing-gcs"
    gcs_block = GcsBucket.load(gcs_bucket_block_name)

    gcs_block.get_directory(
        from_path=gcs_path
    )
    df = pd.read_parquet(gcs_path)
    return df

@task(name="Write data to bigquery", log_prints=True, retries=3)
def write_to_bq(df: pd.DataFrame) -> None:

    """Replace any non-alphanumeric characters in field names with an underscore since BigQuery cannot handle non-alphanumeric field names"""
    df.columns = df.columns.str.replace('[^a-zA-Z0-9]', '_', regex=True)
    df.to_gbq(
        destination_table="housing_bq.housing_data",
        project_id="housing-wealth",
        credentials=GcpCredentials.load("housing-gcp-creds").get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="replace",
    )

@flow(name="Main ETL script, write dataframe from GCS to BigQuery")
def etl_gcs_to_bq_subflow() -> None:
    df = extract_from_gcs()
    write_to_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq_subflow()