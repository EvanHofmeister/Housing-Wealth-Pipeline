from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import api_to_file

@task(name="Write to GCS bucket", log_prints=True)
def write_to_gcs(path: Path) -> None:
    gcs_bucket_block_name = "housing-gcs"
    gcs_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=3000)
    return

@flow(name="Main ETL script, write dataframe locally, and to the cloud")
def etl_web_to_gcs_subflow() -> None:
    """The main ETL function"""
    # df = api_to_file.etl_api_to_file_subflow()
    # df.to_parquet(f"data/avm-data.csv.parquet", compression="gzip")
    path = Path(f"data/housing_data.parquet").as_posix()
    # path = Path(f"~/prefect/data/housing_data.csv.parquet").as_posix()
    print(f"path={path}")
    write_to_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs_subflow()

