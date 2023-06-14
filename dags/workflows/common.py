import logging
from typing import Optional

import pandas as pd
from google.cloud import storage


def gcs_csv_to_dataframe(
    gcs_bucket: str,
    gcs_folder: str,
    filename: str,
    encoding: str = "utf-8",
) -> Optional[pd.DataFrame]:
    bucket = storage.Client().get_bucket(gcs_bucket)
    filepath = f"gs://{gcs_bucket}/{gcs_folder}/{filename}"
    blobpath = f"{gcs_folder}/{filename}"

    if bucket.blob(blobpath).exists():
        logging.info(f"{blobpath} exists!")
        return pd.read_csv(filepath, encoding=encoding)

    logging.info(f"{blobpath} does not exist!")
    return pd.DataFrame()


def gcs_parquet_to_dataframe(
    gcs_bucket: str,
    gcs_folder: str,
    filename: str,
) -> Optional[pd.DataFrame]:
    bucket = storage.Client().get_bucket(gcs_bucket)
    filepath = f"gs://{gcs_bucket}/{gcs_folder}/{filename}"
    blobpath = f"{gcs_folder}/{filename}"

    if bucket.blob(blobpath).exists():
        logging.info(f"{blobpath} exists!")
        return pd.read_parquet(filepath)

    logging.info(f"{blobpath} does not exist!")
    return pd.DataFrame()
