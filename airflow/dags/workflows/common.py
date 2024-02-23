from fnmatch import fnmatch
import logging
from typing import Optional

import pandas as pd
from google.cloud import storage


def gcs_csv_to_dataframe(
    gcs_bucket: str,
    gcs_folder: str,
    pattern: str,
    encoding: str = "utf-8",
) -> Optional[pd.DataFrame]:
    client = storage.Client(project="ae32-vpcservice-datawarehouse")

    df = pd.DataFrame()
    for blob in client.list_blobs(gcs_bucket, prefix=gcs_folder):
        if fnmatch(blob.name, pattern):
            file_path = "gs://" + gcs_bucket + "/" + blob.name
            logging.info("Reading " + file_path + " into dataframe")
            temp_df = pd.read_csv(file_path, encoding=encoding)
            df = pd.concat([df, temp_df], ignore_index=True)

    return df


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
