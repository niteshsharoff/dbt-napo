import json
import logging
from os import getcwd
from typing import Optional

from google.cloud import storage
from jsonschema import validate, exceptions


def load_json_from_cloud_storage(
    bucket_name: str,
    object_path: str,
) -> Optional[str]:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(object_path)
    if blob:
        return blob.download_as_string(client=None).decode("ascii")

    return None


def validate_json(
    bucket_name: str,
    object_path: str,
    schema_path: str,
) -> bool:
    print(getcwd())
    with open(schema_path) as f:
        schema = json.load(f)
        records = load_json_from_cloud_storage(bucket_name, object_path)
        if records is None:
            logging.warning("No records found at {}".format(object_path))
            return True

        for record in records.split("\n"):
            if record:
                try:
                    validate(json.loads(record), schema=schema)
                except exceptions.ValidationError as err:
                    raise Exception("json schema validation failed: {}".format(err))

    return True
