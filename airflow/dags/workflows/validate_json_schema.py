import json
import logging
from typing import Optional

from google.cloud import storage
from jsonschema import exceptions, validate


def load_json_from_cloud_storage(
    project_name: str,
    bucket_name: str,
    object_path: str,
) -> Optional[str]:
    client = storage.Client(project=project_name)
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(object_path)
    if blob:
        return blob.download_as_string(client=None).decode("ascii")

    return None


def validate_json(
    project_name: str,
    bucket_name: str,
    object_path: str,
    schema_path: str,
) -> bool:
    log = logging.getLogger(__name__)
    with open(schema_path) as f:
        schema = json.load(f)
        records = load_json_from_cloud_storage(project_name, bucket_name, object_path)
        if records is None:
            log.warning("No records found at {}".format(object_path))
            return True

        records = records.split("\n")
        for record in records:
            if record:
                try:
                    validate(json.loads(record), schema=schema)
                except exceptions.ValidationError as err:
                    raise Exception("json schema validation failed: {}".format(err))

        log.info("Validated {} records".format(len(records)))

    return True
