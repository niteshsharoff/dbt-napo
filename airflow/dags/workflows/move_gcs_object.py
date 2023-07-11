from google.cloud import storage


def move_gcs_blob(
    project_name: str, src_bucket: str, src_path: str, dst_bucket: str, dst_path: str
):
    storage_client = storage.Client(project=project_name)
    src_bucket = storage_client.get_bucket(src_bucket)
    src_blob = src_bucket.blob(src_path)
    dst_bucket = storage_client.get_bucket(dst_bucket)
    if src_blob.exists():
        _ = src_bucket.copy_blob(src_blob, dst_bucket, dst_path)
        src_blob.delete()
