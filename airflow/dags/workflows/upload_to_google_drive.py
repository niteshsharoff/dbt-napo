import argparse
import io
import logging
from typing import Any

from airflow import AirflowException
from google.auth.transport.requests import Request
from google.cloud import storage
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata",
    "https://www.googleapis.com/auth/drive.file",
]


def get_google_drive_client(token_file: str):
    """
    Constructs the client for interacting with Google Drive API.

    :param token_file: Absolute path to OAuth2 token file
    """
    log = logging.getLogger(__name__)
    try:
        credentials = Credentials.from_authorized_user_file(token_file, OAUTH_SCOPES)
    except FileNotFoundError as error:
        log.error(f"Oauth2 token credentials not found: {error}")
        raise FileNotFoundError(error)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())

    return build("drive", "v3", credentials=credentials)


def create_folder(client: Any, folder_name: str) -> str:
    """
    Creates a folder in Google Drive. Determines whether a folder exists by querying
    the folder name.

    :param client: Google Drive client
    :param folder_name: Name of folder to be created
    :returns: ID of the existing or newly created folder
    """
    log = logging.getLogger(__name__)
    folder_list = (
        client.files()
        .list(
            q=f"""
                mimeType='application/vnd.google-apps.folder'
                and name='{folder_name}'
                and trashed=false
            """,
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    if not folder_list.get("files"):
        folder_metadata = {
            "name": folder_name,
            # "parents": [PARENT_FOLDER_ID],
            "mimeType": "application/vnd.google-apps.folder",
        }
        folder = (
            client.files()
            .create(body=folder_metadata, supportsAllDrives=True, fields="id")
            .execute()
        )
        return folder.get("id")

    log.info(f"Folder '{folder_name}' exists")
    return folder_list.get("files")[0].get("id")


def upload_bytes_to_folder(
    client: Any,
    folder_id: str,
    file_name: str,
    blob: bytes,
    mimetype: str = "application/vnd.google-apps.file",
) -> str:
    """
    Uploads a blob to Google Drive. Determines whether a file exists by file name.
    Existing files are overwritten.

    :param client: Google Drive client
    :param folder_id: ID of the folder the file will be uploaded to
    :param file_name: Name of the file to be uploaded
    :param blob: Bytes to be uploaded
    :param mimetype: MIME type of file to be uploaded
    :returns: ID of the existing or newly created file
    """
    log = logging.getLogger(__name__)
    file_list = (
        client.files()
        .list(
            q=f"name='{file_name}' and trashed=false",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    if not file_list.get("files"):
        log.warning(f"Uploading new file '{file_name}'")
        file_metadata = {
            "name": file_name,
            "parents": [folder_id],
            "mimeType": mimetype,
        }
        media = MediaIoBaseUpload(
            io.BytesIO(blob),
            mimetype=mimetype,
            resumable=True,
        )
        created_file = (
            client.files()
            .create(
                body=file_metadata,
                media_body=media,
                supportsAllDrives=True,
                fields="id",
            )
            .execute()
        )
        return created_file.get("id")

    log.warning(f"Overwriting existing file '{file_name}'")
    file_id = file_list.get("files")[0].get("id")
    media = MediaIoBaseUpload(
        io.BytesIO(blob),
        mimetype=mimetype,
        resumable=True,
    )
    updated_file = (
        client.files()
        .update(fileId=file_id, media_body=media, supportsAllDrives=True, fields="id")
        .execute()
    )
    return updated_file.get("id")


def upload_to_google_drive(
    project_name: str,
    gcs_bucket: str,
    gcs_path: str,
    gdrive_folder_id: str,
    gdrive_file_name: str,
    token_file: str,
):
    """
    Downloads a file from Cloud Storage and uploads it to a folder on Google Drive.

    :param project_name: GCP project ID
    :param gcs_bucket: Bucket where the source file is stored
    :param gcs_path: Bucket prefix to the source file
    :param gdrive_folder_id: Google Drive parent folder ID
    :param gdrive_file_name: Uploaded file name
    :param token_file: Absolute path to OAuth2 token file
    """
    log = logging.getLogger(__name__)
    gdrive_client = get_google_drive_client(token_file)
    storage_client = storage.Client(project=project_name)
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        log.error(f"File '{gcs_path}' not found")
        raise FileNotFoundError

    try:
        upload_bytes_to_folder(
            gdrive_client,
            gdrive_folder_id,
            gdrive_file_name,
            blob.download_as_bytes(),
        )
    except HttpError:
        raise AirflowException


def file_exists_on_google_drive(
    file_name: str,
    token_file: str,
) -> bool:
    """
    Check if a file already exists on Google Drive.

    :param file_name: Name of file to search for
    :param token_file: Absolute path to OAuth2 token file
    """
    log = logging.getLogger(__name__)
    gdrive_client = get_google_drive_client(token_file)
    file_list = (
        gdrive_client.files()
        .list(
            q=f"name='{file_name}' and trashed=false",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    log.info(f"Google Drive query output: '{file_list}'")
    return len(file_list["files"]) > 0


def generate_token_file(credentials_file: str, token_file: str = "token.json"):
    """
    To access Google Drive from Airflow I've created an OAuth 2.0 Client ID here:
    https://console.cloud.google.com/apis/credentials?project=ae32-vpcservice-datawarehouse

    To access drive API without manual user consent, a refresh token is required:
    https://developers.google.com/drive/api/quickstart/python

    This function generates a token.json file which will be mounted onto Airflow pods.

    :param credentials_file: Absolute path to a local GCP credentials file
    :param token_file: Output location for the generated oauth2 token file
    """
    flow = InstalledAppFlow.from_client_secrets_file(credentials_file, OAUTH_SCOPES)
    credentials = flow.run_local_server(port=0)
    with open(token_file, "w") as token:
        token.write(credentials.to_json())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--credentials_file",
        type=str,
        help="Absolute path to your GCP credentials file",
        required=True,
    )
    parser.add_argument(
        "--token_file",
        type=str,
        help="Output location for the oauth2 token file",
    )
    generate_token_file(**vars(parser.parse_args()))
