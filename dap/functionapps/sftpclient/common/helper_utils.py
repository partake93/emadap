import json
import os
import re
import base64
import time
import pgpy
import tempfile
from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo
from io import StringIO
from azure.storage.blob import BlobClient, ContainerClient
from azure.core.exceptions import HttpResponseError
from common.logger_utils import logger
from common.constants import PUBLIC_KEY_EMA_PGP, TRACKER_FILE_NAME, ACTIVITIES_CONFIG
from common.exception_handlers import raise_error


def create_temp_file(source_blob_client: BlobClient) -> tuple[str, int]:
    """
    Get temp file data
    """
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_name = temp_file.name
            for chunk in source_blob_client.download_blob().chunks():
                temp_file.write(chunk)
        return temp_file_name

    except Exception as e:
        raise_error(error_string=f"Unable to create temp file. An error occurred: {e}")


def convert_date_format_for_filename(name: str) -> str:
    """
    Convert dd-mmm-yy to dd-mm-yy
    """
    pattern = r"(\d{2})-([A-Za-z]{3})-(\d{2})"

    def replace_match(match):
        day, month_str, year = match.groups()
        month_num = datetime.strptime(month_str, "%b").month
        return f"{day}-{month_num:02d}-{year}"

    new_name = re.sub(pattern, replace_match, name)
    return new_name


def get_tracker_file_data(
    container_client: ContainerClient,
) -> tuple[list[str], BlobClient]:
    """
    Fetch the tracker file data, if not present initialize tracker file
    """
    try:
        tracker_blob_client = container_client.get_blob_client(TRACKER_FILE_NAME)
        tracker_blob_data = tracker_blob_client.download_blob()
        processed_files = tracker_blob_data.readall().decode("utf-8").splitlines()
    except Exception:
        logger.info("Tracker file does not exist. Creating a new one.")
        processed_files = []
        tracker_blob_client.upload_blob("\n".join(processed_files), overwrite=True)
    return processed_files, tracker_blob_client


def update_file_name(
    file_pattern_name: str, file_name: str, zip_file_name: str, timestamp: str
) -> str:
    """
    Update file name
    """
    file_name_new = file_name
    if file_pattern_name == "dd-mm-yyyy_<period>_Run.zip":
        period = zip_file_name.split("_")[1].split("_")[0]
        file_name_new = f"{file_name.split('.')[0]}_{period}.csv"
        condition_1 = "rep01_ego_query_" in file_name_new
        condition_2 = "rep01_egb_query_" in file_name_new
        if condition_1 or condition_2:
            file_name_new = f"normal_{file_name_new}"
    if file_pattern_name == "yyyymmdd_<period>_EGO-EGB_Offers.zip":
        file_name_new = f"offers_{file_name_new}"
    file_name_new = f"{os.path.splitext(file_name_new)[0]}_{timestamp}.csv"
    file_name_new = os.path.basename(file_name_new)
    file_name_new = convert_date_format_for_filename(name=file_name_new)
    return file_name_new


def update_tracker_file_data(
    processed_files: list[str], tracker_blob_client: BlobClient
) -> None:
    """
    Update the tracker file the processes file name
    """
    try:
        tracker_blob_client.upload_blob("\n".join(processed_files), overwrite=True)
    except Exception as e:
        raise_error(
            error_string=f"Unable to update tracker file data. An error occurred: {e}"
        )


def upload_log(
    log_file_name: str, log_container_client: ContainerClient, log_stream: StringIO
) -> None:
    """
    Uploads the log to azure blob
    """
    try:
        log_container_client.upload_blob(
            name=log_file_name, data=log_stream.getvalue(), overwrite=True
        )
    except Exception as e:
        raise_error(error_string=f"Unable to upload log. An error occurred: {e}")


def get_current_time_in_timezone(timezone: str = "Asia/Singapore") -> datetime:
    """
    Get the current date and time in the specified timezone.
    """
    return datetime.now(ZoneInfo(timezone))


def create_activity_ref_details(activity_type: str, **kwargs: Any) -> str:
    """
    Create activity reference details in JSON format based on the activity type.

    Args:
        activity_type (str): The type of the activity.
        **kwargs: Additional parameters for the activity reference details.

    Returns:
        str: JSON string of activity reference details.
    """
    if activity_type not in ACTIVITIES_CONFIG:
        raise ValueError(f"Unsupported activity type: {activity_type}")

    expected_format = ACTIVITIES_CONFIG[activity_type]
    activity_ref_details = {key: kwargs.get(key) for key in expected_format}

    return json.dumps(activity_ref_details)


def cleanup_empty_directories(container_client: ContainerClient, blob_name: str):
    """
    Check and delete empty directories in the source container.
    """
    directory_path = "/".join(blob_name.split("/")[:-1])
    while directory_path:
        blobs_in_directory = list(
            container_client.list_blobs(name_starts_with=directory_path + "/")
        )

        if not blobs_in_directory:
            logger.info(f"Cleaning up empty directory: {directory_path}")
            container_client.delete_blob(directory_path)
        else:
            logger.info(f"Directory {directory_path} is not empty. No cleanup needed.")
            break
        directory_path = "/".join(directory_path.split("/")[:-1])


def encrypt_and_upload(
    file_path: str,
    file_name: str,
    destination_blob_client: BlobClient,
) -> None:
    """
    encrypt with ema dap public key
    """
    try:
        pgp_key_updated = base64.b64decode(PUBLIC_KEY_EMA_PGP)
        public_key, _ = pgpy.PGPKey.from_blob(pgp_key_updated)
        if ".csv" in file_name:
            with open(file_path, "r") as file:
                file_content = file.read()
        else:
            with open(file_path, "rb") as file:
                file_content = file.read()
        message = pgpy.PGPMessage.new(file_content, file=True)
        encrypted_message = public_key.encrypt(message)
        destination_blob_client.upload_blob(str(encrypted_message), overwrite=True)
    except Exception as e:
        raise_error(
            error_string=f"Unable to encrypt/upload {file_name}. An error occurred: {e}"
        )


def move_blob(
    source_blob_client: BlobClient,
    destination_blob_client: BlobClient,
    file_name: str,
):
    """
    Move a blob from source to destination container.
    """
    temp_file_name = create_temp_file(source_blob_client=source_blob_client)
    logger.info(f"Encrypting and Archiving  {file_name}")
    encrypt_and_upload(temp_file_name, file_name, destination_blob_client)
    source_blob_client.delete_blob()
    os.remove(temp_file_name)
    logger.info(f"Archiving completed for {file_name}")


def transfer_blob(
    source_container_client: ContainerClient,
    target_container_client: ContainerClient,
    source_blob_name: str,
    operation_type: Literal["archive", "reject"],
) -> None:
    """
    Archive a blob by copying it to the archive container, checking the copy status,
    and deleting the source blob after successful archiving.
    """
    source_blob_client = source_container_client.get_blob_client(source_blob_name)
    source_blob_url = source_blob_client.url
    target_blob_client = target_container_client.get_blob_client(source_blob_name)

    try:
        target_blob_client.start_copy_from_url(source_blob_url)
        while True:
            props = target_blob_client.get_blob_properties()
            if props.copy.status == "success":
                logger.info(
                    "Blob %s %s successfully.", source_blob_name, operation_type
                )
                source_blob_client.delete_blob()
                logger.info("Blob %s deleted from source container.", source_blob_name)
                break
            elif props.copy.status == "pending":
                time.sleep(1)
            else:
                raise HttpResponseError(
                    f"{operation_type.capitalize()} failed for {source_blob_name} status: {props.copy.status}"
                )
    except HttpResponseError as e:
        logger.error("Error during blob transfer: %s", e)
        raise


def get_file_configs(all_file_configs: dict, source_file_type: str) -> list[dict]:
    """
    Get file configurations based on the source file type.
    """
    if source_file_type == "zip":
        return all_file_configs.get("zip_file_types", [])
    else:
        return all_file_configs.get("file_types", [])
