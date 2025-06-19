import os
from azure.storage.blob import ContainerClient
from common.helper_utils import (
    create_temp_file,
    transfer_blob,
    update_tracker_file_data,
    cleanup_empty_directories,
    move_blob,
    get_file_configs,
)
from common.connection_manager import (
    read_file_configs,
    read_zip_file_configs,
    get_container_client,
)
from common.exception_handlers import (
    FileValidationException,
    raise_error,
)
from common.decryption_handlers import decryption_handlers_map
from common.logger_utils import logger
from common.constants import MALWARE_SCANNING_TAG, NO_THREATS_FOUND, MALICIOUS
from processor.file_type_handlers import file_type_handlers_map


def process_file(
    source_type: str,
    source_container_client: ContainerClient,
    destination_connection_string: str,
    destination_container_path: str,
    tracker_blob_client: ContainerClient,
    processed_files: list,
    parquet_flag: str,
    all_file_configs: dict,
    source_blob_name: str,
    source_blob_size: str,
):
    try:
        source_blob_client = source_container_client.get_blob_client(source_blob_name)
        source_blob_parts = source_blob_name.split("/")
        source_timestamp, source_name, source_file_name_pgp = (
            source_blob_parts[0],
            source_blob_parts[1],
            source_blob_parts[2],
        )
        source_file_name = source_file_name_pgp.replace(".pgp", "")
        source_file_decrypt_type = source_file_name_pgp.split(".")[-1]
        source_file_type = source_file_name.split(".")[-1]

        decryption_handler = decryption_handlers_map.get(source_file_decrypt_type)
        if decryption_handler:
            temp_file_name, source_blob_size = decryption_handler(
                source_blob_client, source_file_name, source_name, source_type
            )
        else:
            temp_file_name = create_temp_file(source_blob_client=source_blob_client)

        file_type_handler = file_type_handlers_map.get(source_file_type)
        file_configs = get_file_configs(
            all_file_configs=all_file_configs, source_file_type=source_file_type
        )
        if file_type_handler:
            file_type_handler(
                source_type,
                source_blob_name,
                source_file_name,
                temp_file_name,
                source_blob_size,
                file_configs,
                source_timestamp,
                destination_container_path,
                destination_connection_string,
                parquet_flag,
                processed_files,
                source_name,
            )

        update_tracker_file_data(
            processed_files=processed_files, tracker_blob_client=tracker_blob_client
        )
    except FileValidationException as e:
        logger.error("Error processing file %s: %s", source_blob_name, e)
        raise e
    finally:
        if os.path.exists(temp_file_name):
            os.remove(temp_file_name)
        logger.info(f"Temporary file {temp_file_name} removed.")


def process_sftp_files(
    source_type: str,
    source_container_client: ContainerClient,
    destination_connection_string: str,
    destination_container_path: str,
    tracker_blob_client: ContainerClient,
    processed_files: list,
    parquet_flag: str,
    archive_connection_string: str,
    archive_sftp_container_path: str,
    rejected_files_adls_connection_string: str,
    rejected_files_adls_container_path: str,
) -> None:
    """
    Process SFTP Files
    """
    try:
        file_types_configs = read_file_configs()
        zip_file_configs = read_zip_file_configs()
        all_file_configs = {
            "file_types": file_types_configs,
            "zip_file_types": zip_file_configs,
        }
        archive_sftp_container_client = get_container_client(
            connection_string=archive_connection_string,
            container_path=archive_sftp_container_path,
        )
        rejected_files_adls_container_client = get_container_client(
            connection_string=rejected_files_adls_connection_string,
            container_path=rejected_files_adls_container_path,
        )
        for blob in source_container_client.list_blobs():
            source_blob_name = blob.name
            source_blob_size = blob.size
            if (
                "." in source_blob_name.split("/")[-1]
                and source_blob_name not in processed_files
            ):
                try:
                    process_file(
                        source_type=source_type,
                        source_container_client=source_container_client,
                        destination_connection_string=destination_connection_string,
                        destination_container_path=destination_container_path,
                        tracker_blob_client=tracker_blob_client,
                        processed_files=processed_files,
                        parquet_flag=parquet_flag,
                        all_file_configs=all_file_configs,
                        source_blob_name=source_blob_name,
                        source_blob_size=source_blob_size,
                    )
                    transfer_blob(
                        source_container_client=source_container_client,
                        target_container_client=archive_sftp_container_client,
                        source_blob_name=source_blob_name,
                        operation_type="archive",
                    )
                    cleanup_empty_directories(source_container_client, source_blob_name)

                except FileValidationException as e:
                    logger.error(
                        "File validation error: %s, additional details: %s",
                        e.details,
                        e.additional_details,
                    )
                    if e.reject_file:
                        transfer_blob(
                            source_container_client=source_container_client,
                            target_container_client=rejected_files_adls_container_client,
                            source_blob_name=source_blob_name,
                            operation_type="reject",
                        )
                    cleanup_empty_directories(source_container_client, source_blob_name)
                except Exception as e:
                    logger.error("Error processing sftp file %s: %s", source_blob_name, e)

    except Exception as e:
        raise_error(error_string=f"An error occurred on SFTP file processing: {e}")


def process_manual_upload_files(
    source_type: str,
    manual_upload_container_client: ContainerClient,
    destination_connection_string: str,
    destination_container_path: str,
    tracker_blob_client: ContainerClient,
    processed_files: list,
    parquet_flag: str,
    archive_manual_upload_connection_string: str,
    archive_quarantine_connection_string: str,
    archive_manual_upload_container_path: str,
    archive_quarantine_container_path: str,
    rejected_files_adls_connection_string: str,
    rejected_files_adls_container_path: str,
) -> None:
    """
    Process the manual upload files
    """
    try:
        file_types_configs = read_file_configs()
        zip_file_configs = read_zip_file_configs()
        all_file_configs = {
            "file_types": file_types_configs,
            "zip_file_types": zip_file_configs,
        }
        archive_manual_upload_container_client = get_container_client(
            connection_string=archive_manual_upload_connection_string,
            container_path=archive_manual_upload_container_path,
        )
        archive_quarantine_container_client = get_container_client(
            connection_string=archive_quarantine_connection_string,
            container_path=archive_quarantine_container_path,
        )
        rejected_files_adls_container_client = get_container_client(
            connection_string=rejected_files_adls_connection_string,
            container_path=rejected_files_adls_container_path,
        )
        for blob in manual_upload_container_client.list_blobs():
            source_blob_name = blob.name
            source_blob_size = blob.size
            source_blob_client = manual_upload_container_client.get_blob_client(
                source_blob_name
            )
            rejected_files_adls_blob_client = (
                rejected_files_adls_container_client.get_blob_client(
                    f"{source_blob_name}.pgp"
                )
            )
            if (
                "." in source_blob_name.split("/")[-1]
                and source_blob_name not in processed_files
            ):
                try:

                    if (
                        MALWARE_SCANNING_TAG
                        in manual_upload_container_client.get_blob_client(
                            source_blob_name
                        ).get_blob_tags()
                    ):
                        scan_result = manual_upload_container_client.get_blob_client(
                            source_blob_name
                        ).get_blob_tags()[MALWARE_SCANNING_TAG]
                        if scan_result == NO_THREATS_FOUND:
                            process_file(
                                source_type=source_type,
                                source_container_client=manual_upload_container_client,
                                destination_connection_string=destination_connection_string,
                                destination_container_path=destination_container_path,
                                tracker_blob_client=tracker_blob_client,
                                processed_files=processed_files,
                                parquet_flag=parquet_flag,
                                all_file_configs=all_file_configs,
                                source_blob_name=source_blob_name,
                                source_blob_size=source_blob_size,
                            )
                            destination_blob_client = (
                                archive_manual_upload_container_client.get_blob_client(
                                    f"{source_blob_name}.pgp"
                                )
                            )
                        elif scan_result == MALICIOUS:
                            destination_blob_client = (
                                archive_quarantine_container_client.get_blob_client(
                                    f"{source_blob_name}.pgp"
                                )
                            )
                            logger.info(
                                f"Blob {source_blob_name} moved to QUARANTINE CONTAINER container."
                            )
                        else:
                            logger.warning(
                                f"Blob {source_blob_name} has an unknown scan result: {scan_result}."
                            )

                        move_blob(
                            source_blob_client,
                            destination_blob_client,
                            source_blob_name,
                        )
                    else:
                        logger.info(
                            f"Blob {source_blob_name} does not have a scan result tag. Skipping."
                        )
                except FileValidationException as e:
                    logger.error(
                        "File validation error: %s, additional details: %s",
                        e.details,
                        e.additional_details,
                    )
                    if e.reject_file:
                        move_blob(
                            source_blob_client,
                            rejected_files_adls_blob_client,
                            source_blob_name,
                        )
                except Exception as e:
                    logger.error(
                        "Error processing manual upload file %s: %s", source_blob_name, e
                    )

    except Exception as e:
        raise_error(error_string=f"An error occurred: {e}")
