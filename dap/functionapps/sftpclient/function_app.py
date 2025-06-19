import os
import azure.functions as func
from common.helper_utils import (
    get_tracker_file_data,
    upload_log,
    get_current_time_in_timezone,
)
from common.exception_handlers import raise_error
from common.connection_manager import (
    get_container_client,
    PUBLIC_KEY_EMA_PGP,
    EZ_PRESTAGING_ADLS_CONNECTION_STRING,
    EZ_PRESTAGING_BLOB_CONNECTION_STRING,
    IZ_STAGING_ADLS_CONNECTION_STRING,
)
from processor.file_traversal import process_sftp_files, process_manual_upload_files
from common.logger_utils import logger, log_stream
from common.constants import (
    EZ_PRESTAGING_ADLS_SFTP_CONTAINER_PATH,
    EZ_PRESTAGING_BLOB_MANUAL_UPLOAD_CONTAINER_PATH,
    IZ_STAGING_ADLS_SFTP_CONTAINER_PATH,
    AzureWebJobsStorage,
    TRACKER_CONTAINER_PATH,
    LOG_CONTAINER_PATH,
    PARQUET_FLAG,
    EZ_PRESTAGING_ADLS_ARCHIVE_SFTP_CONTAINER_PATH,
    IZ_STAGING_ADLS_MANUAL_UPLOAD_CONTAINER_PATH,
    EZ_PRESTAGING_ADLS_ARCHIVE_MANUAL_UPLOAD_CONTAINER_PATH,
    EZ_PRESTAGING_BLOB_ARCHIVE_QUARANTINE_CONTAINER_PATH,
    EZ_PRESTAGING_ADLS_REJECTED_SFTP_FILES_CONTAINER_PATH,
    EZ_PRESTAGING_ADLS_REJECTED_MANUAL_UPLOAD_FILES_CONTAINER_PATH,
    ENABLED_PROCESS,
)

app = func.FunctionApp()
cron = os.environ["CRON"]


@app.timer_trigger(
    schedule=cron, arg_name="mytimer", run_on_startup=False, use_monitor=False
)
def timer_trigger_fa(mytimer: func.TimerRequest) -> None:
    """
    The main time trigger function which will execute at the given cron
    """
    try:
        logger.info("Python timer trigger function app started.")

        sftp_container_client = get_container_client(
            connection_string=EZ_PRESTAGING_ADLS_CONNECTION_STRING,
            container_path=EZ_PRESTAGING_ADLS_SFTP_CONTAINER_PATH,
        )
        manual_upload_container_client = get_container_client(
            connection_string=EZ_PRESTAGING_BLOB_CONNECTION_STRING,
            container_path=EZ_PRESTAGING_BLOB_MANUAL_UPLOAD_CONTAINER_PATH,
        )
        tracker_container_client = get_container_client(
            connection_string=AzureWebJobsStorage,
            container_path=TRACKER_CONTAINER_PATH,
        )
        log_container_client = get_container_client(
            connection_string=AzureWebJobsStorage, container_path=LOG_CONTAINER_PATH
        )

        dt_now = get_current_time_in_timezone()
        log_file_name = f"log_{dt_now.strftime('%Y-%m-%d_%H-%M-%S')}.log"

        processed_files, tracker_blob_client = get_tracker_file_data(
            container_client=tracker_container_client
        )

        process_source_type_map = {
            "SFTP": (
                process_sftp_files,
                {
                    "source_type": "sftp",
                    "source_container_client": sftp_container_client,
                    "destination_connection_string": IZ_STAGING_ADLS_CONNECTION_STRING,
                    "destination_container_path": IZ_STAGING_ADLS_SFTP_CONTAINER_PATH,
                    "tracker_blob_client": tracker_blob_client,
                    "processed_files": processed_files,
                    "parquet_flag": PARQUET_FLAG,
                    "archive_connection_string": EZ_PRESTAGING_ADLS_CONNECTION_STRING,
                    "archive_sftp_container_path": EZ_PRESTAGING_ADLS_ARCHIVE_SFTP_CONTAINER_PATH,
                    "rejected_files_adls_connection_string": EZ_PRESTAGING_ADLS_CONNECTION_STRING,
                    "rejected_files_adls_container_path": EZ_PRESTAGING_ADLS_REJECTED_SFTP_FILES_CONTAINER_PATH,
                },
            ),
            "MANUAL_FILE_UPLOAD": (
                process_manual_upload_files,
                {
                    "source_type": "manual_upload",
                    "manual_upload_container_client": manual_upload_container_client,
                    "destination_connection_string": IZ_STAGING_ADLS_CONNECTION_STRING,
                    "destination_container_path": IZ_STAGING_ADLS_MANUAL_UPLOAD_CONTAINER_PATH,
                    "tracker_blob_client": tracker_blob_client,
                    "processed_files": processed_files,
                    "parquet_flag": PARQUET_FLAG,
                    "archive_manual_upload_connection_string": EZ_PRESTAGING_ADLS_CONNECTION_STRING,
                    "archive_quarantine_connection_string": EZ_PRESTAGING_BLOB_CONNECTION_STRING,
                    "archive_manual_upload_container_path": EZ_PRESTAGING_ADLS_ARCHIVE_MANUAL_UPLOAD_CONTAINER_PATH,
                    "archive_quarantine_container_path": EZ_PRESTAGING_BLOB_ARCHIVE_QUARANTINE_CONTAINER_PATH,
                    "rejected_files_adls_connection_string": EZ_PRESTAGING_ADLS_CONNECTION_STRING,
                    "rejected_files_adls_container_path": EZ_PRESTAGING_ADLS_REJECTED_MANUAL_UPLOAD_FILES_CONTAINER_PATH,
                },
            ),
        }

        for process in ENABLED_PROCESS:
            if process in process_source_type_map:
                func, kwargs = process_source_type_map[process]
                func(**kwargs)

        upload_log(
            log_file_name=log_file_name,
            log_container_client=log_container_client,
            log_stream=log_stream,
        )

    except Exception as e:
        raise_error(
            error_string=f"Unable to complete the process. An error occurred: {e}"
        )
