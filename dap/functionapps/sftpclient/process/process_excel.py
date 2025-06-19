import os
from common.connection_manager import get_output_client
from common.exception_handlers import (
    FileValidationException,
    raise_error,
)
from writers.writers import write_parquet
from common.constants import INSTANCE_TYPE, LOG_ACTIVITY_END_FAILED, ActivityTypes
from common.audit_logger import (
    log_activity_end,
    log_activity_error,
    retrieve_activity_id,
    log_activity_start,
)
from common.logger_utils import logger
from common.helper_utils import create_activity_ref_details


def process_excel(
    source_type: str,
    temp_file: str,
    file_name: str,
    destination_container_path: str,
    destination_connection_string: str,
    file_pattern_name: str,
    file_configs: list,
    timestamp: str,
    parquet_flag: str,
    source_name: str,
) -> None:
    """
    Process excel and write to blob
    """
    try:
        activity_type = ActivityTypes.PROCESS_EXCEL.value
        logger.info("Started %s activity for excel file: %s", activity_type, file_name)
        logging_completed = False
        activity_id = retrieve_activity_id(
            activity_type=activity_type, instance_type=INSTANCE_TYPE
        )
        activity_run_id = log_activity_start(
            activity_id=activity_id,
            instance_type=INSTANCE_TYPE,
            source_name=source_name,
            source_type=source_type,
            source_file_name=file_name,
        )

        file_name_new = f"{os.path.splitext(file_name)[0]}_{timestamp}.xls"
        container_client = get_output_client(
            file_configs=file_configs,
            file_pattern_name=file_pattern_name,
            connection_string=destination_connection_string,
            output_container_path=destination_container_path,
            source_name=source_name,
        )
        if parquet_flag.lower() == "true":
            write_parquet(
                container_client=container_client,
                file_name=file_name_new,
                temp_file_name=temp_file,
                timestamp=timestamp,
                zip_file_name=None,
                org_file_name=file_name,
                file_configs=file_configs,
                file_pattern_name=file_pattern_name,
                activity_type=activity_type,
                activity_run_id=activity_run_id,
                logging_completed=logging_completed,
                source_name=source_name,
            )
    except FileValidationException as e:
        logger.error("Error processing excel file: %s", e)
        raise e
    except Exception as e:
        if not logging_completed:
            error_string = (
                f"Unable to process the excel file: {file_name}. An error occurred: {e}"
            )
            activity_ref_details = create_activity_ref_details(
                activity_type=activity_type,
                file_name=file_name,
            )
            log_activity_error(activity_run_id=activity_run_id, error_log=error_string)
            log_activity_end(
                activity_run_id=activity_run_id,
                run_status=LOG_ACTIVITY_END_FAILED,
                activity_ref_details=activity_ref_details,
            )
            raise_error(error_string=error_string)
