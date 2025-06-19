from datetime import datetime
from typing import Any
from azure.storage.blob import ContainerClient
from writers.utils import (
    get_file_counts,
    send_message_to_queue,
    standardize_dataframe_columns,
    add_audit_columns,
    write_parquet_file,
)
from preprocess.preprocess_csv import preprocess_csv_file
from preprocess.preprocess_excel import preprocess_excel_file
from common.constants import LOG_ACTIVITY_END_SUCCESS
from common.audit_logger import log_activity_end
from common.logger_utils import logger
from common.helper_utils import create_activity_ref_details, raise_error
from common.exception_handlers import FileValidationException
from common.connection_manager import get_source_file_prefix


def write_parquet(
    container_client: ContainerClient,
    file_name: str,
    temp_file_name: str,
    timestamp: str,
    zip_file_name: str,
    org_file_name: str,
    file_configs: list,
    file_pattern_name: str,
    **kwargs: Any,
) -> None:
    """
    Convert csv to parquet and write to blob
    """
    try:
        activity_type = kwargs.get("activity_type")
        activity_run_id = kwargs.get("activity_run_id")
        logging_completed = kwargs.get("logging_completed")
        source_name = kwargs.get("source_name")
        file_in_zip_pattern_name = kwargs.get("file_in_zip_pattern_name")
        source_file_prefix = get_source_file_prefix(
            file_pattern_name=file_pattern_name,
            file_in_zip_pattern_name=file_in_zip_pattern_name,
        )

        file_type = org_file_name.split(".")[-1]
        if file_type.lower() == "csv":
            ingestion_time = datetime.strptime(timestamp, "%Y%m%d%H%M%S%f").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            parquet_blob_name = f'{file_name.rsplit(".",1)[0]}.parquet'
            df, expected_count, condition, logging_completed = preprocess_csv_file(
                file_path=temp_file_name,
                file_configs=file_configs,
                file_pattern_name=file_pattern_name,
                org_file_name=org_file_name,
                zip_file_name=zip_file_name,
                activity_type=activity_type,
                activity_run_id=activity_run_id,
                logging_completed=logging_completed,
            )

            df = add_audit_columns(
                df, ingestion_time, org_file_name, zip_file_name, parquet_blob_name
            )
            standardized_df = standardize_dataframe_columns(df=df)
            row_count = write_parquet_file(
                container_client, standardized_df, parquet_blob_name
            )
            if not logging_completed:
                counts = get_file_counts(condition, expected_count)
                summary_count = counts.get("summary_count")
                header_count = counts.get("header_count")
                is_split_file = counts.get("is_split_file")
                activity_ref_details = create_activity_ref_details(
                    activity_type=activity_type,
                    zip_file_name=zip_file_name,
                    file_name=org_file_name,
                    parquet_file_name=parquet_blob_name,
                    validation_condition=condition,
                    summary_count=summary_count,
                    header_count=header_count,
                    row_count=row_count,
                    is_split_file=is_split_file,
                )
                log_activity_end(
                    activity_run_id=activity_run_id,
                    run_status=LOG_ACTIVITY_END_SUCCESS,
                    activity_ref_details=activity_ref_details,
                    target_file_name=parquet_blob_name,
                )
                logging_completed = True
                send_message_to_queue(
                    message={
                        "source_file_name": parquet_blob_name,
                        "source_file_prefix": source_file_prefix,
                        "source_name": source_name,
                        "split_file": is_split_file,
                        "summary_count": summary_count,
                    }
                )
            logger.info(
                "Completed %s activity for csv file: %s", activity_type, org_file_name
            )
        elif file_type.lower() in ("xls", "xlsx"):
            preprocess_excel_file(
                container_client=container_client,
                file_pattern_name=file_pattern_name,
                file_configs=file_configs,
                temp_file_name=temp_file_name,
                timestamp=timestamp,
                zip_file_name=zip_file_name,
                org_file_name=org_file_name,
                activity_type=activity_type,
                activity_run_id=activity_run_id,
                logging_completed=logging_completed,
                source_name=source_name,
            )
        else:
            raise ValueError(
                "Unsupported file format. Please provide a CSV or Excel file."
            )
    except FileValidationException as e:
        logger.error("Error writing parquet file: %s", e)
        raise e
