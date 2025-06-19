from validations.validations_csv import execute_validations_csv
from validations.validations_zip import execute_validations_zip
from validations.validations_excel import execute_validations_excel
from process.process_zip import process_zip
from process.process_excel import process_excel
from process.process_csv import process_csv
from common.exception_handlers import (
    FileValidationException,
    raise_error,
)
from common.logger_utils import logger


def handle_csv_file(
    source_type: str,
    source_blob_name: str,
    source_file_name: str,
    temp_file_name: str,
    source_blob_size: int,
    file_configs: dict,
    source_timestamp: str,
    destination_container_path: str,
    destination_connection_string: str,
    parquet_flag: str,
    processed_files: list,
    source_name: str,
):
    """Process CSV files."""
    try:
        file_pattern_name, validation_status = execute_validations_csv(
            source_type=source_type,
            source_name=source_name,
            source_file_name=source_file_name,
            temp_file_name=temp_file_name,
            source_blob_size=source_blob_size,
            file_configs=file_configs,
        )
        if validation_status:
            process_csv(
                source_type=source_type,
                temp_file=temp_file_name,
                file_name=source_file_name,
                destination_container_path=destination_container_path,
                destination_connection_string=destination_connection_string,
                file_pattern_name=file_pattern_name,
                file_configs=file_configs,
                timestamp=source_timestamp,
                parquet_flag=parquet_flag,
                source_name=source_name,
            )
            processed_files.append(source_blob_name)
    except FileValidationException as e:
        logger.error("Error handling CSV file: %s", e)
        raise e


def handle_zip_file(
    source_type: str,
    source_blob_name: str,
    source_file_name: str,
    temp_file_name: str,
    source_blob_size: int,
    file_configs: dict,
    source_timestamp: str,
    destination_container_path: str,
    destination_connection_string: str,
    parquet_flag: str,
    processed_files: list,
    source_name: str,
):
    """Process ZIP files."""
    file_pattern_name, valid_csv_files, validation_status = execute_validations_zip(
        source_type=source_type,
        source_name=source_name,
        source_file_name=source_file_name,
        temp_file_name=temp_file_name,
        source_blob_size=source_blob_size,
        file_configs=file_configs,
    )
    if validation_status:
        process_zip(
            source_type=source_type,
            file=temp_file_name,
            zip_file_name=source_file_name,
            destination_container_path=destination_container_path,
            destination_connection_string=destination_connection_string,
            file_pattern_name=file_pattern_name,
            file_configs=file_configs,
            timestamp=source_timestamp,
            parquet_flag=parquet_flag,
            valid_csv_files=valid_csv_files,
            source_name=source_name,
        )
        processed_files.append(source_blob_name)


def handle_excel_file(
    source_type: str,
    source_blob_name: str,
    source_file_name: str,
    temp_file_name: str,
    source_blob_size: int,
    file_configs: dict,
    source_timestamp: str,
    destination_container_path: str,
    destination_connection_string: str,
    parquet_flag: str,
    processed_files: list,
    source_name: str,
):
    """Process Excel files."""
    try:
        file_pattern_name, validation_status = execute_validations_excel(
            source_type=source_type,
            source_name=source_name,
            source_file_name=source_file_name,
            temp_file_name=temp_file_name,
            source_blob_size=source_blob_size,
            file_configs=file_configs,
        )
        if validation_status:
            process_excel(
                source_type=source_type,
                temp_file=temp_file_name,
                file_name=source_file_name,
                destination_container_path=destination_container_path,
                destination_connection_string=destination_connection_string,
                file_pattern_name=file_pattern_name,
                file_configs=file_configs,
                timestamp=source_timestamp,
                parquet_flag=parquet_flag,
                source_name=source_name,
            )
            processed_files.append(source_blob_name)
    except FileValidationException as e:
        logger.error("Error handling Excel file: %s", e)
        raise e


# File type handlers
file_type_handlers_map = {
    "zip": handle_zip_file,
    "csv": handle_csv_file,
    "xls": handle_excel_file,
    "xlsx": handle_excel_file,
}
