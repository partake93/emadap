from typing import Any, Dict, List, Tuple
import pandas as pd
from datetime import datetime
from azure.storage.blob import ContainerClient
from common.constants import (
    ACTIVITY_FILE_CONFIG_TBL,
    CONTROL_TBL_SCHEMA,
    EXCEL_SCENARIOS_CONFIG_FILE,
    LOG_ACTIVITY_END_FAILED,
    LOG_ACTIVITY_END_SUCCESS,
)
from common.connection_manager import get_source_file_prefix, read_scenarios_configs
from common.logger_utils import logger
from preprocess.utils import (
    fill_missing_values,
    get_metadata_from_multiple_rows,
    validate_file_metadata,
    append_metadata_to_dataframe,
)
from writers.utils import (
    send_message_to_queue,
    standardize_dataframe_columns,
    add_audit_columns,
    write_parquet_file,
)
from common.audit_logger import log_activity_end, log_activity_error
from common.helper_utils import create_activity_ref_details, raise_error
from common.exception_handlers import (
    FileValidationException,
    InvalidFileAsPerConfigException,
)

# Constants
DEFAULT_HEADER_ROW = 1


def preprocess_excel_file(
    container_client: ContainerClient,
    file_pattern_name: str,
    file_configs: list,
    temp_file_name: str,
    timestamp: str,
    zip_file_name: str,
    org_file_name: str,
    **kwargs: Any,
):
    """Process the excel file according to the specified configuration."""

    activity_type = kwargs.get("activity_type")
    activity_run_id = kwargs.get("activity_run_id")
    logging_completed = kwargs.get("logging_completed")
    source_name = kwargs.get("source_name")
    file_in_zip_pattern_name = kwargs.get("file_in_zip_pattern_name")
    source_file_prefix = get_source_file_prefix(
        file_pattern_name=file_pattern_name,
        file_in_zip_pattern_name=file_in_zip_pattern_name,
    )

    ingestion_time = datetime.strptime(timestamp, "%Y%m%d%H%M%S%f").strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    scenario_configs = read_scenarios_configs(EXCEL_SCENARIOS_CONFIG_FILE)

    file_type_config = get_excel_config(file_configs, file_pattern_name)

    header_row = DEFAULT_HEADER_ROW
    condition = None
    expected_count = None
    try:
        with pd.ExcelFile(temp_file_name) as excel_data:
            sheet_name = excel_data.sheet_names[0]
            raw_data = pd.read_excel(
                excel_data, sheet_name=sheet_name, header=None, dtype=str
            )
            if file_type_config:
                header_row = file_type_config.get("header_row", DEFAULT_HEADER_ROW)
                metadata, scenario_config = process_metadata(
                    raw_data, scenario_configs, file_type_config
                )
                df = pd.read_excel(
                    excel_data, sheet_name=sheet_name, header=header_row - 1, dtype=str
                )
                if file_type_config.get("validate_count", False):
                    condition = file_type_config.get("condition", "summary_count")
                    expected_count = int(metadata.get("expected_count"))
                    validate_file_metadata(condition, expected_count, df, org_file_name)

                # Add specified metadata to the DataFrame as new columns
                append_metadata_to_dataframe(df, metadata, scenario_config)

                # Fill missing values based on the configuration
                fill_missing_values_config = file_type_config.get(
                    "fill_missing_values", []
                )
                df = fill_missing_values(df, fill_missing_values_config)
            else:
                if source_name.lower() == "genco":
                    df = pd.read_excel(
                        excel_data, sheet_name=sheet_name, header=None, dtype=str
                    )
                    df.columns = [f"_c{i}" for i in range(df.shape[1])]
                else:
                    df = pd.read_excel(
                        excel_data,
                        sheet_name=sheet_name,
                        header=header_row - 1,
                        dtype=str,
                    )

            # Reset index for the DataFrame
            df.reset_index(drop=True, inplace=True)

            parquet_blob_name = f"{org_file_name.rsplit('.', 1)[0]}_{timestamp}.parquet"
            if source_name.lower() == "genco":
                row_count = write_parquet_file(container_client, df, parquet_blob_name)
            else:
                df = add_audit_columns(
                    df, ingestion_time, org_file_name, zip_file_name, parquet_blob_name
                )
                standardized_df = standardize_dataframe_columns(df=df)
                row_count = write_parquet_file(
                    container_client, standardized_df, parquet_blob_name
                )
            log_activity_completion(
                activity_type=activity_type,
                activity_run_id=activity_run_id,
                zip_file_name=zip_file_name,
                org_file_name=org_file_name,
                parquet_blob_name=parquet_blob_name,
                logging_completed=logging_completed,
                condition=condition,
                expected_count=expected_count,
                row_count=row_count,
            )
            send_message_to_queue(
                message={
                    "source_file_name": parquet_blob_name,
                    "source_file_prefix": source_file_prefix,
                    "source_name": source_name,
                    "split_file": True if condition == "summary_count" else False,
                    "summary_count": (
                        expected_count if condition == "summary_count" else None
                    ),
                }
            )

            logger.info(
                "Completed %s activity for excel file: %s", activity_type, org_file_name
            )

    except FileValidationException as e:
        handle_logging_error(
            activity_run_id,
            activity_type,
            zip_file_name,
            org_file_name,
            e.details.get("error"),
            logging_completed,
            condition,
            expected_count,
        )
        raise e
    except Exception as e:
        error_string = (
            f"An unexpected error occurred while processing {org_file_name}: {e}"
        )
        raise_error(error_string)


def get_excel_config(
    file_configs: List[Dict[str, Any]], file_pattern_name: str
) -> Dict[str, Any]:
    """Retrieve Excel configuration for the given file pattern name."""
    for item in file_configs:
        file_config = item.get("file_config")
        file_type_config = item.get("file_type_config")
        if file_config.get("file_pattern_name") == file_pattern_name:
            return file_type_config
    return None


def process_metadata(
    raw_data: pd.DataFrame,
    scenario_configs: Dict[str, Any],
    file_type_config: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Process metadata based on the scenario configuration."""
    scenario_config = {}
    metadata = {}
    is_valid = True
    if "metadata" in file_type_config:
        scenario_key = file_type_config["metadata"]
        scenario_config = scenario_configs.get(scenario_key, {})
        if scenario_config["type"] == "multiple_rows":
            metadata, is_valid = get_metadata_from_multiple_rows(
                raw_data, scenario_config["multiple_rows"]
            )
    if not is_valid:
        raise InvalidFileAsPerConfigException(
            message=f"Warning: The file does not match the configuration specified in the table: {CONTROL_TBL_SCHEMA}.{ACTIVITY_FILE_CONFIG_TBL}.",
            reject_file=True,
        )
    return metadata, scenario_config


def handle_logging_error(
    activity_run_id: int,
    activity_type: str,
    zip_file_name: str,
    org_file_name: str,
    error_message: str,
    logging_completed: bool,
    condition: str,
    expected_count: int,
) -> None:
    """Handle logging for errors."""
    if not logging_completed:
        activity_ref_details = create_activity_ref_details(
            activity_type=activity_type,
            zip_file_name=zip_file_name,
            file_name=org_file_name,
            parquet_file_name=None,
            validation_condition=condition,
            summary_count=expected_count if condition == "summary_count" else None,
            header_count=expected_count if condition == "header_count" else None,
            row_count=None,
            is_split_file=True if condition == "summary_count" else False,
        )
        log_activity_error(activity_run_id=activity_run_id, error_log=error_message)
        log_activity_end(
            activity_run_id=activity_run_id,
            run_status=LOG_ACTIVITY_END_FAILED,
            activity_ref_details=activity_ref_details,
        )


def log_activity_completion(
    activity_type: str,
    activity_run_id: int,
    zip_file_name: str,
    org_file_name: str,
    parquet_blob_name: str,
    logging_completed: bool,
    condition: str,
    expected_count: int,
    row_count: int,
) -> None:
    """Log the completion of an activity."""
    if not logging_completed:
        activity_ref_details = create_activity_ref_details(
            activity_type=activity_type,
            zip_file_name=zip_file_name,
            file_name=org_file_name,
            parquet_file_name=parquet_blob_name,
            validation_condition=condition,
            summary_count=expected_count if condition == "summary_count" else None,
            header_count=expected_count if condition == "header_count" else None,
            row_count=row_count,
            is_split_file=True if condition == "summary_count" else False,
        )
        log_activity_end(
            activity_run_id=activity_run_id,
            run_status=LOG_ACTIVITY_END_SUCCESS,
            activity_ref_details=activity_ref_details,
            target_file_name=parquet_blob_name,
        )
