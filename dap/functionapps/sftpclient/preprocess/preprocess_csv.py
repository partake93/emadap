from typing import Any, Dict, List, Tuple
import pandas as pd
from common.constants import (
    ACTIVITY_FILE_CONFIG_TBL,
    CONTROL_TBL_SCHEMA,
    CSV_SCENARIOS_CONFIG_FILE,
    LOG_ACTIVITY_END_FAILED,
)
from common.connection_manager import read_scenarios_configs
from preprocess.utils import (
    get_metadata_from_single_row,
    get_metadata_from_multiple_rows,
    validate_file_metadata,
    append_metadata_to_dataframe,
)
from common.audit_logger import log_activity_end, log_activity_error
from common.helper_utils import create_activity_ref_details, raise_error
from common.exception_handlers import (
    FileValidationException,
    InvalidFileAsPerConfigException,
)

# Constants
DEFAULT_DELIMITER = ","
DEFAULT_HEADER_ROW = 1
DEFAULT_DATA_START_ROW = 2


def preprocess_csv_file(
    file_path: str,
    file_configs: list,
    file_pattern_name: str,
    org_file_name: str,
    zip_file_name: str,
    **kwargs: Any,
) -> pd.DataFrame:
    """Process the CSV file according to the specified configuration."""

    activity_type = kwargs.get("activity_type")
    activity_run_id = kwargs.get("activity_run_id")
    logging_completed = kwargs.get("logging_completed")
    condition = None
    expected_count = None

    scenario_configs = read_scenarios_configs(CSV_SCENARIOS_CONFIG_FILE)
    try:
        file_type_config = get_csv_config(file_configs, file_pattern_name)
        if file_type_config:
            delimiter = file_type_config.get("delimiter", DEFAULT_DELIMITER)
            header_row = file_type_config.get("header_row", DEFAULT_HEADER_ROW)
            data_start_row = file_type_config.get(
                "data_start_row", DEFAULT_DATA_START_ROW
            )
            skip_empty_rows = file_type_config.get("skip_empty_rows", False)

            raw_data = pd.read_csv(
                file_path, header=None, delimiter=delimiter, dtype=str
            )

            metadata, scenario_config = process_metadata(
                raw_data, scenario_configs, file_type_config
            )

            df = load_dataframe(file_path, delimiter, header_row, data_start_row)

            if skip_empty_rows:
                df = df.dropna(how="all")

            # Create default column names if no header row is provided
            if header_row is None:
                df.columns = [f"column{i+1}" for i in range(df.shape[1])]

            # Validate file metadata
            if file_type_config.get("validate_count", False):
                condition = file_type_config.get("condition", "summary_count")
                expected_count = int(metadata.get("expected_count"))
                validate_file_metadata(condition, expected_count, df, org_file_name)

            # Append metadata to the DataFrame
            append_metadata_to_dataframe(df, metadata, scenario_config)
        else:
            delimiter = DEFAULT_DELIMITER
            header_row = DEFAULT_HEADER_ROW
            data_start_row = DEFAULT_DATA_START_ROW

            df = load_dataframe(file_path, delimiter, header_row, data_start_row)

        df.reset_index(drop=True, inplace=True)

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

    return df, expected_count, condition, logging_completed


def get_csv_config(
    file_configs: List[Dict[str, Any]], file_pattern_name: str
) -> Dict[str, Any]:
    """Retrieve CSV configuration for the given file pattern name."""
    for item in file_configs:
        file_config = item.get("file_config")
        file_type_config = item.get("file_type_config")
        if (
            file_config.get("file_pattern_name") == file_pattern_name
            or file_config.get("zip_pattern_name") == file_pattern_name
        ):
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
        if scenario_config["type"] == "single_row":
            metadata = get_metadata_from_single_row(
                raw_data, scenario_config["single_row"]
            )
        elif scenario_config["type"] == "multiple_rows":
            metadata, is_valid = get_metadata_from_multiple_rows(
                raw_data, scenario_config["multiple_rows"]
            )
    if not is_valid:
        raise InvalidFileAsPerConfigException(
            message=f"Warning: The file does not match the configuration specified in the table: {CONTROL_TBL_SCHEMA}.{ACTIVITY_FILE_CONFIG_TBL}.",
            reject_file=True,
        )
    return metadata, scenario_config


def load_dataframe(
    file_path: str, delimiter: str, header_row: int, data_start_row: int
) -> pd.DataFrame:
    """Load DataFrame from CSV file with specified parameters."""
    if header_row is not None:
        return pd.read_csv(
            file_path, skiprows=header_row - 1, delimiter=delimiter, dtype=str
        )
    else:
        return pd.read_csv(
            file_path,
            header=None,
            skiprows=data_start_row - 1,
            delimiter=delimiter,
            dtype=str,
        )


def handle_logging_error(
    activity_run_id: int,
    activity_type: str,
    zip_file_name: str,
    org_file_name: str,
    error_string: str,
    logging_completed: bool,
    condition: str,
    expected_count: int,
):
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
        log_activity_error(activity_run_id=activity_run_id, error_log=error_string)
        log_activity_end(
            activity_run_id=activity_run_id,
            run_status=LOG_ACTIVITY_END_FAILED,
            activity_ref_details=activity_ref_details,
        )
