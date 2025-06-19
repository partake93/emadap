from io import BytesIO
from validations.utils import (
    file_empty_check,
    validate_csv_delimiter,
    validate_csv_file,
    file_name_validation_l2,
    validate_csv_file_encoding,
)
from common.constants import (
    LOG_ACTIVITY_END_SUCCESS,
    LOG_ACTIVITY_END_FAILED,
    INSTANCE_TYPE,
    ActivityTypes,
)
from common.audit_logger import (
    retrieve_activity_id,
    log_activity_end,
    log_activity_start,
    log_activity_error,
)
from common.logger_utils import logger
from common.helper_utils import create_activity_ref_details
from common.exception_handlers import FileValidationException, raise_error


def execute_validations_csv(
    source_type: str,
    source_name: str,
    source_file_name: str,
    temp_file_name: str,
    source_blob_size: int,
    file_configs: list,
) -> tuple[str, str, bool]:
    """
    validations for CSV
    """
    try:
        activity_type = ActivityTypes.VALIDATIONS.value
        logger.info(
            "Started %s activity for csv file: %s", activity_type, source_file_name
        )
        logging_completed = False
        activity_id = retrieve_activity_id(
            activity_type=activity_type, instance_type=INSTANCE_TYPE
        )
        activity_run_id = log_activity_start(
            activity_id=activity_id,
            instance_type=INSTANCE_TYPE,
            source_name=source_name,
            source_type=source_type,
            source_file_name=source_file_name,
        )

        with open(temp_file_name, "rb") as file_data:
            fd_sample = file_data.read(2 * 1024 * 1024)
            fd_sample_b_io = BytesIO(fd_sample)
        results = {}

        validations = [
            (file_name_validation_l2, (file_configs, source_file_name)),
            (file_empty_check, (source_blob_size, source_file_name)),
            # (validate_csv_file_encoding, (fd_sample_b_io, source_file_name)),
        ]

        for validation_func, args in validations:
            result = validation_func(*args)
            results[validation_func.__name__] = result.get("value")

        file_pattern_name = results.get("file_name_validation_l2", None)
        validate_csv_delimiter_result = validate_csv_delimiter(
            fd_sample, source_file_name, file_pattern_name, file_configs
        )
        validate_csv_file(
            file=fd_sample_b_io,
            file_name=source_file_name,
            delimiter=validate_csv_delimiter_result.get("value"),
        )

        if not logging_completed:
            all_validations = list(results.keys())
            all_validations.extend(["validate_csv_delimiter", "validate_csv_file"])
            activity_ref_details = create_activity_ref_details(
                activity_type=activity_type,
                zip_file_name=None,
                file_name=source_file_name,
                validations=all_validations,
            )
            log_activity_end(
                activity_run_id=activity_run_id,
                run_status=LOG_ACTIVITY_END_SUCCESS,
                activity_ref_details=activity_ref_details,
                target_file_name=source_file_name,
            )
            logging_completed = True
        logger.info(
            "Completed %s activity for csv file: %s", activity_type, source_file_name
        )
        return (file_pattern_name, True)

    except FileValidationException as e:
        handle_logging_error(
            activity_run_id,
            activity_type,
            source_file_name,
            e.details.get("error"),
            results,
            logging_completed,
        )
        raise e

    except Exception as e:
        error_string = f"Unable to execute validations for CSV {source_file_name}: {e}"
        handle_logging_error(
            activity_run_id,
            activity_type,
            source_file_name,
            error_string,
            results,
            logging_completed,
        )
        raise_error(error_string)


def handle_logging_error(
    activity_run_id: str,
    activity_type: str,
    source_file_name: str,
    error_string: str,
    results: dict,
    logging_completed: bool,
):
    """Handle logging for errors."""
    if not logging_completed:
        log_activity_error(activity_run_id=activity_run_id, error_log=error_string)
        activity_ref_details = create_activity_ref_details(
            activity_type=activity_type,
            zip_file_name=None,
            file_name=source_file_name,
            validations=list(results.keys()),
        )
        log_activity_end(
            activity_run_id=activity_run_id,
            run_status=LOG_ACTIVITY_END_FAILED,
            activity_ref_details=activity_ref_details,
        )
