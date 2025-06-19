import os
from io import BytesIO
import pyzipper
from validations.utils import (
    validate_csv_delimiter,
    validate_file_compression,
    file_empty_check,
    validate_csv_file_encoding,
    file_name_validation_l1,
    file_name_validation_l2,
    validate_csv_file,
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


def execute_validations_zip(
    source_type: str,
    source_name: str,
    source_file_name: str,
    temp_file_name: str,
    source_blob_size: int,
    file_configs: list,
) -> tuple[str, str, list, bool]:
    """
    Validations for ZIP
    """
    try:
        activity_type = ActivityTypes.VALIDATIONS.value
        logger.info(
            "Started %s activity for zip file: %s", activity_type, source_file_name
        )
        logging_completed = False
        activity_id = retrieve_activity_id(
            activity_type=activity_type, instance_type=INSTANCE_TYPE
        )
        activity_zip_run_id = log_activity_start(
            activity_id=activity_id,
            instance_type=INSTANCE_TYPE,
            source_name=source_name,
            source_type=source_type,
            source_file_name=source_file_name,
            zip_file_name=source_file_name,
        )

        results = {}

        validations = [
            (file_name_validation_l1, (file_configs, source_file_name)),
            (validate_file_compression, (temp_file_name, source_file_name)),
            (file_empty_check, (source_blob_size, source_file_name)),
        ]

        for validation_func, args in validations:
            result = validation_func(*args)
            results[validation_func.__name__] = result.get("value")

        file_pattern_name = results["file_name_validation_l1"]
        valid_csv_file, validation_status = execute_validations_zip_l2(
            source_name=source_name,
            source_type=source_type,
            file_configs=file_configs,
            source_file=temp_file_name,
            zip_file_name=source_file_name,
        )

        if not logging_completed:
            all_validations = list(results.keys())
            activity_ref_details = create_activity_ref_details(
                activity_type=activity_type,
                zip_file_name=source_file_name,
                file_name=source_file_name,
                validations=all_validations,
            )
            log_activity_end(
                activity_run_id=activity_zip_run_id,
                run_status=LOG_ACTIVITY_END_SUCCESS,
                activity_ref_details=activity_ref_details,
                target_file_name=source_file_name,
            )
            logging_completed = True
        logger.info(
            "Completed %s activity for zip file: %s", activity_type, source_file_name
        )
        return (file_pattern_name, valid_csv_file, validation_status)

    except FileValidationException as e:
        handle_logging_error(
            activity_run_id=activity_zip_run_id,
            activity_type=activity_type,
            zip_file_name=source_file_name,
            file_name=source_file_name,
            error_string=e.details.get("error"),
            results=results,
            logging_completed=logging_completed,
        )
        raise e

    except Exception as e:
        error_string = f"Unable to execute validations for ZIP {source_file_name}: {e}"
        handle_logging_error(
            activity_run_id=activity_zip_run_id,
            activity_type=activity_type,
            zip_file_name=source_file_name,
            file_name=source_file_name,
            error_string=error_string,
            results=results,
            logging_completed=logging_completed,
        )
        raise_error(error_string)


def execute_validations_zip_l2(
    source_name: str,
    source_type: str,
    file_configs: list,
    source_file: str,
    zip_file_name: str,
) -> tuple[list, bool]:
    """
    Validations for ZIP level 2 i.e files inside zip
    """
    activity_type = ActivityTypes.VALIDATIONS.value

    valid_csv_files = []
    with pyzipper.AESZipFile(source_file, "r") as zip_ref:
        for file_name in zip_ref.namelist():
            file_size = zip_ref.getinfo(file_name).file_size
            directory = os.path.basename(file_name)
            if not directory:
                continue
            with zip_ref.open(file_name) as file_data:
                file_data_sample = file_data.read(2 * 1024 * 1024)
                file_data_sample_bytesio = BytesIO(file_data_sample)
            if directory:
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
                    zip_file_name=zip_file_name,
                )

                results = {}
                validations = {
                    "file_name_validation_l2": lambda dir=directory: file_name_validation_l2(
                        file_configs=file_configs, file_name=dir
                    ),
                    "file_empty_check": lambda fsize=file_size, dir=directory: file_empty_check(
                        size=fsize, file_name=dir
                    ),
                    # "validate_csv_file_encoding": lambda sample_bytesio=file_data_sample_bytesio, dir= directory: validate_csv_file_encoding(
                    #     file=sample_bytesio, file_name=dir
                    # ),
                    "validate_csv_delimiter": lambda sample=file_data_sample, fname=file_name, res=results: validate_csv_delimiter(
                        file_sample=sample,
                        file_name=fname,
                        file_pattern_name=res.get("file_name_validation_l2"),
                        file_configs=file_configs,
                    ),
                    "validate_csv_file": lambda sample_bytesio=file_data_sample_bytesio, dir=directory, res=results: validate_csv_file(
                        file=sample_bytesio,
                        file_name=dir,
                        delimiter=res.get("validate_csv_delimiter"),
                    ),
                }

                for field, validation_func in validations.items():
                    try:
                        result = validation_func()
                        if result["success"]:
                            results[field] = result["value"]
                            continue
                        logger.error(
                            error_string=f"For {zip_file_name}: {result['message']}"
                        )
                        handle_logging_error(
                            activity_run_id=activity_run_id,
                            activity_type=activity_type,
                            zip_file_name=zip_file_name,
                            file_name=file_name,
                            error_string=result["message"],
                            results=results,
                            logging_completed=logging_completed,
                        )
                        logging_completed = True
                        break
                    except FileValidationException as e:
                        handle_logging_error(
                            activity_run_id=activity_run_id,
                            activity_type=activity_type,
                            zip_file_name=zip_file_name,
                            file_name=file_name,
                            error_string=e.details.get("error"),
                            results=results,
                            logging_completed=logging_completed,
                        )
                        logging_completed = True
                        break
                if not logging_completed:
                    activity_ref_details = create_activity_ref_details(
                        activity_type=activity_type,
                        zip_file_name=zip_file_name,
                        file_name=file_name,
                        validations=list(results.keys()),
                    )
                    log_activity_end(
                        activity_run_id=activity_run_id,
                        run_status=LOG_ACTIVITY_END_SUCCESS,
                        activity_ref_details=activity_ref_details,
                        target_file_name=file_name,
                    )
                    logging_completed = True
                    valid_file_element = {
                        "file_name": file_name,
                        "file_pattern_name": results.get("file_name_validation_l2"),
                    }
                    valid_csv_files.append(valid_file_element)
        if len(valid_csv_files) > 0:
            return (valid_csv_files, True)
        raise FileValidationException(
            message=f"No valid files found in ZIP file: {zip_file_name}",
            reject_file=True,
        )


def handle_logging_error(
    activity_run_id: str,
    activity_type: str,
    zip_file_name: str,
    file_name: str,
    error_string: str,
    results: dict,
    logging_completed: bool,
):
    """Handle logging for errors."""
    if not logging_completed:
        log_activity_error(activity_run_id=activity_run_id, error_log=error_string)
        activity_ref_details = create_activity_ref_details(
            activity_type=activity_type,
            zip_file_name=zip_file_name,
            file_name=file_name,
            validations=list(results.keys()),
        )
        log_activity_end(
            activity_run_id=activity_run_id,
            run_status=LOG_ACTIVITY_END_FAILED,
            activity_ref_details=activity_ref_details,
        )
