from typing import Optional
import pyodbc
from common.constants import (
    LOG_ACTIVITY_START,
    AUDIT_TBL_SCHEMA,
    ACTIVITY_RUN_LOG_TBL,
    ACTIVITY_ERROR_LOG_TBL,
    CONTROL_TBL_SCHEMA,
    ACTIVITY_TYPES_TBL,
)
from common.connection_manager import METADATA_SQL_DB_CONNECTION_STRING
from common.exception_handlers import raise_error
from common.helper_utils import get_current_time_in_timezone


def retrieve_activity_id(activity_type: str, instance_type: str) -> None:
    """
    Retrieves the activity id from the activity types table.
    """
    select_query = f"""
    SELECT ACTIVITY_ID FROM {CONTROL_TBL_SCHEMA}.{ACTIVITY_TYPES_TBL} 
    WHERE LOWER(ACTIVITY_TYPE) = '{activity_type}' AND LOWER(INSTANCE_TYPE) = '{instance_type}'
    """
    try:
        with pyodbc.connect(METADATA_SQL_DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query)
                activity_id = cursor.fetchone()[0]
        return activity_id
    except Exception as e:
        raise_error(
            error_string=f"Unable to retrieve activity id from activity types table. An error occurred: {e}"
        )


def log_activity_start(
    activity_id: int,
    instance_type: str,
    source_name: str,
    source_type: str,
    source_file_name: Optional[str] = None,
    zip_file_name: Optional[str] = None,
) -> None:
    """
    Logs the start of a activity.
    """
    activity_start_time = get_current_time_in_timezone()
    activity_status = LOG_ACTIVITY_START

    insert_query = f"""
    INSERT INTO {AUDIT_TBL_SCHEMA}.{ACTIVITY_RUN_LOG_TBL}(RUN_START_DATETIME, RUN_STATUS, ACTIVITY_ID, INSTANCE_TYPE, SOURCE_NAME, SOURCE_TYPE, SOURCE_FILE_NAME, ZIP_FILE_NAME) 
    OUTPUT INSERTED.ACTIVITY_RUN_ID
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        with pyodbc.connect(METADATA_SQL_DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    insert_query,
                    activity_start_time,
                    activity_status,
                    activity_id,
                    instance_type,
                    source_name,
                    source_type,
                    source_file_name,
                    zip_file_name,
                )
                activity_run_id = cursor.fetchone()[0]
                conn.commit()
        return activity_run_id
    except Exception as e:
        raise_error(
            error_string=f"Unable to log activity start run log. An error occurred: {e}"
        )


def log_activity_end(
    activity_run_id: int,
    run_status: str,
    activity_ref_details: Optional[str] = None,
    target_file_name: Optional[str] = None,
) -> None:
    """
    Logs the start of a activity.
    """
    activity_end_time = get_current_time_in_timezone()
    milliseconds = int(activity_end_time.microsecond / 1000)
    formatted_activity_end_time = (
        activity_end_time.strftime("%Y-%m-%d %H:%M:%S") + f".{milliseconds:03}"
    )

    update_query = f"""
    UPDATE {AUDIT_TBL_SCHEMA}.{ACTIVITY_RUN_LOG_TBL} 
    SET RUN_END_DATETIME = ?, 
        RUN_STATUS = ?,
        ACTIVITY_REF_DETAILS = ?,
        TARGET_FILE_NAME = ?
    WHERE ACTIVITY_RUN_ID = ? 
    """

    try:
        with pyodbc.connect(METADATA_SQL_DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    update_query,
                    formatted_activity_end_time,
                    run_status,
                    activity_ref_details,
                    target_file_name,
                    activity_run_id,
                )
                conn.commit()
    except Exception as e:
        raise_error(
            error_string=f"Unable to log activity end run log. An error occurred: {e}"
        )


def log_activity_error(
    activity_run_id: int,
    error_code: Optional[str] = None,
    error_log: Optional[str] = None,
) -> None:
    """
    Logs the start of a activity.
    """
    activity_error_logged_time = get_current_time_in_timezone()

    insert_query = f"""
    INSERT INTO {AUDIT_TBL_SCHEMA}.{ACTIVITY_ERROR_LOG_TBL}(ERROR_LOGGED_DATETIME, ERROR_CODE, ERROR_LOG, ACTIVITY_RUN_ID) 
    VALUES (?, ?, ?, ?)
    """
    try:
        with pyodbc.connect(METADATA_SQL_DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    insert_query,
                    activity_error_logged_time,
                    error_code,
                    error_log,
                    activity_run_id,
                )
                conn.commit()
    except Exception as e:
        raise_error(
            error_string=f"Unable to log activity error run log. An error occurred: {e}"
        )
