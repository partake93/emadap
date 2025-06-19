import base64
import json
import os
import re
import tempfile
from typing import Any, Dict
import pandas as pd
from azure.storage.blob import ContainerClient
from azure.storage.queue import QueueClient
from common.logger_utils import logger
from common.helper_utils import raise_error
from common.connection_manager import IZ_STAGING_ADLS_CONNECTION_STRING
from common.constants import STAGING_ADLS_QUEUE_NAME


def standardize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes the column names of a DataFrame.
    This function applies the standardization process to all column names in
    the provided DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame whose column names are to be
        standardized.

    Raises:
        ValueError: If the input is not a pandas DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with standardized column names.
    """

    def standardize_column_name(column_name: str) -> str:
        """
        Standardizes a single column name.
        This function removes leading and trailing special characters,
        replaces internal special characters with underscores, and converts
        the column name to lowercase.

        Args:
            column_name (str): The original column name to standardize.

        Returns:
            str: The standardized column name.
        """
        column_name = re.sub(r"^[^A-Za-z0-9]+", "", column_name)
        column_name = re.sub(r"[^A-Za-z0-9]+$", "", column_name)
        column_name = column_name.replace("'", "")
        column_name = re.sub(r"[^A-Za-z0-9]+", "_", column_name)
        column_name = column_name.lower()
        return column_name

    if not isinstance(df, pd.DataFrame):
        raise_error(error_string="Input must be a pandas DataFrame.")

    df.columns = [standardize_column_name(column_name=col) for col in df.columns]
    return df


def add_audit_columns(
    df: pd.DataFrame,
    ingestion_time: str,
    org_file_name: str,
    zip_file_name: str,
    parquet_blob_name: str,
) -> pd.DataFrame:
    """Add audit columns to the DataFrame."""

    df["da_ingestion_time"] = ingestion_time
    df["da_src_filename"] = org_file_name
    df["da_src_zip_filename"] = zip_file_name
    df["da_filename"] = parquet_blob_name
    return df


def write_parquet_file(
    container_client: ContainerClient, df: pd.DataFrame, parquet_blob_name: str
) -> int:
    """Write DataFrame to a Parquet file and upload to Azure Blob Storage."""
    row_count = len(df)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tp:
        temp_parquet_name = tp.name
        df.to_parquet(temp_parquet_name, engine="pyarrow")
        parquet_blob_client = container_client.get_blob_client(parquet_blob_name)
        with open(temp_parquet_name, "rb") as data:
            parquet_blob_client.upload_blob(data, overwrite=True)

    os.remove(temp_parquet_name)
    logger.info(f"Parquet file '{parquet_blob_name}' uploaded successfully to blob.")
    return row_count


def send_message_to_queue(message: Dict[str, Any]) -> None:
    """
    Sends a message to the specified Azure Queue Storage.
    """
    try:
        queue_client = QueueClient.from_connection_string(
            IZ_STAGING_ADLS_CONNECTION_STRING, STAGING_ADLS_QUEUE_NAME
        )
        message_json = json.dumps(message)
        message_json = base64.b64encode(message_json.encode("utf-8")).decode("utf-8")
        queue_client.send_message(message_json)
        logger.info(f"Message sent to queue '{STAGING_ADLS_QUEUE_NAME}'")

    except Exception as e:
        logger.error(
            f"Failed to send message to queue '{STAGING_ADLS_QUEUE_NAME}': {e}"
        )
        raise e


def get_file_counts(condition: str, expected_count: int) -> dict:
    """Return a dictionary with counts based on the condition."""
    return {
        "summary_count": expected_count if condition == "summary_count" else None,
        "header_count": expected_count if condition == "header_count" else None,
        "is_split_file": condition == "summary_count",
    }
