import re
import pandas as pd
from common.exception_handlers import (
    InvalidHeaderCountException,
    InvalidFileCountConditionException,
    InvalidSummaryCountException,
)


def get_metadata_from_single_row(
    raw_data: pd.DataFrame, single_row_config: dict
) -> dict:
    """Extract metadata from a single row."""
    metadata = {}
    row_index = single_row_config["row"] - 1
    mapping = single_row_config["mapping"]
    metadata_info = raw_data.iloc[row_index]

    # Map the values to the corresponding keys
    for key, col_index in mapping.items():
        metadata[key] = metadata_info[col_index]

    return metadata


def get_metadata_from_multiple_rows(
    raw_data: pd.DataFrame, multiple_rows_config: dict
) -> tuple[dict, bool]:
    """Extract metadata from multiple rows."""
    metadata = {}
    is_valid = True
    for meta in multiple_rows_config["rows"]:
        row_index = meta["row"] - 1
        expected_keywords = meta["expected_keywords"]
        metadata_info = " ".join(raw_data.iloc[row_index].dropna().astype(str))

        # Check if expected keywords are present
        for keyword in expected_keywords:
            keyword = keyword.lower()
            if keyword in metadata_info.lower():
                key = meta.get("include_in_dataframe")
                if not key:
                    key = keyword.replace(" ", "_")
                extraction_type = meta.get("extraction_type")

                if extraction_type == "expected_count" and keyword in [
                    "no of records",
                    "total number of records",
                ]:
                    match = re.search(r"(\d+)", metadata_info)
                    if match:
                        metadata["expected_count"] = int(match.group(1))
                elif keyword in ["as of month"]:
                    metadata[key] = metadata_info.split(":")[-1].strip()
            else:
                is_valid = False

    return metadata, is_valid


def append_metadata_to_dataframe(
    df: pd.DataFrame, metadata: dict, scenario_config: dict
):
    """Add metadata to the DataFrame as new columns."""
    if scenario_config["type"] == "single_row":
        include_keys = scenario_config["single_row"].get("include_in_dataframe", [])
        for key in include_keys:
            if key in metadata:
                df[key] = metadata[key]

    elif scenario_config["type"] == "multiple_rows":
        for meta in scenario_config["multiple_rows"].get("rows", []):
            key = meta.get("include_in_dataframe")
            if key and key in metadata:
                df[key] = metadata[key]


def validate_file_metadata(
    condition: str, expected_count: int, df: pd.DataFrame, org_file_name: str
) -> dict:
    """Handle logic specific to files with summary count/header count."""

    row_count = len(df)
    if condition == "summary_count":
        if expected_count < row_count:
            raise InvalidSummaryCountException(
                message=f"Failed: Expectation: summary count should be greater than or equal to the row count, Invalid file: {org_file_name}, summary_count ({expected_count}) is not greater than or equal to row_count ({row_count}).",
                reject_file=True,
                additional_details={
                    "summary_count": expected_count,
                    "row_count": row_count,
                },
            )
    elif condition == "header_count":
        if expected_count != row_count:
            raise InvalidHeaderCountException(
                message=f"Failed: Expectation: header count should match with row count, Invalid file: {org_file_name}, header_count ({expected_count}) is not matching row_count ({row_count}).",
                reject_file=True,
                additional_details={
                    "header_count": expected_count,
                    "row_count": row_count,
                },
            )
    else:
        raise InvalidFileCountConditionException(
            message=f"Failed: Invalid summary or header count condition: {condition} or invalid file: {org_file_name}",
            reject_file=True,
            additional_details={
                "condition": condition,
                "expected_count": expected_count,
                "row_count": row_count,
            },
        )


def fill_missing_values(df: pd.DataFrame, fill_configs: list) -> pd.DataFrame:
    """
    Fill missing values in the DataFrame based on the provided configurations.
    """
    for fill_config in fill_configs:
        method = fill_config.get("method")
        column = fill_config.get("column")

        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")

        if method == "forward_fill":
            df[column] = df[column].ffill()
        elif method == "backward_fill":
            df[column] = df[column].bfill()
        elif method == "constant":
            value = fill_config.get("value")
            if value is None:
                raise ValueError("Value for constant fill method cannot be None.")
            df[column] = df[column].fillna(value)
        else:
            raise ValueError(f"Unknown fill method: '{method}'")
    return df
