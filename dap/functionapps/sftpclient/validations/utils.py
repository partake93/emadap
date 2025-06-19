import re
import csv
import chardet
from io import BytesIO
from typing import Dict
import pandas as pd
import pyzipper
from common.exception_handlers import (
    CSVFileCorruptionException,
    EmptyFileException,
    ExcelFileCorruptionException,
    InvalidCSVDelimiterException,
    InvalidFileCompressionException,
    InvalidFileEncodingException,
    InvalidFileNameException,
    InvalidZIPFileNameException,
)


def file_empty_check(size: int, file_name: str) -> Dict:
    """
    Validate if file is empty
    """
    if size == 0:
        raise EmptyFileException(
            message=f"Warning: File {file_name} is empty",
            reject_file=True,
            additional_details={
                "size": size,
            },
        )
    return {"value": "", "success": True}


def validate_excel_empty_check(
    source_blob_size: int, file: BytesIO, file_name: str
) -> Dict:
    """
    Helper function to check excel type.
    """
    df = pd.read_excel(file, nrows=5)
    if df.empty or source_blob_size == 0:
        raise EmptyFileException(
            message=f"Warning: File {file_name} is empty",
            reject_file=True,
            additional_details={"file_name": file_name, "size": 0},
        )
    return {"value": "", "success": True}


def validate_csv_file(file: BytesIO, file_name: str, delimiter: str = ",") -> Dict:
    """
    Helper function to check csv type.
    """
    try:
        file.seek(0)
        pd.read_csv(file, nrows=5, delimiter=delimiter)
    except pd.errors.ParserError as e:
        raise CSVFileCorruptionException(
            message=f"Warning: File {file_name} is not a valid csv file, error: {str(e)}",
            reject_file=True,
            additional_details={
                "file_name": file_name,
                "error": "File is not a valid CSV format",
            },
        )
    return {"value": "", "success": True}


def validate_excel_file(file: BytesIO, file_name: str) -> Dict:
    """
    Helper function to check excel type.
    """
    try:
        pd.read_excel(file, nrows=5)
    except (pd.errors.XLRDError, ValueError) as e:
        raise ExcelFileCorruptionException(
            message=f"Warning: File {file_name} is not a valid excel file, error: {str(e)}",
            reject_file=True,
            additional_details={
                "file_name": file_name,
                "error": "File is not a valid Excel format",
            },
        )
    return {"value": "", "success": True}


def file_name_validation_l1(file_configs: list, file_name: str) -> Dict:
    """
    Zip file name validation
    """
    for items in file_configs:
        file_config = items.get("file_config")
        zip_pattern = file_config.get("zip_pattern")
        pattern_name = file_config.get("zip_pattern_name")
        if zip_pattern:
            match = re.match(zip_pattern, file_name)
            if match:
                return {"value": pattern_name, "success": True}

    raise InvalidZIPFileNameException(
        message=f"Failed: No matching file pattern for : {file_name}",
        reject_file=True,
        additional_details={"file_name": file_name},
    )


def file_name_validation_l2(file_configs: list, file_name: str) -> Dict:
    """
    CSV file name validation
    """
    for items in file_configs:
        file_config = items.get("file_config")
        file_pattern = file_config.get("file_pattern")
        pattern_name = file_config.get("file_pattern_name")
        match = re.match(file_pattern, file_name)
        if match:
            return {"value": pattern_name, "success": True}
    raise InvalidFileNameException(
        message=f"Failed: No matching file pattern for : {file_name}",
        reject_file=True,
        additional_details={"file_name": file_name},
    )


def validate_csv_delimiter(
    file_sample: bytes, file_name: str, file_pattern_name: str, file_configs: list
) -> Dict:
    """
    Helper function for CSV Delimter validation
    """
    default_delimiter = ","
    default_header_row = 1

    delimiter, header_row = default_delimiter, default_header_row

    for config in file_configs:
        file_config = config.get("file_config")
        file_type_config = config.get("file_type_config")
        if file_config.get("file_pattern_name") == file_pattern_name:
            if file_type_config:
                delimiter = file_type_config.get("delimiter", default_delimiter)
                header_row = file_type_config.get("header_row", default_header_row)
            sniffer = csv.Sniffer()
            encoding = chardet.detect(file_sample)["encoding"]
            file_sample_decoded = file_sample.decode(encoding)
            file_lines = file_sample_decoded.splitlines()
            if header_row:
                file_lines_without_metadata = "\n".join(file_lines[header_row - 1 :])
            else:
                header_row = file_type_config.get("data_start_row")
                file_lines_without_metadata = "\n".join(file_lines[header_row:])
            detected_delimiter = sniffer.sniff(file_lines_without_metadata).delimiter
            if detected_delimiter == delimiter:
                return {"value": detected_delimiter, "success": True}
            raise InvalidCSVDelimiterException(
                message=f"Warning: Invalid delimiter-{detected_delimiter} for {file_name}",
                reject_file=True,
                additional_details={
                    "file_name": file_name,
                    "detected_delimiter": detected_delimiter,
                    "expected_delimiter": delimiter,
                },
            )


def validate_file_compression(source_file: str, file_name: str) -> Dict:
    """
    To Check if the files is a valid zip
    """
    try:
        with pyzipper.AESZipFile(source_file, "r") as zf:
            zf.testzip()
        return {"value": "", "success": True}

    except (pyzipper.BadZipFile, pyzipper.LargeZipFile) as e:
        raise InvalidFileCompressionException(
            message=f"Failed: Zip File {file_name} is not a valid zip file, error: {str(e)}",
            reject_file=True,
            additional_details={"file_name": file_name},
        )


def validate_csv_file_encoding(file: BytesIO, file_name: str) -> Dict:
    """
    Validate CSV file encoding.
    """
    sample_data = file.read()
    detected_encoding = chardet.detect(sample_data)["encoding"]
    if detected_encoding.lower() != "utf-8":
        raise InvalidFileEncodingException(
            message=f"Warning: File {file_name} is not encoded in UTF-8",
            reject_file=True,
            additional_details={
                "file_name": file_name,
                "detected_encoding": detected_encoding,
            },
        )
    return {"value": "", "success": True}


def validate_excel_file_encoding(file: BytesIO, file_name: str) -> Dict:
    """
    Validate Excel file encoding.
    """
    df = pd.read_excel(file, nrows=5)
    for column in df.columns:
        for value in df[column]:
            if isinstance(value, str):
                if value.encode("utf-8").decode("utf-8") != value:
                    raise InvalidFileEncodingException(
                        message=f"Warning: File {file_name} is not encoded in UTF-8",
                        reject_file=True,
                        additional_details={"file_name": file_name},
                    )
    return {"value": "", "success": True}
