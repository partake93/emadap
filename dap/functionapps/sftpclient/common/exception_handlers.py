from common.logger_utils import logger


def raise_error(error_string: str) -> None:
    """
    Log and raise error
    """
    logger.error(error_string)
    raise ValueError(error_string)


class FileValidationException(Exception):
    def __init__(self, message, reject_file, additional_details=None):
        super().__init__(message)
        self.reject_file = reject_file
        self.details = {"error": message, "reject_file": reject_file}
        self.additional_details = additional_details or {}


class InvalidSummaryCountException(FileValidationException):
    pass


class InvalidHeaderCountException(FileValidationException):
    pass


class InvalidFileCountConditionException(FileValidationException):
    pass


class EmptyFileException(FileValidationException):
    pass


class CSVFileCorruptionException(FileValidationException):
    pass


class ExcelFileCorruptionException(FileValidationException):
    pass


class InvalidCSVDelimiterException(FileValidationException):
    pass


class InvalidFileEncodingException(FileValidationException):
    pass


class InvalidFileCompressionException(FileValidationException):
    pass


class InvalidFileNameException(FileValidationException):
    pass


class InvalidZIPFileNameException(FileValidationException):
    pass


class InvalidFileAsPerConfigException(FileValidationException):
    pass
