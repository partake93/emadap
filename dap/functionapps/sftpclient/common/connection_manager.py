import json
import pyodbc
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from common.helper_utils import raise_error
from common.constants import (
    ACTIVITY_FILE_CONFIG_TBL,
    CONTROL_TBL_SCHEMA,
    PRIVATE_KEY_EMA_PGP,
    PUBLIC_KEY_EMA_PGP,
    KV_ENABLE,
    KV_PRIVATE_KEY_EMA_PGP_SECRET_NAME,
    KV_PUBLIC_KEY_EMA_PGP_SECRET_NAME,
    KV_URL,
    METADATA_SQL_DB_CONNECTION_STRING,
    METADATA_SQL_DB_CONNECTION_SECRET_NAME,
    EZ_PRESTAGING_ADLS_CONNECTION_STRING,
    EZ_PRESTAGING_BLOB_CONNECTION_STRING,
    IZ_STAGING_ADLS_CONNECTION_STRING,
    EZ_PRESTAGING_ADLS_CONNECTION_SECRET_NAME,
    EZ_PRESTAGING_BLOB_CONNECTION_SECRET_NAME,
    IZ_STAGING_ADLS_CONNECTION_SECRET_NAME,
)


def read_scenarios_configs(file_path: str) -> dict:
    """
    return file name pattern dict
    """
    with open(f"{file_path}") as file:
        pattern_dict = json.load(file)
    return pattern_dict


def get_container_client(
    connection_string: str, container_path: str
) -> ContainerClient:
    """
    Get the container client object for the container location provided
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        container_client = blob_service_client.get_container_client(container_path)
    except Exception as e:
        raise_error(
            error_string=f"Unable to get container client. An error occurred: {e}"
        )
    return container_client


def get_output_client(
    file_configs: list,
    file_pattern_name: str,
    connection_string: str,
    output_container_path: str,
    source_name: str,
) -> ContainerClient:
    """
    Get the output container client based on the provided pattern type.

    Args:
        file_configs (list): A list of dictionaries containing pattern
        information.
        file_pattern_name (str): The type of pattern to match.
        connection_string (str): The connection string for the storage account.
        output_container_path (str): The base path for the output container.

    Returns:
        ContainerClient: The client for the matched output container.
    """

    for item in file_configs:
        file_config = item.get("file_config")
        if (
            file_config.get("zip_pattern_name") == file_pattern_name
            or file_config.get("file_pattern_name") == file_pattern_name
        ):
            container_path = f"{output_container_path}/{source_name}/current/{file_config['frequency']}"
            container_client = get_container_client(
                connection_string=connection_string, container_path=container_path
            )
            return container_client

    raise_error(error_string=f"No matching pattern found for type: {file_pattern_name}")


def reterive_kv_secret(
    vault_url: str,
    secret_name: str,
) -> str:
    """
    Retrieve a secret from key vault
    """
    try:
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=vault_url, credential=credential)
        kv_secret = secret_client.get_secret(secret_name).value
    except Exception as e:
        error_string = (
            f"Unable to retrieve {secret_name} from Key vault. An error occurred: {e}"
        )
        raise_error(error_string)
    return kv_secret


if KV_ENABLE.lower() == "true":
    PRIVATE_KEY_EMA_PGP = reterive_kv_secret(KV_URL, KV_PRIVATE_KEY_EMA_PGP_SECRET_NAME)
    PUBLIC_KEY_EMA_PGP = reterive_kv_secret(KV_URL, KV_PUBLIC_KEY_EMA_PGP_SECRET_NAME)
    METADATA_SQL_DB_CONNECTION_STRING = reterive_kv_secret(
        KV_URL, METADATA_SQL_DB_CONNECTION_SECRET_NAME
    )
    EZ_PRESTAGING_ADLS_CONNECTION_STRING = reterive_kv_secret(
        KV_URL, EZ_PRESTAGING_ADLS_CONNECTION_SECRET_NAME
    )
    EZ_PRESTAGING_BLOB_CONNECTION_STRING = reterive_kv_secret(
        KV_URL, EZ_PRESTAGING_BLOB_CONNECTION_SECRET_NAME
    )
    IZ_STAGING_ADLS_CONNECTION_STRING = reterive_kv_secret(
        KV_URL, IZ_STAGING_ADLS_CONNECTION_SECRET_NAME
    )
else:
    PRIVATE_KEY_EMA_PGP = PRIVATE_KEY_EMA_PGP
    PUBLIC_KEY_EMA_PGP = PUBLIC_KEY_EMA_PGP
    METADATA_SQL_DB_CONNECTION_STRING = METADATA_SQL_DB_CONNECTION_STRING
    EZ_PRESTAGING_ADLS_CONNECTION_STRING = EZ_PRESTAGING_ADLS_CONNECTION_STRING
    EZ_PRESTAGING_BLOB_CONNECTION_STRING = EZ_PRESTAGING_BLOB_CONNECTION_STRING
    IZ_STAGING_ADLS_CONNECTION_STRING = IZ_STAGING_ADLS_CONNECTION_STRING


def read_file_configs():
    """
    Reads file configurations from the database.
    """
    query = f"""
        SELECT 
            FILE_PATTERN_NAME, 
            FILE_PATTERN, 
            JSON_CONFIG, 
            FREQUENCY 
        FROM 
            {CONTROL_TBL_SCHEMA}.{ACTIVITY_FILE_CONFIG_TBL} 
        WHERE 
            IS_ENABLED = 1 AND COMPRESSED_FILE_ID IS NULL AND FILE_TYPE <> 'zip'
    """
    try:
        with pyodbc.connect(METADATA_SQL_DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:

                cursor.execute(query)

                config_list = []
                for row in cursor.fetchall():
                    file_config = {
                        "file_pattern_name": row.FILE_PATTERN_NAME,
                        "file_pattern": row.FILE_PATTERN.replace("\\\\", "\\"),
                        "frequency": row.FREQUENCY,
                    }
                    config_dict = {"file_config": file_config}
                    if row.JSON_CONFIG:
                        file_type_config = json.loads(row.JSON_CONFIG.replace("'", '"'))
                        config_dict = {
                            "file_config": file_config,
                            "file_type_config": file_type_config,
                        }

                    config_list.append(config_dict)
    except Exception as e:
        raise_error(
            error_string=f"Unable to fetch file configurations from the database. An error occurred: {e}"
        )
    return config_list


def read_zip_file_configs():
    """
    Fetches zip file configurations from the database.
    """
    query = f"""
        SELECT 
            zip.FILE_PATTERN_NAME AS ZIP_PATTERN_NAME, 
            zip.FILE_PATTERN AS ZIP_PATTERN, 
            file_type.FILE_PATTERN_NAME, 
            file_type.FILE_PATTERN, 
            file_type.JSON_CONFIG, 
            file_type.FREQUENCY 
        FROM 
            {CONTROL_TBL_SCHEMA}.{ACTIVITY_FILE_CONFIG_TBL} zip
        INNER JOIN 
            {CONTROL_TBL_SCHEMA}.{ACTIVITY_FILE_CONFIG_TBL} file_type
        ON 
            zip.FILE_ID = file_type.COMPRESSED_FILE_ID 
        WHERE 
            zip.IS_ENABLED = 1 AND file_type.IS_ENABLED = 1
    """
    try:
        with pyodbc.connect(METADATA_SQL_DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:

                cursor.execute(query)

                config_list = []
                for row in cursor.fetchall():
                    file_config = {
                        "zip_pattern_name": row.ZIP_PATTERN_NAME,
                        "zip_pattern": row.ZIP_PATTERN.replace("\\\\", "\\"),
                        "file_pattern_name": row.FILE_PATTERN_NAME,
                        "file_pattern": row.FILE_PATTERN.replace("\\\\", "\\"),
                        "frequency": row.FREQUENCY,
                    }

                    config_dict = {"file_config": file_config}
                    if row.JSON_CONFIG:
                        file_type_config = json.loads(row.JSON_CONFIG.replace("'", '"'))
                        config_dict = {
                            "file_config": file_config,
                            "file_type_config": file_type_config,
                        }

                    config_list.append(config_dict)
    except Exception as e:
        raise_error(
            error_string=f"Unable to fetch zip file configurations from the database. An error occurred: {e}"
        )
    return config_list


def get_source_file_prefix(
    file_pattern_name: str, file_in_zip_pattern_name: str
) -> str:
    """
    Fetches source file prefix from the database.
    """
    query = f"""
        SELECT 
            FILE_PREFIX 
        FROM 
            {CONTROL_TBL_SCHEMA}.{ACTIVITY_FILE_CONFIG_TBL}
        WHERE 
            FILE_PATTERN_NAME = '{file_pattern_name}';
    """
    if file_in_zip_pattern_name is not None:
        query = f"""
        SELECT 
            FILE_PREFIX
        FROM
            {CONTROL_TBL_SCHEMA}.{ACTIVITY_FILE_CONFIG_TBL}
        WHERE
            COMPRESSED_FILE_ID = (SELECT DISTINCT FILE_ID 
                                  FROM {CONTROL_TBL_SCHEMA}.{ACTIVITY_FILE_CONFIG_TBL}
                                  WHERE FILE_PATTERN_NAME = '{file_pattern_name}')
            AND FILE_PATTERN_NAME= '{file_in_zip_pattern_name}';

    """
    try:
        with pyodbc.connect(METADATA_SQL_DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:

                cursor.execute(query)

                file_prefix = cursor.fetchone()
                if file_prefix:
                    return file_prefix.FILE_PREFIX
    except Exception as e:
        raise_error(
            error_string=f"Unable to fetch file prefix from the database. An error occurred: {e}"
        )
    return ""
