import base64
import pgpy
import tempfile
from azure.storage.blob import BlobClient
from common.logger_utils import logger
from common.exception_handlers import raise_error
from common.constants import (
    LOG_ACTIVITY_END_SUCCESS,
    LOG_ACTIVITY_END_FAILED,
    INSTANCE_TYPE,
    ActivityTypes,
)
from common.connection_manager import PRIVATE_KEY_EMA_PGP
from common.audit_logger import (
    retrieve_activity_id,
    log_activity_end,
    log_activity_start,
    log_activity_error,
)
from common.helper_utils import create_activity_ref_details


def decrypt_pgp(
    source_blob_client: BlobClient, file_name: str, source_name: str, source_type: str
) -> tuple[str, int]:
    """
    decrypt the pgp file
    """
    try:
        file_name_pgp = file_name + ".pgp"
        activity_type = ActivityTypes.DECRYPTION.value
        logger.info("Started %s activity for file: %s", activity_type, file_name_pgp)
        activity_ref_details = create_activity_ref_details(
            activity_type=activity_type, file_name=file_name_pgp
        )
        logging_completed = False
        activity_id = retrieve_activity_id(
            activity_type=activity_type, instance_type=INSTANCE_TYPE
        )
        activity_run_id = log_activity_start(
            activity_id=activity_id,
            source_name=source_name,
            source_type=source_type,
            instance_type=INSTANCE_TYPE,
            source_file_name=file_name_pgp,
        )
        pgp_key_updated = base64.b64decode(PRIVATE_KEY_EMA_PGP)
        private_key, _ = pgpy.PGPKey.from_blob(pgp_key_updated)
        encrypted_message = pgpy.PGPMessage.from_blob(
            source_blob_client.download_blob().readall()
        )
        with private_key.unlock(""):
            decrypted_message = private_key.decrypt(encrypted_message).message
            if ".csv" in file_name:
                decrypted_message = decrypted_message.encode()
        with tempfile.NamedTemporaryFile(delete=False) as temp_dec_file:
            temp_dec_file_name = temp_dec_file.name
            temp_dec_file.write(decrypted_message)
        if not logging_completed:
            log_activity_end(
                activity_run_id=activity_run_id,
                run_status=LOG_ACTIVITY_END_SUCCESS,
                activity_ref_details=activity_ref_details,
                target_file_name=file_name,
            )
            logging_completed = True
        logger.info("Completed Decryption activity for file: %s", file_name)
        source_blob_size = len(decrypted_message)
        return temp_dec_file_name, source_blob_size
    except Exception as e:
        if not logging_completed:
            error_string = f"Failed: Failed to decrypt file: {file_name}. Error: {e}"
            log_activity_error(activity_run_id=activity_run_id, error_log=error_string)
            log_activity_end(
                activity_run_id=activity_run_id,
                run_status=LOG_ACTIVITY_END_FAILED,
                activity_ref_details=activity_ref_details,
            )
            raise_error(error_string=error_string)


decryption_handlers_map = {"pgp": decrypt_pgp}
