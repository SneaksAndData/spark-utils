"""
  Common helper functions.
"""
import os
from typing import Optional

from cryptography.fernet import Fernet
from hadoop_fs_wrapper.wrappers.file_system import FileSystem


def is_valid_source_path(file_system: FileSystem, path: str):
    """
     Checks whether a regexp path refers to a valid set of paths
    :param file_system: pyHadooopWrapper FileSystem
    :param path: path e.g. abfss://hello@world.com/path/part*.csv
    :return: dict containing "base_path" and "glob_filter"
    """
    return len(file_system.glob_status(path)) > 0


def decrypt_sensitive(sensitive_content: Optional[str]) -> Optional[str]:
    """
      Decrypts a provided string

    :param sensitive_content: payload to decrypt
    :return: Decrypted payload
    """
    encryption_key = os.environ.get('RUNTIME_ENCRYPTION_KEY', None).encode('utf-8')

    if not encryption_key:
        print('Encryption key not set - skipping operation.')

    if encryption_key and sensitive_content:
        fernet = Fernet(encryption_key)
        return fernet.decrypt(sensitive_content.encode('utf-8')).decode('utf-8')

    return None
