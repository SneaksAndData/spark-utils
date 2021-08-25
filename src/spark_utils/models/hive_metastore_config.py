"""
  Configuration for an external Hive Metastore
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class HiveMetastoreConfig:
    """
      Parameters required by Spark when connecting to an external Hive Metastore
    """
    metastore_version: str
    metastore_jars: str
    connection_url: Optional[str] = None
    connection_username: Optional[str] = None
    connection_password: Optional[str] = None
    connection_driver_name: Optional[str] = None
    metastore_uri: Optional[str] = None
