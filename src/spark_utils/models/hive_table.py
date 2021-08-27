"""
 Model classes for hive-related functionality
"""

from dataclasses import dataclass


@dataclass
class HiveTableColumn:
    """Describes a Hive Metastore table column"""
    name: str
    type: str
