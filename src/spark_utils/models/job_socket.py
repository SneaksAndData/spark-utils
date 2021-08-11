"""
 Input mapping class for all python jobs
"""

from dataclasses import dataclass


@dataclass
class JobSocket:
    """
     Input/Output data map

     Attributes:
         alias: mapping key to be used by a consumer
         data_path: fully qualified path to actual data, i.e. abfss://..., s3://... etc.
         data_format: data format, i.e. csv, json, delta etc.
    """
    alias: str
    data_path: str
    data_format: str
