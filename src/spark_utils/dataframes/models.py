"""
  Options for copy_data.py.
"""

from dataclasses import dataclass
from typing import Optional

from spark_utils.models.job_socket import JobSocket


@dataclass
class CopyDataOptions:
    """
      Options for copy_data.py

    :arg src: Source job socket
    :arg dest: Destination job socket
    :arg read_options: Spark session options to set when reading
    :arg include_filename: Adds "filename" column to the destination output.
    :arg include_row_sequence: Adds "sequence_number" column to the destination output.
    :arg clean_destination: Wipe destination path before starting a copy.
    :arg timestamp_column: Column name to use for evaluating data age.
    :arg timestamp_column_format: Format for the timestamp
    """
    src: JobSocket
    dest: JobSocket
    include_filename: bool = False
    include_row_sequence: bool = False
    clean_destination: bool = False
    clean_column_names: bool = False
    read_options: Optional[dict] = None
    output_file_count: Optional[int] = None
    timestamp_column: Optional[str] = None
    timestamp_column_format: Optional[str] = None
