# MIT License
#
# Copyright (c) 2022 Ecco Sneaks & Data
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
    :arg write_options: Spark session options to set when writing
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
    write_options: Optional[dict] = None
    output_file_count: Optional[int] = None
    timestamp_column: Optional[str] = None
    timestamp_column_format: Optional[str] = None
