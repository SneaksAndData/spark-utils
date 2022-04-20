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

    def serialize(self, separator: str = '|') -> str:
        """Serializes job socket to the format used by SparkJobArgs when reading from command line.

        Attributes:
            separator: Separator to use for serialization
        """
        return separator.join([self.alias, self.data_path, self.data_format])
