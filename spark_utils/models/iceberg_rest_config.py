# MIT License
#
# Copyright (c) 2022-2026 Ecco Sneaks & Data
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


from dataclasses import dataclass


@dataclass
class IcebergRestConfig:
    """
    Iceberg configs for Spark session
    """

    catalog_uri: str
    catalog_alias: str
    warehouse: str
    oauth2_uri: str | None = None
    scope: str | None = None
    client_id: str | None = None
    client_secret: str | None = None

    version: str = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"
    s3_version: str = "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
    catalog_class: str = "org.apache.iceberg.spark.SparkCatalog"
    sql_extensions: str = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    catalog_impl: str = "org.apache.iceberg.rest.RESTCatalog"

    def get_credentials(self) -> str | None:
        """
        Generate Iceberg REST credential
        """
        if self.client_id is None or self.client_secret is None:
            return None
        return f"{self.client_id}:{self.client_secret}"

    def has_auth(self) -> bool:
        """
        Check if all OAuth properties have been set
        """
        return (
            self.oauth2_uri is not None
            and self.client_id is not None
            and self.client_secret is not None
            and self.scope is not None
        )
