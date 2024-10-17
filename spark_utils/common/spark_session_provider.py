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
  Provides convenient building of a Spark Session
"""
import base64
import json
import logging
import os
import tempfile
import uuid
from typing import Optional, List, Dict

import backoff
from py4j.protocol import Py4JJavaError

try:
    from kubernetes.client import (
        V1Pod,
        V1ObjectMeta,
        V1PodSpec,
        V1Container,
        V1ContainerPort,
        V1EnvVar,
        V1PodSecurityContext,
        V1NodeAffinity,
        V1NodeSelector,
        V1NodeSelectorTerm,
        V1NodeSelectorRequirement,
        V1Toleration,
    )
except ModuleNotFoundError:
    pass
import pyspark
from pyspark.sql import SparkSession

from spark_utils.models.k8s_config import SparkKubernetesConfig
from spark_utils.models.hive_metastore_config import HiveMetastoreConfig


class SparkSessionProvider:
    """
    Provider of a Spark session and related objects
    """

    DELTA_CATALOG_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
    TRANSIENT_INIT_ERRORS = ["temporary failure in name resolution"]

    def __init__(
        self,
        *,
        delta_lake_version="2.12:3.2.1",
        hive_metastore_config: Optional[HiveMetastoreConfig] = None,
        additional_packages: Optional[List[str]] = None,
        additional_configs: Optional[Dict[str, str]] = None,
        run_local=False,
        session_init_max_backoff_seconds=180,
    ):
        """
        :param delta_lake_version: Delta lake package version.
        :param hive_metastore_config: Optional configuration of a hive metastore that should be connected to this Spark Session.
        :param additional_packages: Additional jars to download. Would not override jars installed or provided from spark-submit.
         This setting only works if a session is started from python and not spark-submit.
        :param additional_configs: Any additional spark configurations.
        :param run_local: Whether single-node local master should be used.
        :param session_init_max_backoff_seconds: Maximum amount of time to spend in backoff retries in case of transient session creation errors.
        """

        self._session_init_max_backoff_seconds = session_init_max_backoff_seconds
        logging.getLogger("backoff").addHandler(logging.StreamHandler())

        packages = [f"io.delta:delta-spark_{delta_lake_version}"]
        if additional_packages:
            packages.extend(additional_packages)

        self._spark_session_builder = (
            SparkSession.builder.config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.extensions", SparkSessionProvider.DELTA_CATALOG_EXTENSION)
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.ivy", os.path.join(tempfile.gettempdir(), ".ivy2"))
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        )

        if hive_metastore_config:
            if hive_metastore_config.connection_driver_name:
                self._spark_session_builder = (
                    self._spark_session_builder.config(
                        "spark.hadoop.javax.jdo.option.ConnectionURL", hive_metastore_config.connection_url
                    )
                    .config(
                        "spark.hadoop.javax.jdo.option.ConnectionUserName", hive_metastore_config.connection_username
                    )
                    .config(
                        "spark.hadoop.javax.jdo.option.ConnectionPassword", hive_metastore_config.connection_password
                    )
                    .config(
                        "spark.hadoop.javax.jdo.option.ConnectionDriverName",
                        hive_metastore_config.connection_driver_name,
                    )
                )
            elif hive_metastore_config.metastore_uri:
                self._spark_session_builder = self._spark_session_builder.config(
                    "spark.hadoop.hive.metastore.uris", hive_metastore_config.metastore_uri
                )
            else:
                raise ValueError("Invalid Hive Metastore Configuration provided")

            self._spark_session_builder = (
                self._spark_session_builder.config(
                    "spark.sql.hive.metastore.version", hive_metastore_config.metastore_version
                )
                .config("spark.sql.hive.metastore.jars", hive_metastore_config.metastore_jars)
                .config("spark.sql.catalogImplementation", "hive")
            )

        if additional_configs:
            for config_key, config_value in additional_configs.items():
                self._spark_session_builder = self._spark_session_builder.config(config_key, config_value)

        self._run_local = run_local

    @property
    def session_builder(self) -> pyspark.sql.session.SparkSession.Builder:
        """
        Return a current session builder object.
        """
        return self._spark_session_builder

    def with_astra_bundle(self, db_name: str, bundle_bytes: str) -> "SparkSessionProvider":
        """
         Mounts Astra DB bundle into a Spark Session.

        :param db_name: Astra database name to use as Spark Catalog reference
        :param bundle_bytes: Base64 encoded secure bundle zip content. Generate with `cat bundle.zip | base64`
        """
        bundle_file_name = f"{db_name}_bundle"
        bundle_path = os.path.join(tempfile.gettempdir(), ".astra")
        os.makedirs(bundle_path, exist_ok=True)

        with open(os.path.join(bundle_path, bundle_file_name), "wb") as bundle_file:
            bundle_file.write(base64.b64decode(bundle_bytes))

        self._spark_session_builder = self._spark_session_builder.config(
            "spark.files", os.path.join(bundle_path, bundle_file_name)
        )

        return self

    def configure_for_k8s(
        self, master_url: str, spark_config: SparkKubernetesConfig, master_port: int = 443
    ) -> "SparkSessionProvider":
        """
        Configures spark session for using Kubernetes as a resource manager.

        :param master_url: Kubernetes API URL, i.e. https://my-server-api.mydomain.com.
        :param spark_config: Spark on K8S-specific configurations.
        :param master_port: Connection port for the API server.
        """
        executor_name = spark_config.executor_name_prefix or str(uuid.uuid4())
        # base configuration
        self._spark_session_builder = (
            self._spark_session_builder.master(f"k8s://{master_url}:{master_port}")
            .config("spark.kubernetes.driver.pod.name", spark_config.driver_name or os.getenv("SPARK_DRIVER_NAME"))
            .config("spark.app.name", spark_config.application_name)
            .config("spark.kubernetes.executor.podNamePrefix", executor_name)
            .config("spark.driver.host", spark_config.driver_ip or os.getenv("SPARK_DRIVER_IP"))
            .config("spark.kubernetes.namespace", spark_config.k8s_namespace)
            .config("spark.kubernetes.container.image", spark_config.spark_image)
            .config("spark.shuffle.service.enabled", "false")
        )  # disable external shuffle service for now

        # generate executor template
        executor_template = V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=V1ObjectMeta(name="spark-executor", namespace=spark_config.k8s_namespace),
            spec=V1PodSpec(
                containers=[
                    V1Container(
                        name="spark-executor",
                        image=spark_config.spark_image,
                        ports=[V1ContainerPort(name="http", container_port=8080, protocol="TCP")],
                        env=[V1EnvVar(name="SPARK_WORKER_WEBUI_PORT", value="8080")],
                    )
                ],
                restart_policy="Never",
                security_context=V1PodSecurityContext(
                    run_as_user=spark_config.spark_uid, run_as_group=spark_config.spark_gid
                ),
                affinity=V1NodeAffinity(
                    required_during_scheduling_ignored_during_execution=V1NodeSelector(
                        node_selector_terms=[
                            V1NodeSelectorTerm(
                                match_expressions=[
                                    V1NodeSelectorRequirement(key=affinity_key, values=[affinity_value], operator="In")
                                    for affinity_key, affinity_value in spark_config.executor_node_affinity.items()
                                ]
                            )
                        ]
                    )
                )
                if spark_config.executor_node_affinity
                else None,
                tolerations=[
                    V1Toleration(effect="NoSchedule", key=affinity_key, operator="Equal", value=affinity_value)
                    for affinity_key, affinity_value in spark_config.executor_node_affinity.items()
                ]
                if spark_config.executor_node_affinity
                else None,
            ),
        )

        template_path = os.path.join(tempfile.gettempdir(), executor_name)
        os.makedirs(template_path, exist_ok=True)

        with open(os.path.join(template_path, "template.yml"), "w", encoding="utf-8") as pod_template:
            pod_template.write(json.dumps(executor_template.to_dict()))

        self._spark_session_builder = self._spark_session_builder.config(
            "spark.kubernetes.executor.podTemplateFile", os.path.join(template_path, "template.yml")
        )

        return self

    def get_session(self):
        """
          Launch a configured Spark Session.

        :return: SparkSession
        """

        def is_fatal_py4j_error(error: Py4JJavaError) -> bool:
            """
            Check whether this py4j error can be retried.
            """
            return not any(transient_error in str(error).lower() for transient_error in self.TRANSIENT_INIT_ERRORS)

        @backoff.on_exception(
            wait_gen=backoff.expo,
            exception=(Py4JJavaError,),
            raise_on_giveup=True,
            giveup=is_fatal_py4j_error,
            max_time=self._session_init_max_backoff_seconds,
        )
        def _get_session():
            if os.environ.get("PYTEST_CURRENT_TEST") or self._run_local:
                return self._spark_session_builder.master("local[*]").getOrCreate()

            return self._spark_session_builder.getOrCreate()

        return _get_session()
