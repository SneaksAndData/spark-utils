"""
 Spark on Kubernetes configuration flags.
"""
import socket

from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class SparkKubernetesConfig:
    """
    Configuration parameters required to launch a k8s driver in client mode.
    """

    def __post_init__(self):
        self.driver_ip = socket.gethostbyname(socket.gethostname())
        self.driver_name = socket.gethostname()

    # Spark Application name for this session.
    application_name: str

    # Kubernetes namespace where to launch executors.
    k8s_namespace: str

    # Image to use for executors.
    spark_image: str

    # Group for the Spark UID.
    spark_gid: Optional[str] = "0"

    # Spark UID.
    spark_uid: Optional[str] = "1001"

    # Default memory to assign to executors if spark.executor.memory is not provided.
    default_executor_memory: Optional[int] = 2000

    # Name of a driver host.
    driver_name: Optional[str] = None

    # IP address of a driver host.
    driver_ip: Optional[str] = None

    # Prefix to use when naming executors.
    executor_name_prefix: Optional[str] = None

    # Default node affinity for all executors.
    executor_node_affinity: Optional[Dict[str, str]] = None
