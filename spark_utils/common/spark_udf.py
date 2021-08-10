"""
  Spark UDF shared by all jobs
"""

import uuid


def getfromstr_uuid(input_str):
    """
      Generates UUID seeded by input
    :param input_str: Any string value
    :return: UUID that only matches provided input
    """
    null_namespace = type('', (), dict(bytes=b''))()
    return str(uuid.uuid3(null_namespace, input_str))
