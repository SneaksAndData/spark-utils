"""
  Common helper functions.
"""
from hadoop_fs_wrapper.wrappers.file_system import FileSystem


def is_valid_source_path(file_system: FileSystem, path: str):
    """
     Checks whether a regexp path refers to a valid set of paths
    :param file_system: pyHadooopWrapper FileSystem
    :param path: path e.g. abfss://hello@world.com/path/part*.csv
    :return: dict containing "base_path" and "glob_filter"
    """
    return len(file_system.glob_status(path)) > 0
