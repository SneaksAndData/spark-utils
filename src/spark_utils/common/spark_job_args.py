"""
 Argument provider for Spark jobs
"""

import argparse
from typing import Optional, Iterable

from src.spark_utils.models.job_socket import JobSocket


class SparkJobArgs:
    """
     Argsparse-based Spark job arguments provider

     This adds three default arguments to each job:
      - --source a|b|c d|e|f ...
      Describes inputs used by a job
      Here `a` is a mapping key that a developer should use to extract path/format information for the source
           `b` is a source path, in URI format: file:///, abfss:// etc.
           'c' is a data format: json, csv, delta etc.
      - --output a|b|c d|e|f...
      Describes output locations used by a job
      Same meanings as source attributes
      - --overwrite
      Controls overwrite behaviour. Will wipe the whole directory if set and honored by job developer.
    """
    def __init__(self, *, job_name):
        self._parser = argparse.ArgumentParser(description=f"Runtime arguments for {job_name}")
        self._parser.add_argument("--source", type=str, nargs='+', default=[],
                                  help='Sources to read data from, in a form of <source key>:<source path>')
        self._parser.add_argument("--output", type=str, nargs='+', default=[],
                                  help='Outputs to write data to, in a form of <output key>:<output path>')
        self._parser.add_argument("--overwrite", dest='overwrite', action='store_true', help="Overwrite outputs")

        self._parsed_args = None
        self._parsed_sources = None
        self._parsed_outputs = None
        self._overwrite = False

    def _sources(self) -> Iterable[JobSocket]:
        for source in self._parsed_args.source:
            yield JobSocket(*source.split('|'))

    def _outputs(self) -> Iterable[JobSocket]:
        for output in self._parsed_args.output:
            yield JobSocket(*output.split('|'))

    def new_arg(self, *args):
        """
         Adds one or more new arguments

        :param args: argsparse.add_argument(...)
        :return:
        """
        self._parser.add_argument(*args)

        return self

    def parse(self, arg_list=None):
        """
          Parse args using provided or implicit sys.argv list

        :param arg_list:
        :return:
        """
        if arg_list:
            self._parsed_args = self._parser.parse_args(arg_list)
        else:
            self._parsed_args = self._parser.parse_args()

        self._parsed_sources = list(self._sources())
        self._parsed_outputs = list(self._outputs())
        self._overwrite = self._parsed_args.overwrite

        return self

    def source(self, key) -> Optional[JobSocket]:
        """
          Get a JobSource from input args, by key

        :param key: Mapping key
        :return:
        """
        for parsed in self._parsed_sources:
            if parsed.source_key == key:
                return parsed

        return None

    def output(self, key) -> Optional[JobSocket]:
        """
          Get a JobOutput from input args, by key

        :param key: Mapping key
        :return:
        """
        for parsed in self._parsed_outputs:
            if parsed.output_key == key:
                return parsed

        return None

    def overwrite(self) -> bool:
        """
         Get a value for --overwrite argument

        :return: bool
        """
        return self._overwrite
