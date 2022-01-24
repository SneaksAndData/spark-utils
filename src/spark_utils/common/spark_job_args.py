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
 Argument provider for Spark jobs
"""

import argparse
from typing import Optional, Iterable

from spark_utils.common.functions import decrypt_sensitive
from spark_utils.models.job_socket import JobSocket


class DecryptAction(argparse.Action):
    """
      Action that performs decryption of a provided value using encryption key provided from the environment.
    """

    def __init__(self,
                 option_strings,
                 dest,
                 const=None,
                 default=None,
                 required=False,
                 **kwargs):
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            nargs=1,
            const=const,
            default=default,
            required=required,
            **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        arg_value = values[0]
        if arg_value.startswith("'"):
            arg_value = arg_value[1:]
        if arg_value.endswith("'"):
            arg_value = arg_value[:-1]
        setattr(namespace, self.dest, decrypt_sensitive(arg_value))


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

    def __init__(self):
        self._parser = argparse.ArgumentParser(description="Runtime arguments")
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

    def new_arg(self, *args, **kwargs):
        """
        Adds one or more new arguments
        :param args: argsparse.add_argument(...)
        :return:
        """
        self._parser.add_argument(*args, **kwargs)

        return self

    def new_encrypted_arg(self, *args, **kwargs):
        """
        Adds an argument that requires decryption before it can be used.

        :param args: argsparse.add_argument(...)
        :return:
        """
        if 'action' not in kwargs:
            kwargs.setdefault('action', DecryptAction)
        else:
            kwargs['action'] = DecryptAction

        self._parser.add_argument(*args, **kwargs)

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
            if parsed.alias == key:
                return parsed

        return None

    def output(self, key) -> Optional[JobSocket]:
        """
          Get a JobOutput from input args, by key

        :param key: Mapping key
        :return:
        """
        for parsed in self._parsed_outputs:
            if parsed.alias == key:
                return parsed

        return None

    def overwrite(self) -> bool:
        """
         Get a value for --overwrite argument

        :return: bool
        """
        return self._overwrite

    @property
    def parsed_args(self) -> argparse.Namespace:
        """
         Gets the parsed arguments

        :return: Namespace
        """
        return self._parsed_args
