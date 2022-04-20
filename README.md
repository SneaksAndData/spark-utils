# Introduction 
Utility functions and classes for working with Dataframes, provisioning SparkSession and much more.

Core features:
- Provisioning Spark session with some routine settings set in advance, including Delta Lake configuration. You must have delta-core jars in class path for this to work.
- Spark job argument wrappers, allowing to specify job inputs for `spark.read.format(...).options(...).load(...)` and outputs for `spark.write.format(...).save(...)` in a generic way. Those are exposed as `source` and `target` built-in arguments (see example below).

Consider a simple Spark Job that reads `json` data from `source` and stores it as `parquet` in `target`. This job can be defined using `spark-utils` as below:

```python
from spark_utils.common.spark_job_args import SparkJobArgs
from spark_utils.common.spark_session_provider import SparkSessionProvider


def main(args=None):
    """
     Job entrypoint
    :param args:
    :return:
    """
    spark_args = SparkJobArgs()
        .parse(args)

    source_table = spark_args.source('json_source')
    target_table = spark_args.output('parquet_target')

    # Spark session and hadoop FS
    spark_session = SparkSessionProvider().get_session()
    df = spark_session.read.format(source_table.data_format).load(source_table.data_path)
    df.write.format(target_table.data_format).save(target_table.data_path)
```

Now we can call this job directly or with `spark-submit`. Note that you must have `spark-utils` in PYTHONPATH before running the script:
```commandline
spark-submit --master local[*] --deploy-mode client --name simpleJob ~/path/to/main.py --source 'json_source|file://tmp/test_json/*|json' --output 'parquet_target|file://tmp/test_parquet/*|parquet'
```

  - Job argument encryption is supported. This functionality requires an encryption key to be present in a cluster environment variable `RUNTIME_ENCRYPTION_KEY`. The only supported algorithm now is `fernet`. You can declare an argument as encrypted using `new_encrypted_arg` function. You then must pass an encrypted value to the declared argument, which will be decrypted by `spark-utils` when a job is executed and passed to the consumer.

For example, you can pass sensitive spark configuration (storage access keys, hive database passwords etc.) encrypted:

```python
import json

from spark_utils.common.spark_job_args import SparkJobArgs
from spark_utils.common.spark_session_provider import SparkSessionProvider


def main(args=None):
    spark_args = SparkJobArgs()
        .new_encrypted_arg("--custom-config", type=str, default=None,
                           help="Optional spark configuration flags to pass. Will be treated as an encrypted value.")
        .parse(args)

    spark_session = SparkSessionProvider(
        additional_configs=json.loads(
            spark_args.parsed_args.custom_config) if spark_args.parsed_args.custom_config else None).get_session()

    ...
```

- Metrics reporting via Opencensus library. Note that metrics API might change in the future.
- Delta Lake utilities
  - Table publishing to Hive Metastore.
  - Delta OSS compaction with row count / file optimization target.
- Models for common data operations like data copying etc. Note that actual code for those operations will be migrated to this repo a bit later.
- Utility functions for common data operations, for example, flattening parent-child hierarchy, view concatenation, column name clear etc.

There are so many possibilities with this project - please feel free to open an issue / PR adding new capabilities or fixing those nasty bugs!

# Getting Started
Spark Utils must be installed on your cluster or virtual env that Spark is using Python interpreter from:
```commandline
pip install spark-utils
```

# Build and Test
Test pipeline runs Spark in local mode, so everything can be tested against our current runtime. Update the image used in `build.yaml` if you require a test against a different runtime version.
