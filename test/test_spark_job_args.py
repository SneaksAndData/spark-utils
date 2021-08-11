import pytest

from spark_utils.common.spark_job_args import SparkJobArgs


@pytest.mark.parametrize(
    "source,expected,key",
    [
        pytest.param(
            ["yahoo-exchange-rates-1|file://data/yahoo-exchange-rates-1.json|json",
             "yahoo-exchange-rates-2|file://data/yahoo-exchange-rates-2.json|json"],
            "file://data/yahoo-exchange-rates-2.json", "yahoo-exchange-rates-2", id="1"
        ),
        pytest.param(
            ["yahoo-exchange-rates-1|file://data/yahoo-exchange-rates-1.json|json"],
            "file://data/yahoo-exchange-rates-1.json", "yahoo-exchange-rates-1", id="2"
        ),
    ],
)
def test_parse_source(source, expected, key):
    sa = SparkJobArgs(job_name='test')
    sa.parse([
        "--source", *source,
        "--output", "exchange-rates|file://data/test-result.json|json",
        "--overwrite"
    ])

    assert sa.source(key).data_path == expected


@pytest.mark.parametrize(
    "overwrite,expected",
    [
        pytest.param(
            False,
            False, id="1"
        ),
        pytest.param(
            True,
            True, id="2"
        ),
    ],
)
def test_parse_overwrite(overwrite, expected):
    sa = SparkJobArgs(job_name='test')
    arglist = [
        "--source", "something|1|1",
        "--output", "anything|2|2",
    ]
    if overwrite:
        arglist.append("--overwrite")
    sa.parse(arglist)

    assert sa.overwrite() == expected
