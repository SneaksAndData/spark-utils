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
    sa = SparkJobArgs()
    sa.parse([
        "--source", *source,
        "--output", "exchange-rates|file://data/test-result.json|json",
        "--overwrite"
    ])

    assert sa.source(key).data_path == expected


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param(
            ["colA", "colB", "colC"],
            id="1"
        ),
        pytest.param(
            ["colA"],
            id="2"
        ),
    ],
)
def test_parsed_args(columns):
    sa = SparkJobArgs()
    sa.new_arg("--columns", type=str, nargs='+', default=[],
               help='Unit Test')
    sa.parse([
        "--columns", *columns
    ])
    assert sa.parsed_args.columns == columns


def test_new_arg_chain():
    sa = SparkJobArgs()
    parsed = sa\
        .new_arg("--arg1", type=str, help='Argument 1')\
        .new_arg("--arg2", type=str, help='Argument 2')\
        .parse(["--arg1", "value1", "--arg2", "value2"])

    assert parsed.parsed_args.arg1 == "value1" and parsed.parsed_args.arg2 == "value2"


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
    sa = SparkJobArgs()
    arglist = [
        "--source", "something|1|1",
        "--output", "anything|2|2",
    ]
    if overwrite:
        arglist.append("--overwrite")
    sa.parse(arglist)

    assert sa.overwrite() == expected
