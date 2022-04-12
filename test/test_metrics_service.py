import time

from spark_utils.metrics.azmon_metrics import MetricsService, Sum


def test_track_metric():
    ms = MetricsService(azmon_connection_string='', interval=1)

    ms.register_metric(
        metric_name='test_counter',
        metric_description='test',
        metric_units='test_counter',
        dimensions=['test_dim'],
        aggregation=Sum()
    )

    ms.track_metric(
        metric_name='test_counter',
        metric_value=1,
        dimension_values={
            'test_dim': 'test_dim_value'
        }
    )

    ms.track_metric(
        metric_name='test_counter',
        metric_value=2,
        dimension_values={
            'test_dim': 'test_dim_value'
        }
    )

    time.sleep(2)

    assert ms.read_metric(metric_name='test_counter')[0].time_series[0].points[0].value.value == 3
