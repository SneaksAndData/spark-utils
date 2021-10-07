"""
 Azure Monitor integration
"""

from datetime import datetime
from abc import abstractmethod
from typing import List, Dict, Optional

from opencensus.ext.azure import metrics_exporter
from opencensus.stats import measure as measure_module
from opencensus.stats import view as view_module
from opencensus.stats import aggregation as aggregation_module
from opencensus.stats import stats as stats_module
from opencensus.tags import tag_map as tag_map_module


class MetricAggregation:
    """
     Wrapper for metrics aggregators
    """

    @property
    @abstractmethod
    def underlying(self):
        """
          Underlying aggregator class from opencensus.
        :return:
        """
        pass


class Count(MetricAggregation):
    """
     Count aggregator
    """

    @property
    def underlying(self):
        return self._underlying

    def __init__(self):
        self._underlying = aggregation_module.CountAggregation()


class Sum(MetricAggregation):
    """
      Sum aggregator
    """

    @property
    def underlying(self):
        return self._underlying

    def __init__(self):
        self._underlying = aggregation_module.SumAggregation()


class MetricsService:
    """
      Azure Monitor metric reporter.
    """

    def __init__(self, *, azmon_connection_string: str, enable_standard_export=False, interval=15):
        self.stats = stats_module.stats
        self.view_manager = self.stats.view_manager
        self.stats_recorder = self.stats.stats_recorder
        self._registered_metrics = {}

        if azmon_connection_string:
            self.exporter = metrics_exporter.new_metrics_exporter(
                enable_standard_export=enable_standard_export,
                connection_string=azmon_connection_string,
                export_interval=interval,
            )

            self.view_manager.register_exporter(self.exporter)

    def register_metric(self, *,
                        metric_name: str,
                        metric_description: str,
                        metric_units: str,
                        dimensions: List[str],
                        aggregation: MetricAggregation):
        """
          Registers a metric.

        :param metric_name: Name of a metric.
        :param metric_description: Description of a metric.
        :param metric_units: Metric units.
        :param dimensions: Any dimensions metric is sliced by.
        :param aggregation: Metric aggregation.
        :return:
        """
        metric_measure = measure_module.MeasureInt(name=metric_name,
                                                   description=metric_description,
                                                   unit=metric_units)
        metric_view = view_module.View(name=f"{metric_name} view",
                                       description=metric_description,
                                       columns=dimensions,
                                       measure=metric_measure,
                                       aggregation=aggregation.underlying)

        self.view_manager.register_view(metric_view)
        measurement_map = self.stats_recorder.new_measurement_map()
        tag_map = tag_map_module.TagMap()

        self._registered_metrics.setdefault(metric_name, (metric_measure, measurement_map, tag_map))

    def track_metric(self, *,
                     metric_name: str,
                     metric_value: int,
                     dimension_values:
                     Dict[str, str]):
        """
          Tracks a metric value.
          
        :param metric_name: Name of a metric. 
        :param metric_value: Reported value.
        :param dimension_values: Dimension key-value pairs, if any.
        :return: 
        """
        if metric_name in self._registered_metrics:
            # TODO: add logging in this lib globally and use it here to report metric failures
            # Current behaviour is to simply skip metric report if it is not registered

            (metric, measurement_map, tag_map) = self._registered_metrics[metric_name]

            for dim, dim_value in dimension_values.items():
                if not tag_map.tag_key_exists(dim):
                    tag_map.insert(dim, dim_value)
                else:
                    tag_map.update(dim, dim_value)

            measurement_map.measure_int_put(metric, metric_value)
            measurement_map.record(tag_map)

    def read_metric(self, *, metric_name: str) -> Optional[list]:
        """
         Reads a metric from stat exporter.
        :param metric_name: Name of a metric.
        :return:
        """
        if metric_name in self._registered_metrics:
            (_, measurement_map, _) = self._registered_metrics[metric_name]

            return list(measurement_map.measure_to_view_map.get_metrics(datetime.utcnow()))

        return None
