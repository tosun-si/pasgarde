"""
This file contains DoFn classes representing functions like map, flatMap and filter but with error handling.
Errors are caught in an except block and a failure object is put in a side output.
The failure contains the current input element, the exception and its stack trace.
"""
from collections.abc import Callable
from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics import Metrics

from asgarde.failure import Failure

FAILURES = 'failures'

# Beam counters: one counter per pipeline step, incremented for each failure.
FAILURES_METRICS_NAMESPACE = 'asgarde-failures'


def no_action() -> None:
    pass


class ErrorHandlingDoFn(beam.DoFn):
    """
    Base DoFn class with error handling and lifecycle actions.

    `MemoryError` is re-raised: the worker is in an unstable state and the runner must handle it,
    not the dead letter queue.
    """

    def __init__(self,
                 step: str,
                 setup_action: Callable[[], None] = no_action,
                 start_bundle_action: Callable[[], None] = no_action,
                 finish_bundle_action: Callable[[], None] = no_action,
                 teardown_action: Callable[[], None] = no_action):
        super().__init__()
        self.step = step
        self.setup_action = setup_action
        self.start_bundle_action = start_bundle_action
        self.finish_bundle_action = finish_bundle_action
        self.teardown_action = teardown_action
        self.failures_counter = Metrics.counter(FAILURES_METRICS_NAMESPACE, step)

    def setup(self):
        self.setup_action()

    def start_bundle(self):
        self.start_bundle_action()

    def finish_bundle(self):
        self.finish_bundle_action()

    def teardown(self):
        self.teardown_action()

    def to_failure_output(self, element: Any, err: Exception) -> pvalue.TaggedOutput:
        if isinstance(err, MemoryError):
            raise err

        self.failures_counter.inc()
        return pvalue.TaggedOutput(FAILURES, Failure.from_exception(self.step, element, err))


class FlatMap(ErrorHandlingDoFn):
    """
    Custom DoFn class representing a flatMap operation with error handling.

    The outputs are materialized before being emitted: an iterable failing in the middle gives a failure only,
    not partial outputs that would be duplicated when the failure is replayed.
    """

    def __init__(self, step: str, input_element_mapper: Callable[..., Any], **lifecycle_actions):
        super().__init__(step, **lifecycle_actions)
        self.input_element_mapper = input_element_mapper

    def process(self, element, *args, **kwargs):
        try:
            results = list(self.input_element_mapper(element, *args, **kwargs))
        except Exception as err:
            yield self.to_failure_output(element, err)
            return

        yield from results


class Map(ErrorHandlingDoFn):
    """
    Custom DoFn class representing a map operation with error handling.
    """

    def __init__(self, step: str, input_element_mapper: Callable[..., Any], **lifecycle_actions):
        super().__init__(step, **lifecycle_actions)
        self.input_element_mapper = input_element_mapper

    def process(self, element, *args, **kwargs):
        try:
            yield self.input_element_mapper(element, *args, **kwargs)
        except Exception as err:
            yield self.to_failure_output(element, err)


class Filter(ErrorHandlingDoFn):
    """
    Custom DoFn class representing a filter operation with error handling.
    """

    def __init__(self, step: str, input_element_predicate: Callable[..., bool], **lifecycle_actions):
        super().__init__(step, **lifecycle_actions)
        self.input_element_predicate = input_element_predicate

    def process(self, element, *args, **kwargs):
        try:
            keep = self.input_element_predicate(element, *args, **kwargs)
        except Exception as err:
            yield self.to_failure_output(element, err)
            return

        if keep:
            yield element
