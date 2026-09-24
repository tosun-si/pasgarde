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


def origin_as_string(origin_to_string: Callable[[Any], Any], origin: Any) -> str:
    """
    Converts the origin element with the user function. Never raises: the error handling must not break the pipeline.
    """
    try:
        return str(origin_to_string(origin))
    except Exception as err:
        return f'<conversion of the origin element failed: {err!r}>'


class ErrorHandlingDoFn(beam.DoFn):
    """
    Base DoFn class with error handling and lifecycle actions.

    `MemoryError` is re-raised: the worker is in an unstable state and the runner must handle it,
    not the dead letter queue.

    With an `origin_to_string` function, the elements are `(origin, value)` pairs: the function is applied on the
    value, the origin is kept with the outputs and converted to a string **only when a failure occurs**.
    """

    def __init__(self,
                 step: str,
                 setup_action: Callable[[], None] = no_action,
                 start_bundle_action: Callable[[], None] = no_action,
                 finish_bundle_action: Callable[[], None] = no_action,
                 teardown_action: Callable[[], None] = no_action,
                 origin_to_string: Callable[[Any], Any] | None = None):
        super().__init__()
        self.step = step
        self.setup_action = setup_action
        self.start_bundle_action = start_bundle_action
        self.finish_bundle_action = finish_bundle_action
        self.teardown_action = teardown_action
        self.origin_to_string = origin_to_string
        self.failures_counter = Metrics.counter(FAILURES_METRICS_NAMESPACE, step)

    def setup(self):
        self.setup_action()

    def start_bundle(self):
        self.start_bundle_action()

    def finish_bundle(self):
        self.finish_bundle_action()

    def teardown(self):
        self.teardown_action()

    def split_origin(self, element: Any) -> tuple[Any, Any]:
        """Returns the `(origin, value)` of the element, the origin being `None` when it's not tracked."""
        return element if self.origin_to_string is not None else (None, element)

    def with_origin(self, origin: Any, output: Any) -> Any:
        """Keeps the origin with the output when it's tracked."""
        return (origin, output) if self.origin_to_string is not None else output

    def to_failure_output(self, element: Any, err: Exception, origin: Any = None) -> pvalue.TaggedOutput:
        if isinstance(err, MemoryError):
            raise err

        self.failures_counter.inc()
        failure = Failure.from_exception(self.step, element, err)

        if self.origin_to_string is not None:
            failure = failure.with_origin_element(origin_as_string(self.origin_to_string, origin))

        return pvalue.TaggedOutput(FAILURES, failure)


class FlatMap(ErrorHandlingDoFn):
    """
    Custom DoFn class representing a flatMap operation with error handling.

    The outputs are materialized before being emitted: an iterable failing in the middle gives a failure only,
    not partial outputs that would be duplicated when the failure is replayed.
    """

    def __init__(self, step: str, input_element_mapper: Callable[..., Any], **options):
        super().__init__(step, **options)
        self.input_element_mapper = input_element_mapper

    def process(self, element, *args, **kwargs):
        origin, value = self.split_origin(element)
        try:
            results = list(self.input_element_mapper(value, *args, **kwargs))
        except Exception as err:
            yield self.to_failure_output(value, err, origin)
            return

        yield from (self.with_origin(origin, result) for result in results)


class Map(ErrorHandlingDoFn):
    """
    Custom DoFn class representing a map operation with error handling.
    """

    def __init__(self, step: str, input_element_mapper: Callable[..., Any], **options):
        super().__init__(step, **options)
        self.input_element_mapper = input_element_mapper

    def process(self, element, *args, **kwargs):
        origin, value = self.split_origin(element)
        try:
            output = self.input_element_mapper(value, *args, **kwargs)
        except Exception as err:
            yield self.to_failure_output(value, err, origin)
            return

        yield self.with_origin(origin, output)


class Filter(ErrorHandlingDoFn):
    """
    Custom DoFn class representing a filter operation with error handling.
    """

    def __init__(self, step: str, input_element_predicate: Callable[..., bool], **options):
        super().__init__(step, **options)
        self.input_element_predicate = input_element_predicate

    def process(self, element, *args, **kwargs):
        origin, value = self.split_origin(element)
        try:
            keep = self.input_element_predicate(value, *args, **kwargs)
        except Exception as err:
            yield self.to_failure_output(value, err, origin)
            return

        if keep:
            yield self.with_origin(origin, value)
