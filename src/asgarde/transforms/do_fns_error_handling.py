"""
This file contains DoFn classes representing functions like map, flatMap and filter but with error handling.
Errors are caught in an except block and a failure object is put in a side output.
The failure contains the current input element, the exception and its stack trace.
"""
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics import Metrics

from asgarde.failure import Failure, SerializableException, element_as_string, qualified_name

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


def dead_letter_to_failure(dead_letter: tuple[Any, tuple[type, str, list[str]]],
                           pipeline_step: str,
                           input_element_to_string: Callable[[Any], str] | None = None,
                           input_coder: Any = None) -> Failure:
    """
    Converts a dead letter of the Beam `with_exception_handling`, `(element, (exception type, exception repr, stack
    trace lines))`, to a `Failure`, and increments the failure counter of the step.

    Beam only gives the representation of the exception: the exception is a `SerializableException` with the
    original type and the message of the last line of the stack trace.
    """
    element, (exception_type, exception_repr, stack_trace_lines) = dead_letter
    type_name = qualified_name(exception_type)
    last_line = stack_trace_lines[-1].strip() if stack_trace_lines else ''
    message = last_line.split(': ', 1)[1] if ': ' in last_line else exception_repr

    Metrics.counter(FAILURES_METRICS_NAMESPACE, pipeline_step).inc()

    failure = Failure(
        pipeline_step=pipeline_step,
        input_element=element_as_string(element, input_element_to_string),
        exception=SerializableException(type_name, message),
        stack_trace=''.join(stack_trace_lines),
        timestamp=datetime.now(timezone.utc)
    )

    return failure.with_encoded_input_element(element, input_coder) if input_coder is not None else failure


class ErrorHandlingDoFn(beam.DoFn):
    """
    Base DoFn class with error handling and lifecycle actions.

    `MemoryError` is re-raised: the worker is in an unstable state and the runner must handle it,
    not the dead letter queue.

    With an `origin_to_string` function, the elements are `(origin, value)` pairs: the function is applied on the
    value, the origin is kept with the outputs and converted to a string **only when a failure occurs**.

    `input_element_to_string` converts the input element in the failures (default conversion if `None`), and the
    `input_coder` / `origin_coder` encode the input and origin elements in the failures (not encoded if `None`).
    """

    def __init__(self,
                 step: str,
                 setup_action: Callable[[], None] = no_action,
                 start_bundle_action: Callable[[], None] = no_action,
                 finish_bundle_action: Callable[[], None] = no_action,
                 teardown_action: Callable[[], None] = no_action,
                 origin_to_string: Callable[[Any], Any] | None = None,
                 input_element_to_string: Callable[[Any], str] | None = None,
                 input_coder: Any = None,
                 origin_coder: Any = None):
        super().__init__()
        self.step = step
        self.setup_action = setup_action
        self.start_bundle_action = start_bundle_action
        self.finish_bundle_action = finish_bundle_action
        self.teardown_action = teardown_action
        self.origin_to_string = origin_to_string
        self.input_element_to_string = input_element_to_string
        self.input_coder = input_coder
        self.origin_coder = origin_coder
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
        failure = Failure.from_exception(self.step, element, err, self.input_element_to_string)

        if self.input_coder is not None:
            failure = failure.with_encoded_input_element(element, self.input_coder)

        if self.origin_to_string is not None:
            failure = failure.with_origin_element(origin_as_string(self.origin_to_string, origin))

            if self.origin_coder is not None:
                failure = failure.with_encoded_origin_element(origin, self.origin_coder)

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
