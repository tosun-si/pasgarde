import dataclasses
import json
import pickle
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# BigQuery table schema of the failures converted with `Failure.to_dict`, same fields as the Java
# `FailureTransforms.SCHEMA` (snake_case instead of camelCase).
FAILURE_BIGQUERY_SCHEMA = {
    'fields': [
        {'name': 'pipeline_step', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'input_element', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'origin_element', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'exception_type', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'exception_message', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'stack_trace', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    ]
}


@dataclass
class Failure:
    """
    Object returned in the failure sink.

    Build it with `Failure.from_exception` in custom DoFn classes: the exception is always picklable and the
    stack trace is kept as a string (a pickled exception loses its traceback).

    `origin_element` is the element that entered the flow, when the origin is tracked with
    `CollectionComposer.with_origin_element`, `None` otherwise.
    """
    pipeline_step: str
    input_element: str
    exception: Exception
    stack_trace: str = ''
    # Default values also used when unpickling a failure of a previous version, without these fields.
    origin_element: str | None = None
    timestamp: datetime | None = None

    @property
    def exception_type(self) -> str:
        """Qualified name of the exception type, the original one for a `SerializableException`."""
        if isinstance(self.exception, SerializableException):
            return self.exception.original_type

        return qualified_name(type(self.exception))

    @property
    def exception_message(self) -> str | None:
        """Message of the exception, `None` if the exception has no message."""
        return str(self.exception) or None

    def to_dict(self) -> dict[str, Any]:
        """
        Flat and JSON serializable form of the failure, e.g. for `WriteToBigQuery` with `FAILURE_BIGQUERY_SCHEMA`.
        """
        return {
            'pipeline_step': self.pipeline_step,
            'input_element': self.input_element,
            'origin_element': self.origin_element,
            'exception_type': self.exception_type,
            'exception_message': self.exception_message,
            'stack_trace': self.stack_trace,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
        }

    def to_bad_record(self) -> tuple[Any, tuple[type, str, list[str]]]:
        """
        The failure in the dead letter format of the Beam `with_exception_handling`:
        `(element, (exception type, exception repr, stack trace lines))`, to add the failures to a Beam `ErrorHandler`
        with the bad records of the Beam transforms.

        The element is the origin element when it's tracked (to replay from the start), the input element otherwise.
        """
        element = self.origin_element if self.origin_element is not None else self.input_element

        return element, (type(self.exception), repr(self.exception), self.stack_trace.splitlines(keepends=True))

    def with_origin_element(self, origin_element: str) -> 'Failure':
        """Returns a copy of this failure with the given origin element."""
        return dataclasses.replace(self, origin_element=origin_element)

    @classmethod
    def from_exception(cls, pipeline_step: str, element: Any, exception: Exception) -> 'Failure':
        """
        Builds a failure that can't break the pipeline: the element conversion never raises and a non picklable
        exception is replaced by a `SerializableException` keeping its type and message.
        """
        return cls(
            pipeline_step=pipeline_step,
            input_element=element_as_string(element),
            exception=SerializableException.of(exception),
            stack_trace=''.join(traceback.format_exception(type(exception), exception, exception.__traceback__)),
            timestamp=datetime.now(timezone.utc)
        )


class SerializableException(Exception):
    """
    Picklable stand-in for an exception that can't be pickled (e.g. holding a lock or a client, or with a custom
    `__init__` signature): failures are pickled by Beam, storing such an exception would make the job fail.
    """

    def __init__(self, original_type: str, message: str) -> None:
        super().__init__(message)
        self.original_type = original_type
        self.message = message

    def __reduce__(self):
        return SerializableException, (self.original_type, self.message)

    def __repr__(self) -> str:
        return f'{self.original_type}({self.message!r})'

    @staticmethod
    def of(exception: Exception) -> Exception:
        """
        Returns the given exception if it survives a pickle round trip, otherwise a `SerializableException` copy.
        """
        try:
            pickle.loads(pickle.dumps(exception))
            return exception
        except Exception:
            return SerializableException(qualified_name(type(exception)), str(exception))


def qualified_name(exception_type: type) -> str:
    """Qualified name of a type, without the `builtins` module of the built-in exceptions."""
    if exception_type.__module__ == 'builtins':
        return exception_type.__qualname__

    return f'{exception_type.__module__}.{exception_type.__qualname__}'


def element_as_string(element: Any) -> str:
    """
    Converts the input element for the Failure object, a dict is converted to a JSON string.

    Never raises: the error handling must not break the pipeline (e.g. a dict with non JSON types).
    """
    try:
        return json.dumps(element, default=str) if isinstance(element, dict) else str(element)
    except Exception as err:
        try:
            return repr(element)
        except Exception:
            return f'<conversion of {type(element).__qualname__} failed: {err!r}>'
