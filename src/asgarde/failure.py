import json
import pickle
import traceback
from dataclasses import dataclass
from typing import Any


@dataclass
class Failure:
    """
    Object returned in the failure sink.

    Build it with `Failure.from_exception` in custom DoFn classes: the exception is always picklable and the
    stack trace is kept as a string (a pickled exception loses its traceback).
    """
    pipeline_step: str
    input_element: str
    exception: Exception
    stack_trace: str = ''

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
            stack_trace=''.join(traceback.format_exception(type(exception), exception, exception.__traceback__))
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
            exception_type = type(exception)
            return SerializableException(f'{exception_type.__module__}.{exception_type.__qualname__}', str(exception))


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
