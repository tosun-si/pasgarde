import json
from typing import Iterable

import apache_beam as beam
from apache_beam import pvalue

from asgarde.failure import Failure

FAILURES = 'failures'

"""
This file contains DoFn classes representing functions like map, flatMap and filter but with error handling.
Errors are caught in a catch bloc and a failure object are put in a side output.
The failure contains the current input element and stackTrace.
"""


def get_input_element(element) -> str:
    if isinstance(element, dict):
        return json.dumps(element)
    else:
        return str(element)


class FlatMap(beam.DoFn):

    def __init__(self, step: str, fn):
        self.step = step
        self.fn = fn

        super(FlatMap, self).__init__()

    def process(self, element, *args, **kwargs):
        try:
            results: Iterable = self.fn(element, *args, **kwargs)

            for result in results:
                yield result
        except Exception as err:
            failure = Failure(
                pipeline_step=self.step,
                input_element=get_input_element(element),
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class Map(beam.DoFn):

    def __init__(self, step: str, fn):
        self.step = step
        self.fn = fn

        super(Map, self).__init__()

    def process(self, element, *args, **kwargs):
        try:
            yield self.fn(element, *args, **kwargs)
        except Exception as err:
            failure = Failure(
                pipeline_step=self.step,
                input_element=get_input_element(element),
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class Filter(beam.DoFn):

    def __init__(self, step: str, predicate):
        self.step = step
        self.predicate = predicate

        super(Filter, self).__init__()

    def process(self, element, *args, **kwargs):
        try:
            if self.predicate(element, *args, **kwargs):
                yield element
        except Exception as err:
            failure = Failure(
                pipeline_step=self.step,
                input_element=get_input_element(element),
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)
