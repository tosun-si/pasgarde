import json
from typing import Iterable, Callable, Any

import apache_beam as beam
from apache_beam import pvalue

from asgarde.failure import Failure

FAILURES = 'failures'

"""
This file contains DoFn classes representing functions like map, flatMap and filter but with error handling.
Errors are caught in a except bloc and a failure object are put in a side output.
The failure contains the current input element and exception.
"""


def get_input_element(element) -> str:
    """
    Get the input element for the Failure object.
    For a dict a json string is returned

    For other object, the string representation of the object is returned.
    """
    return json.dumps(element) if isinstance(element, dict) else str(element)


class FlatMap(beam.DoFn):
    """
    Custom DnFn class representing a flatMap operation with error handling.
    """

    def __init__(self,
                 step: str,
                 input_element_mapper: Callable[[Any], Any],
                 setup_action: Callable[[], None] = lambda: None,
                 start_bundle_action: Callable[[], None] = lambda: None,
                 finish_bundle_action: Callable[[], None] = lambda: None,
                 teardown_action: Callable[[], None] = lambda: None):
        self.step = step
        self.input_element_mapper = input_element_mapper
        self.setup_action = setup_action
        self.start_bundle_action = start_bundle_action
        self.finish_bundle_action = finish_bundle_action
        self.teardown_action = teardown_action

        super().__init__()

    def setup(self):
        self.setup_action()

    def start_bundle(self):
        self.start_bundle_action()

    def finish_bundle(self):
        self.finish_bundle_action()

    def teardown(self):
        self.teardown_action()

    def process(self, element, *args, **kwargs):
        try:
            results: Iterable = self.input_element_mapper(element, *args, **kwargs)

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
    """
    Custom DnFn class representing a map operation with error handling.
    """

    def __init__(self,
                 step: str,
                 input_element_mapper: Callable[[Any], Any],
                 setup_action: Callable[[], None] = lambda: None,
                 start_bundle_action: Callable[[], None] = lambda: None,
                 finish_bundle_action: Callable[[], None] = lambda: None,
                 teardown_action: Callable[[], None] = lambda: None):
        self.step = step
        self.input_element_mapper = input_element_mapper
        self.setup_action = setup_action
        self.start_bundle_action = start_bundle_action
        self.finish_bundle_action = finish_bundle_action
        self.teardown_action = teardown_action

        super().__init__()

    def setup(self):
        self.setup_action()

    def start_bundle(self):
        self.start_bundle_action()

    def finish_bundle(self):
        self.finish_bundle_action()

    def teardown(self):
        self.teardown_action()

    def process(self, element, *args, **kwargs):
        try:
            yield self.input_element_mapper(element, *args, **kwargs)
        except Exception as err:
            failure = Failure(
                pipeline_step=self.step,
                input_element=get_input_element(element),
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class Filter(beam.DoFn):
    """
    Custom DnFn class representing a filter operation with error handling.
    """

    def __init__(self,
                 step: str,
                 input_element_predicate: Callable[[Any], bool]):
        self.step = step
        self.input_element_predicate = input_element_predicate

        super().__init__()

    def process(self, element, *args, **kwargs):
        try:
            if self.input_element_predicate(element, *args, **kwargs):
                yield element
        except Exception as err:
            failure = Failure(
                pipeline_step=self.step,
                input_element=get_input_element(element),
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)
