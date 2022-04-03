from typing import Callable, Any

import apache_beam as beam
from apache_beam import ParDo, PCollection

from asgarde.failure import Failure
from asgarde.transforms.do_fns_error_handling import FlatMap, FAILURES, Map, Filter


class CollectionComposer:
    """
    This class take an input PCollection and propose methods to applies function like map, flatMap and filter,
    with error handling.

    For each step the failure is caught and concatenated with previous failures present in the composer class,
    and the new output is computed.

    Finally the composer class returns the last output and all failures, via Beam side outputs.
    """

    def __init__(self, inputs: PCollection, failures: PCollection[Failure]) -> None:
        self.outputs = inputs
        self.failures = failures

    @staticmethod
    def of(inputs: PCollection):
        return CollectionComposer(
            inputs=inputs,
            failures=inputs.pipeline | f'init_failures {str(repr(inputs))}' >> beam.Create([])
        )

    def map(self,
            name: str,
            input_element_mapper: Callable[[Any], Any],
            setup_action: Callable[[], None] = lambda: None,
            start_bundle_action: Callable[[], None] = lambda: None,
            finish_bundle_action: Callable[[], None] = lambda: None,
            teardown_action: Callable[[], None] = lambda: None,
            *args,
            **kwargs):
        current_outputs, current_failures = (self.outputs |
                                             name >>
                                             ParDo(Map(step=name,
                                                       input_element_mapper=input_element_mapper,
                                                       setup_action=setup_action,
                                                       start_bundle_action=start_bundle_action,
                                                       finish_bundle_action=finish_bundle_action,
                                                       teardown_action=teardown_action), *args, **kwargs)
                                             .with_outputs(FAILURES, main='outputs'))

        concatenated_failures = CollectionComposer.concat_failures(
            name=f'{name} Concat Failure PCollections',
            failures_tuple=(self.failures, current_failures)
        )

        return CollectionComposer(current_outputs, concatenated_failures)

    def flat_map(self,
                 name,
                 input_element_mapper: Callable[[Any], Any],
                 setup_action: Callable[[], None] = lambda: None,
                 start_bundle_action: Callable[[], None] = lambda: None,
                 finish_bundle_action: Callable[[], None] = lambda: None,
                 teardown_action: Callable[[], None] = lambda: None,
                 *args,
                 **kwargs):
        current_outputs, current_failures = (self.outputs |
                                             name >>
                                             ParDo(FlatMap(
                                                 step=name,
                                                 input_element_mapper=input_element_mapper,
                                                 setup_action=setup_action,
                                                 start_bundle_action=start_bundle_action,
                                                 finish_bundle_action=finish_bundle_action,
                                                 teardown_action=teardown_action), *args, **kwargs)
                                             .with_outputs(FAILURES, main='outputs'))

        concatenated_failures = CollectionComposer.concat_failures(
            name=f'{name} Concat Failure PCollections',
            failures_tuple=(self.failures, current_failures)
        )

        return CollectionComposer(current_outputs, concatenated_failures)

    def filter(self,
               name,
               input_element_predicate,
               *args,
               **kwargs):
        current_outputs, current_failures = (self.outputs |
                                             name >> ParDo(Filter(name, input_element_predicate), *args, **kwargs)
                                             .with_outputs(FAILURES, main='outputs'))

        concatenated_failures = CollectionComposer.concat_failures(
            name=f'{name} Concat Failure PCollections',
            failures_tuple=(self.failures, current_failures)
        )

        return CollectionComposer(current_outputs, concatenated_failures)

    @staticmethod
    def concat_failures(name: str, failures_tuple):
        return failures_tuple | name >> beam.Flatten()
