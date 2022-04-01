import apache_beam as beam
from apache_beam import ParDo, PCollection

from asgarde.transforms.do_fns_error_handling import FlatMap, FAILURES, Map, Filter
from asgarde.failure import Failure


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
            failures=inputs.pipeline | f'init_failures {inputs.__repr__().__str__()}' >> beam.Create([])
        )

    def map(self, name, fn, *args, **kwargs):
        current_outputs, current_failures = (self.outputs |
                                             name >> ParDo(Map(name, fn), *args, **kwargs)
                                             .with_outputs(FAILURES, main='outputs'))

        concatenated_failures = CollectionComposer.concat_failures(
            name=f'{name} Concat Failure PCollections',
            failures_tuple=(self.failures, current_failures)
        )

        return CollectionComposer(current_outputs, concatenated_failures)

    def flat_map(self, name, fn, *args, **kwargs):
        current_outputs, current_failures = (self.outputs |
                                             name >> ParDo(FlatMap(name, fn), *args, **kwargs)
                                             .with_outputs(FAILURES, main='outputs'))

        concatenated_failures = CollectionComposer.concat_failures(
            name=f'{name} Concat Failure PCollections',
            failures_tuple=(self.failures, current_failures)
        )

        return CollectionComposer(current_outputs, concatenated_failures)

    def filter(self, name, fn, *args, **kwargs):
        current_outputs, current_failures = (self.outputs |
                                             name >> ParDo(Filter(name, fn), *args, **kwargs)
                                             .with_outputs(FAILURES, main='outputs'))

        concatenated_failures = CollectionComposer.concat_failures(
            name=f'{name} Concat Failure PCollections',
            failures_tuple=(self.failures, current_failures)
        )

        return CollectionComposer(current_outputs, concatenated_failures)

    @staticmethod
    def concat_failures(name: str, failures_tuple):
        return failures_tuple | name >> beam.Flatten()
