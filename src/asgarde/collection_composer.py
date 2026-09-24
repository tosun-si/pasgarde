from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import apache_beam as beam
from apache_beam import ParDo, PCollection

from asgarde.failure import Failure
from asgarde.transforms.do_fns_error_handling import FAILURES, ErrorHandlingDoFn, Filter, FlatMap, Map, no_action

FAILURES_STEP_NAME = 'Get all failures'


class CollectionComposer:
    """
    This class takes an input PCollection and proposes methods to apply functions like map, flatMap and filter,
    with error handling.

    For each step the failures are caught and kept with the failures of the previous steps,
    and the new output is computed.

    Finally the composer class returns the last output (`outputs`) and all the failures (`failures`).
    """

    def __init__(self,
                 inputs: PCollection,
                 step_failures: Sequence[PCollection[Failure]] = (),
                 last_step: str | None = None) -> None:
        self.outputs = inputs
        self._step_failures = tuple(step_failures)
        self._last_step = last_step
        self._failures: PCollection[Failure] | None = None

    @staticmethod
    def of(inputs: PCollection) -> CollectionComposer:
        return CollectionComposer(inputs)

    @property
    def failures(self) -> PCollection[Failure]:
        """
        All the failures of the flow, flattened once in a transform with a deterministic label
        (Dataflow streaming updates need stable labels, Beam Python rejects duplicate labels).
        """
        if self._failures is None:
            self._failures = self._flatten_failures()

        return self._failures

    def map(self,
            name: str,
            input_element_mapper: Callable[..., Any],
            *args,
            setup_action: Callable[[], None] = no_action,
            start_bundle_action: Callable[[], None] = no_action,
            finish_bundle_action: Callable[[], None] = no_action,
            teardown_action: Callable[[], None] = no_action,
            **kwargs) -> CollectionComposer:
        """
        Map operation with error handling. `args` and `kwargs` are passed to the ParDo (e.g. side inputs).
        """
        return self._apply(name, Map(
            step=name,
            input_element_mapper=input_element_mapper,
            setup_action=setup_action,
            start_bundle_action=start_bundle_action,
            finish_bundle_action=finish_bundle_action,
            teardown_action=teardown_action
        ), *args, **kwargs)

    def flat_map(self,
                 name: str,
                 input_element_mapper: Callable[..., Any],
                 *args,
                 setup_action: Callable[[], None] = no_action,
                 start_bundle_action: Callable[[], None] = no_action,
                 finish_bundle_action: Callable[[], None] = no_action,
                 teardown_action: Callable[[], None] = no_action,
                 **kwargs) -> CollectionComposer:
        """
        FlatMap operation with error handling. `args` and `kwargs` are passed to the ParDo (e.g. side inputs).
        """
        return self._apply(name, FlatMap(
            step=name,
            input_element_mapper=input_element_mapper,
            setup_action=setup_action,
            start_bundle_action=start_bundle_action,
            finish_bundle_action=finish_bundle_action,
            teardown_action=teardown_action
        ), *args, **kwargs)

    def filter(self,
               name: str,
               input_element_predicate: Callable[..., bool],
               *args,
               **kwargs) -> CollectionComposer:
        """
        Filter operation with error handling. `args` and `kwargs` are passed to the ParDo (e.g. side inputs).
        """
        return self._apply(name, Filter(step=name, input_element_predicate=input_element_predicate), *args, **kwargs)

    def _apply(self, name: str, do_fn: ErrorHandlingDoFn, *args, **kwargs) -> CollectionComposer:
        current_outputs, current_failures = (self.outputs
                                             | name >> ParDo(do_fn, *args, **kwargs)
                                             .with_outputs(FAILURES, main='outputs'))

        return CollectionComposer(current_outputs, (*self._step_failures, current_failures), name)

    def _flatten_failures(self) -> PCollection[Failure]:
        if not self._step_failures:
            label = f'{FAILURES_STEP_NAME} of {self.outputs.producer.full_label}'
            return self.outputs.pipeline | label >> beam.Create([])

        return self._step_failures | f'{FAILURES_STEP_NAME} of {self._last_step}' >> beam.Flatten()
