from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import apache_beam as beam
from apache_beam import ParDo, PCollection

from asgarde.failure import Failure
from asgarde.transforms.do_fns_error_handling import (
    FAILURES,
    ErrorHandlingDoFn,
    Filter,
    FlatMap,
    Map,
    dead_letter_to_failure,
    no_action,
)

FAILURES_STEP_NAME = 'Get all failures'
REMOVE_ORIGIN_STEP_NAME = 'Remove origin'
KEEP_ORIGIN_STEP_SUFFIX = ' - keep origin'


def element_as_origin(element: Any) -> tuple[Any, Any]:
    return element, element


def value_of(origin_and_value: tuple[Any, Any]) -> Any:
    return origin_and_value[1]


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
                 last_step: str | None = None,
                 origin_to_string: Callable[[Any], Any] | None = None,
                 origin_tracked: bool = False) -> None:
        # With an origin function, the elements become (origin, value) pairs from the first step (origin_tracked).
        self._collection = inputs
        self._step_failures = tuple(step_failures)
        self._last_step = last_step
        self._origin_to_string = origin_to_string
        self._origin_tracked = origin_tracked
        self._outputs: PCollection | None = None
        self._failures: PCollection[Failure] | None = None

    @staticmethod
    def of(inputs: PCollection) -> CollectionComposer:
        return CollectionComposer(inputs)

    @property
    def outputs(self) -> PCollection:
        """
        The output of the last step, without the origin elements when they are tracked (removed once, in a transform
        with a deterministic label).
        """
        if self._outputs is None:
            self._outputs = (self._collection
                             | f'{REMOVE_ORIGIN_STEP_NAME} of {self._last_step}' >> beam.Map(value_of)
                             if self._origin_tracked else self._collection)

        return self._outputs

    def with_origin_element(self, origin_to_string: Callable[[Any], Any]) -> CollectionComposer:
        """
        Keeps the origin element of each element for the next steps: their failures give, with
        `Failure.origin_element`, the element that entered the flow (the current output of this composer).

        The given function converts the origin element to a string. It's evaluated **only when a failure occurs**:
        e.g. the full payload to replay the failure from the start, or an identifier (message id, business key).
        """
        return CollectionComposer(self.outputs, self._step_failures, self._last_step, origin_to_string)

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
            teardown_action=teardown_action,
            origin_to_string=self._origin_to_string
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
            teardown_action=teardown_action,
            origin_to_string=self._origin_to_string
        ), *args, **kwargs)

    def filter(self,
               name: str,
               input_element_predicate: Callable[..., bool],
               *args,
               **kwargs) -> CollectionComposer:
        """
        Filter operation with error handling. `args` and `kwargs` are passed to the ParDo (e.g. side inputs).
        """
        return self._apply(name, Filter(
            step=name,
            input_element_predicate=input_element_predicate,
            origin_to_string=self._origin_to_string
        ), *args, **kwargs)

    def apply(self, name: str, transform: beam.ParDo | beam.DoFn, *args, **kwargs) -> CollectionComposer:
        """
        Applies any Beam `ParDo` (`beam.Map`, `beam.FlatMap`, `beam.ParDo(MyDoFn())`...) or `DoFn` with the Beam native
        exception handling (`with_exception_handling`): its errors become `Failure` objects, gathered with the
        failures of the other steps. `args` and `kwargs` are passed to a `DoFn` (e.g. side inputs).

        Not available when the origin element is tracked: the transform would receive the `(origin, value)` pairs.
        """
        if self._origin_to_string is not None:
            raise ValueError(
                f'The step "{name}" can\'t be applied with apply() while the origin element is tracked '
                f'(with_origin_element): use map, flat_map or filter, or apply it before with_origin_element.'
            )

        par_do = transform if isinstance(transform, beam.ParDo) else beam.ParDo(transform, *args, **kwargs)
        current_outputs, dead_letters = self._collection | name >> par_do.with_exception_handling()
        current_failures = dead_letters | f'{name} - to failures' >> beam.Map(dead_letter_to_failure, name)

        return CollectionComposer(current_outputs, (*self._step_failures, current_failures), name)

    def _apply(self, name: str, do_fn: ErrorHandlingDoFn, *args, **kwargs) -> CollectionComposer:
        current_outputs, current_failures = (self._step_input(name)
                                             | name >> ParDo(do_fn, *args, **kwargs)
                                             .with_outputs(FAILURES, main='outputs'))

        return CollectionComposer(
            current_outputs,
            (*self._step_failures, current_failures),
            name,
            self._origin_to_string,
            origin_tracked=self._origin_to_string is not None
        )

    def _step_input(self, name: str) -> PCollection:
        """
        Before the first step tracking the origin, the elements are wrapped once in (origin, value) pairs, the origin
        being the element itself (the name of the first step gives a deterministic label).
        """
        if self._origin_to_string is None or self._origin_tracked:
            return self._collection

        return self._collection | f'{name}{KEEP_ORIGIN_STEP_SUFFIX}' >> beam.Map(element_as_origin)

    def _flatten_failures(self) -> PCollection[Failure]:
        if not self._step_failures:
            label = f'{FAILURES_STEP_NAME} of {self._collection.producer.full_label}'
            return self._collection.pipeline | label >> beam.Create([])

        return self._step_failures | f'{FAILURES_STEP_NAME} of {self._last_step}' >> beam.Flatten()
