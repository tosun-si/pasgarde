"""
Tests of the failure handling edge cases: the error handling itself must never make the pipeline fail.
"""
import threading
from datetime import datetime

import apache_beam as beam
import pytest
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.pvalue import AsDict
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty

from asgarde import FAILURES_METRICS_NAMESPACE, CollectionComposer, Failure, SerializableException


class ExceptionWithLock(Exception):
    """Exception holding a non picklable attribute, like an exception holding a client or a lock."""

    def __init__(self, message: str):
        super().__init__(message)
        self.lock = threading.Lock()


class ExceptionWithCustomInit(Exception):
    """Exception whose pickle round trip fails because of its custom __init__ signature."""

    def __init__(self, code: int, message: str):
        super().__init__(f'{code}: {message}')
        self.code = code


class FailingStr:
    def __str__(self):
        raise ValueError('str error')

    def __repr__(self):
        raise ValueError('repr error')


def fail_with(exception_factory):
    """The exception is built when processing the element: the mapper must stay picklable."""

    def mapper(element):
        raise exception_factory()

    return mapper


def fail_if_prefixed(word: str) -> str:
    if word.startswith('x'):
        raise ValueError(f'Prefixed word {word}')

    return word


def one_output_then_fail(word: str):
    yield word
    raise ValueError('Failing in the middle of the iterable')


def to_exception_repr(failure: Failure) -> str:
    return repr(failure.exception)


@pytest.mark.parametrize(
    'exception_factory, expected_exception_repr',
    [
        # --- Case 1: exception holding a non picklable attribute (lock) ---
        (lambda: ExceptionWithLock('Error with a lock'),
         "tests.test_failure_handling.ExceptionWithLock('Error with a lock')"),
        # --- Case 2: exception with a custom __init__ signature, its pickle round trip fails ---
        (lambda: ExceptionWithCustomInit(500, 'Server error'),
         "tests.test_failure_handling.ExceptionWithCustomInit('500: Server error')"),
    ],
)
def test_given_non_picklable_exception_when_map_then_failure_with_serializable_copy_of_exception(
        exception_factory, expected_exception_repr):
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['a'])

        # WHEN
        result = CollectionComposer.of(words).map('Map', fail_with(exception_factory))

        # THEN
        assert_that(result.outputs, is_empty(), label='CheckOutputs')
        assert_that(result.failures | beam.Map(to_exception_repr), equal_to([expected_exception_repr]),
                    label='CheckFailures')


def test_given_picklable_exception_when_create_failure_then_exception_kept_with_stack_trace():
    # GIVEN
    try:
        raise ValueError('Bad value')
    except ValueError as err:
        exception = err

    # WHEN
    failure = Failure.from_exception('Step', 'element', exception)

    # THEN
    assert failure.exception is exception
    assert failure.stack_trace.startswith('Traceback')
    assert 'ValueError: Bad value' in failure.stack_trace


def test_given_non_picklable_exception_when_create_failure_then_serializable_exception_with_original_type():
    # WHEN
    failure = Failure.from_exception('Step', 'element', ExceptionWithLock('Error with a lock'))

    # THEN
    assert isinstance(failure.exception, SerializableException)
    assert failure.exception.original_type == 'tests.test_failure_handling.ExceptionWithLock'
    assert str(failure.exception) == 'Error with a lock'


@pytest.mark.parametrize(
    'element, expected_input_element',
    [
        # --- Case 1: dict with non JSON types (datetime, bytes) ---
        ({'date': datetime(2026, 1, 2), 'raw': b'\x01'}, '{"date": "2026-01-02 00:00:00", "raw": "b\'\\\\x01\'"}'),
        # --- Case 2: dict with non string keys, not JSON serializable, repr fallback ---
        ({(1, 2): 'tuple key'}, "{(1, 2): 'tuple key'}"),
        # --- Case 3: None element ---
        (None, 'None'),
    ],
)
def test_given_element_not_json_friendly_when_create_failure_then_input_element_converted_without_error(
        element, expected_input_element):
    # WHEN
    failure = Failure.from_exception('Step', element, ValueError('Error'))

    # THEN
    assert failure.input_element == expected_input_element


def test_given_element_with_failing_str_and_repr_when_create_failure_then_fallback_input_element():
    # WHEN
    failure = Failure.from_exception('Step', FailingStr(), ValueError('Error'))

    # THEN
    assert failure.input_element.startswith('<conversion of FailingStr failed:')


def test_given_iterable_failing_in_the_middle_when_flat_map_then_only_failure_and_no_partial_output():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['a'])

        # WHEN
        result = CollectionComposer.of(words).flat_map('FlatMap', one_output_then_fail)

        # THEN
        assert_that(result.outputs, is_empty(), label='CheckOutputs')
        assert_that(result.failures | beam.Map(lambda f: f.pipeline_step), equal_to(['FlatMap']),
                    label='CheckFailures')


def test_given_positional_side_input_when_map_then_side_input_passed_to_the_mapper():
    with TestPipeline() as p:
        # GIVEN
        teams = p | 'Create teams' >> beam.Create(['PSG', 'Real'])
        countries = p | 'Create countries' >> beam.Create([('PSG', 'France'), ('Real', 'Spain')])

        # WHEN
        result = CollectionComposer.of(teams).map(
            'Map with country',
            lambda team, team_countries: f'{team} {team_countries[team]}',
            AsDict(countries)
        )

        # THEN
        assert_that(result.outputs, equal_to(['PSG France', 'Real Spain']), label='CheckOutputs')
        assert_that(result.failures, is_empty(), label='CheckFailures')


def test_given_composer_when_get_failures_twice_then_same_failures_with_deterministic_label():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['xa'])
        composer = CollectionComposer.of(words).map('Map', fail_if_prefixed)

        # WHEN
        failures_1 = composer.failures
        failures_2 = composer.failures

        # THEN
        assert failures_2 is failures_1
        assert failures_1.producer.full_label == 'Get all failures of Map'
        assert_that(failures_1 | beam.Map(lambda f: f.pipeline_step), equal_to(['Map']), label='CheckFailures')


def test_given_same_pcollection_when_create_two_composers_then_no_duplicate_label():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['a', 'xb'])

        # WHEN
        result_1 = CollectionComposer.of(words).map('Map 1', fail_if_prefixed)
        result_2 = CollectionComposer.of(words).map('Map 2', fail_if_prefixed)

        # THEN
        assert_that(result_1.failures | 'Steps 1' >> beam.Map(lambda f: f.pipeline_step), equal_to(['Map 1']),
                    label='CheckFailures1')
        assert_that(result_2.failures | 'Steps 2' >> beam.Map(lambda f: f.pipeline_step), equal_to(['Map 2']),
                    label='CheckFailures2')


def test_given_composer_without_step_when_get_result_then_inputs_as_outputs_and_no_failure():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['a', 'b'])

        # WHEN
        result = CollectionComposer.of(words)

        # THEN
        assert_that(result.outputs, equal_to(['a', 'b']), label='CheckOutputs')
        assert_that(result.failures, is_empty(), label='CheckFailures')


def test_given_memory_error_in_mapper_when_run_pipeline_then_error_raised_and_not_sent_to_failures():
    # GIVEN
    p = TestPipeline()
    words = p | 'Create words' >> beam.Create(['a'])

    # WHEN
    CollectionComposer.of(words).map('Map', fail_with(lambda: MemoryError('Simulated memory error')))

    # THEN
    with pytest.raises(Exception, match='Simulated memory error'):
        p.run().wait_until_finish()


def test_given_failing_steps_when_run_pipeline_then_failure_counter_incremented_by_step():
    # GIVEN
    p = TestPipeline()
    words = p | 'Create words' >> beam.Create(['a', 'xb', 'xc'])

    # WHEN
    (CollectionComposer.of(words)
     .map('Map', fail_if_prefixed)
     .filter('Filter', lambda word: fail_if_prefixed('x' + word) is not None))

    result = p.run()
    result.wait_until_finish()

    # THEN
    counters = result.metrics().query(MetricsFilter().with_namespace(FAILURES_METRICS_NAMESPACE))['counters']
    failures_by_step = {counter.key.metric.name: counter.committed for counter in counters}

    assert failures_by_step == {'Map': 2, 'Filter': 1}
