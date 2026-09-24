"""
Tests of the 1.3.0 features: richer Failure, to_dict with the BigQuery schema, any Beam transform in the composer and
the bridge to the Beam native ErrorHandler.
"""
import json
import pickle
import threading
from datetime import datetime, timezone

import apache_beam as beam
import pytest
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.error_handling import ErrorHandler

from asgarde import FAILURE_BIGQUERY_SCHEMA, FAILURES_METRICS_NAMESPACE, CollectionComposer, Failure


class ExceptionWithLock(Exception):
    """Exception holding a non picklable attribute."""

    def __init__(self, message: str):
        super().__init__(message)
        self.lock = threading.Lock()


class ParseDoFn(beam.DoFn):
    """Custom DoFn applied with apply()."""

    def process(self, element, suffix):
        yield f'{int(element)}{suffix}'


def parse(value: str) -> int:
    return int(value)


def to_step_input_type_message(failure: Failure) -> str:
    return f'{failure.pipeline_step}|{failure.input_element}|{failure.exception_type}|{failure.exception_message}'


def raise_and_get(exception: Exception) -> Exception:
    try:
        raise exception
    except Exception as err:
        return err


@pytest.mark.parametrize(
    'exception, expected_type, expected_message',
    [
        # --- Case 1: built-in exception, no module in the type ---
        (ValueError('Bad value'), 'ValueError', 'Bad value'),
        # --- Case 2: non picklable exception, the original type of the SerializableException ---
        (ExceptionWithLock('Error with a lock'), 'tests.test_failure_enrichment.ExceptionWithLock', 'Error with a lock'),
        # --- Case 3: exception without message ---
        (RuntimeError(), 'RuntimeError', None),
    ],
)
def test_given_exception_when_create_failure_then_computed_exception_type_and_message(
        exception, expected_type, expected_message):
    # WHEN
    failure = Failure.from_exception('Step', 'element', exception)

    # THEN
    assert (failure.exception_type, failure.exception_message) == (expected_type, expected_message)


def test_given_failure_created_from_exception_when_get_timestamp_then_utc_creation_time():
    # GIVEN
    before = datetime.now(timezone.utc)

    # WHEN
    failure = Failure.from_exception('Step', 'element', ValueError('Error'))

    # THEN
    assert before <= failure.timestamp <= datetime.now(timezone.utc)
    assert failure.with_origin_element('origin').timestamp == failure.timestamp


def test_given_failure_pickled_without_timestamp_field_when_unpickle_then_timestamp_is_none():
    """A failure pickled by a previous version (without timestamp), e.g. in flight during a streaming update."""
    # GIVEN
    failure = Failure.from_exception('Step', 'element', ValueError('Error'))
    del failure.__dict__['timestamp']

    # WHEN
    result_failure = pickle.loads(pickle.dumps(failure))

    # THEN
    assert result_failure.timestamp is None
    assert result_failure.to_dict()['timestamp'] is None


def test_given_failure_with_origin_when_to_dict_then_json_serializable_dict_with_the_schema_fields():
    # GIVEN
    failure = Failure.from_exception('Parse', 'not a number', raise_and_get(ValueError('Bad number')))
    failure = failure.with_origin_element('origin')

    # WHEN
    result = failure.to_dict()

    # THEN
    assert list(result) == [field['name'] for field in FAILURE_BIGQUERY_SCHEMA['fields']]
    assert result['pipeline_step'] == 'Parse'
    assert result['input_element'] == 'not a number'
    assert result['origin_element'] == 'origin'
    assert result['exception_type'] == 'ValueError'
    assert result['exception_message'] == 'Bad number'
    assert result['stack_trace'].startswith('Traceback')
    assert result['timestamp'] == failure.timestamp.isoformat()
    assert json.loads(json.dumps(result)) == result


def test_given_failure_with_and_without_origin_when_to_bad_record_then_beam_dead_letter_format():
    # GIVEN
    failure = Failure.from_exception('Parse', 'input', raise_and_get(ValueError('Error')))

    # WHEN
    record_without_origin = failure.to_bad_record()
    record_with_origin = failure.with_origin_element('origin').to_bad_record()

    # THEN
    element, (exception_type, exception_repr, stack_trace_lines) = record_without_origin
    assert (element, exception_type, exception_repr) == ('input', ValueError, "ValueError('Error')")
    assert ''.join(stack_trace_lines) == failure.stack_trace
    assert record_with_origin[0] == 'origin'


def test_given_beam_map_and_custom_dofn_when_apply_then_their_errors_gathered_with_the_other_failures():
    with TestPipeline() as p:
        # GIVEN
        values = p | 'Create values' >> beam.Create(['1', 'x', ' 2 '])

        # WHEN
        result = (CollectionComposer.of(values)
                  .map('Strip', lambda value: value.strip())
                  .apply('Parse with Map', beam.Map(parse))
                  .map('To string', str)
                  .apply('Parse with DoFn', ParseDoFn(), suffix='!'))

        # THEN
        assert_that(result.outputs, equal_to(['1!', '2!']), label='CheckOutputs')
        assert_that(result.failures | beam.Map(to_step_input_type_message),
                    equal_to(["Parse with Map|x|ValueError|invalid literal for int() with base 10: 'x'"]),
                    label='CheckFailures')


def test_given_errors_in_applied_transform_when_run_pipeline_then_failure_counter_and_timestamp():
    # GIVEN
    p = TestPipeline()
    values = p | 'Create values' >> beam.Create(['1', 'x', 'y'])

    # WHEN
    result = CollectionComposer.of(values).apply('Parse', beam.Map(parse))
    assert_that(result.failures | beam.Map(lambda f: f.timestamp is not None and f.stack_trace.startswith('Traceback')),
                equal_to([True, True]), label='CheckTimestampAndStackTrace')

    pipeline_result = p.run()
    pipeline_result.wait_until_finish()

    # THEN
    counters = pipeline_result.metrics().query(MetricsFilter().with_namespace(FAILURES_METRICS_NAMESPACE))['counters']
    assert {counter.key.metric.name: counter.committed for counter in counters} == {'Parse': 2}


def test_given_origin_element_tracked_when_apply_then_clear_error():
    with TestPipeline() as p:
        # GIVEN
        values = p | 'Create values' >> beam.Create(['1'])
        composer = CollectionComposer.of(values).with_origin_element(str)

        # WHEN / THEN
        with pytest.raises(ValueError, match='while the origin element is tracked'):
            composer.apply('Parse', beam.Map(parse))


def test_given_asgarde_failures_and_beam_bad_records_when_added_to_the_beam_error_handler_then_single_dead_letter_queue():
    with TestPipeline() as p:
        # GIVEN
        values = p | 'Create values' >> beam.Create(['1', 'x', 'y'])
        beam_values = p | 'Create Beam values' >> beam.Create(['z'])

        # WHEN
        with ErrorHandler(beam.combiners.Count.Globally()) as error_handler:
            # Bad records of a Beam transform using the error handler.
            _ = beam_values | 'Beam parse' >> beam.Map(parse).with_exception_handling(error_handler=error_handler)

            # Failures of the Asgarde steps, added to the same error handler.
            result = CollectionComposer.of(values).map('Parse', parse)
            error_handler.add_error_pcollection(result.failures | 'To bad records' >> beam.Map(Failure.to_bad_record))

        # THEN
        assert_that(error_handler.output(), equal_to([3]), label='CheckDeadLetterQueue')
