"""
Tests of the origin element: the failures give the element that entered the flow.
"""
import pickle

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.pvalue import AsDict
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty

from asgarde import CollectionComposer, Failure

ORIGIN_CONVERSIONS_NAMESPACE = 'test-origin'
ORIGIN_CONVERSIONS = 'conversions'


def validate(word: str) -> bool:
    if word.lower() == 'bad':
        raise ValueError(f'Bad word {word}')

    return True


def fail_if_prefixed(word: str) -> str:
    if word.startswith('x'):
        raise ValueError(f'Prefixed word {word}')

    return word


def counted_origin_conversion(origin: str) -> str:
    Metrics.counter(ORIGIN_CONVERSIONS_NAMESPACE, ORIGIN_CONVERSIONS).inc()
    return origin


def failing_origin_conversion(origin: str) -> str:
    raise RuntimeError('Origin conversion error')


def to_step_input_and_origin(failure: Failure) -> str:
    return f'{failure.pipeline_step}|{failure.input_element}|{failure.origin_element}'


def test_given_failure_in_third_step_when_track_origin_element_then_failure_with_the_element_that_entered_the_flow():
    with TestPipeline() as p:
        # GIVEN
        messages = p | 'Create messages' >> beam.Create(['psg,ol', 'real,bad'])

        # WHEN
        result = (CollectionComposer.of(messages)
                  .with_origin_element(lambda message: f'message: {message}')
                  .map('Parse', lambda message: message.upper())
                  .flat_map('To words', lambda line: line.split(','))
                  .filter('Validate', validate))

        # THEN
        assert_that(result.outputs, equal_to(['PSG', 'OL', 'REAL']), label='CheckOutputs')
        assert_that(result.failures | beam.Map(to_step_input_and_origin),
                    equal_to(['Validate|BAD|message: real,bad']), label='CheckFailures')


def test_given_good_and_bad_elements_when_track_origin_element_then_origin_converted_only_for_the_failures():
    # GIVEN
    p = TestPipeline()
    words = p | 'Create words' >> beam.Create(['a', 'b', 'bad', 'c'])

    # WHEN
    (CollectionComposer.of(words)
     .with_origin_element(counted_origin_conversion)
     .map('Parse', lambda word: word)
     .filter('Validate', validate))

    result = p.run()
    result.wait_until_finish()

    # THEN
    counters = result.metrics().query(MetricsFilter().with_name(ORIGIN_CONVERSIONS))['counters']
    assert sum(counter.committed for counter in counters) == 1


def test_given_origin_conversion_failing_when_failure_occurs_then_failure_with_fallback_origin_element():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['bad'])

        # WHEN
        result = (CollectionComposer.of(words)
                  .with_origin_element(failing_origin_conversion)
                  .filter('Validate', validate))

        # THEN
        assert_that(result.failures | beam.Map(lambda f: f.origin_element.startswith('<conversion of the origin')),
                    equal_to([True]), label='CheckFallbackOrigin')


def test_given_steps_before_tracking_the_origin_when_failures_then_only_the_next_steps_give_the_origin():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['xa', 'bad'])

        # WHEN
        result = (CollectionComposer.of(words)
                  .map('Before origin', fail_if_prefixed)
                  .with_origin_element(lambda word: f'origin {word}')
                  .filter('Validate', validate))

        # THEN
        assert_that(result.failures | beam.Map(to_step_input_and_origin),
                    equal_to(['Before origin|xa|None', 'Validate|bad|origin bad']), label='CheckFailures')


def test_given_positional_side_input_when_track_origin_element_then_side_input_passed_with_the_value():
    with TestPipeline() as p:
        # GIVEN
        teams = p | 'Create teams' >> beam.Create(['PSG', 'OM'])
        countries = p | 'Create countries' >> beam.Create([('PSG', 'France')])

        # WHEN
        result = (CollectionComposer.of(teams)
                  .with_origin_element(lambda team: f'team {team}')
                  .map('Map with country', lambda team, team_countries: f'{team} {team_countries[team]}',
                       AsDict(countries)))

        # THEN
        assert_that(result.outputs, equal_to(['PSG France']), label='CheckOutputs')
        assert_that(result.failures | beam.Map(to_step_input_and_origin),
                    equal_to(['Map with country|OM|team OM']), label='CheckFailures')


def test_given_origin_composer_without_step_when_get_result_then_inputs_as_outputs_and_no_failure():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['a', 'b'])

        # WHEN
        result = CollectionComposer.of(words).with_origin_element(str)

        # THEN
        assert_that(result.outputs, equal_to(['a', 'b']), label='CheckOutputs')
        assert_that(result.failures, is_empty(), label='CheckFailures')


def test_given_origin_composer_when_get_outputs_and_failures_twice_then_same_collections_with_deterministic_labels():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['a'])
        composer = (CollectionComposer.of(words)
                    .with_origin_element(str)
                    .map('Parse', lambda word: word)
                    .filter('Validate', validate))

        # WHEN
        outputs_1, outputs_2 = composer.outputs, composer.outputs
        failures_1, failures_2 = composer.failures, composer.failures

        # THEN
        assert outputs_2 is outputs_1
        assert failures_2 is failures_1
        assert outputs_1.producer.full_label == 'Remove origin of Validate'
        assert failures_1.producer.full_label == 'Get all failures of Validate'
        assert_that(outputs_1, equal_to(['a']), label='CheckOutputs')


def test_given_failure_when_with_origin_element_then_copy_with_origin_and_same_fields():
    # GIVEN
    failure = Failure.from_exception('Step', 'element', ValueError('Error'))

    # WHEN
    result_failure = failure.with_origin_element('origin')

    # THEN
    assert result_failure is not failure
    assert failure.origin_element is None
    assert result_failure.origin_element == 'origin'
    assert (result_failure.pipeline_step, result_failure.input_element) == ('Step', 'element')


def test_given_failure_pickled_without_origin_field_when_unpickle_then_origin_element_is_none():
    """A failure pickled by a previous version (without origin_element), e.g. in flight during a streaming update."""
    # GIVEN
    failure = Failure.from_exception('Step', 'element', ValueError('Error'))
    del failure.__dict__['origin_element']

    # WHEN
    result_failure = pickle.loads(pickle.dumps(failure))

    # THEN
    assert result_failure.origin_element is None
    assert result_failure.pipeline_step == 'Step'
