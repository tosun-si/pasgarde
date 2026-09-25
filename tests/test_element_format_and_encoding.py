"""
Tests of the 1.4.0 features: the input element format (with_input_element_to_string) and the encoded elements
(with_encoded_elements).
"""
import base64
import pickle
import typing
from dataclasses import dataclass

import apache_beam as beam
import pytest
from apache_beam import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from asgarde import (
    FAILURE_BIGQUERY_SCHEMA,
    FAILURE_BIGQUERY_SCHEMA_WITH_ENCODED_ELEMENTS,
    CollectionComposer,
    Failure,
)


class Team(typing.NamedTuple):
    name: str
    score: int


@dataclass
class Player:
    name: str


class FailingCoder(coders.Coder):
    """Coder failing to encode."""

    def encode(self, value):
        raise ValueError('Encoding error')

    def decode(self, encoded):
        raise NotImplementedError


def fail_if_bad(word: str) -> str:
    if word == 'bad':
        raise ValueError('Bad word')

    return word


def raise_error(element):
    raise RuntimeError('Format error')


@pytest.mark.parametrize(
    'step',
    [
        # --- Case 1: map ---
        lambda composer: composer.map('Step', fail_if_bad),
        # --- Case 2: flat_map ---
        lambda composer: composer.flat_map('Step', lambda word: [fail_if_bad(word)]),
        # --- Case 3: filter ---
        lambda composer: composer.filter('Step', lambda word: fail_if_bad(word) != ''),
        # --- Case 4: apply with a Beam Map ---
        lambda composer: composer.apply('Step', beam.Map(fail_if_bad)),
        # --- Case 5: map with the origin tracked ---
        lambda composer: composer.with_origin_element(str).map('Step', fail_if_bad),
    ],
)
def test_given_input_element_to_string_when_failure_in_any_kind_of_step_then_input_element_with_the_format(step):
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['bad'])
        composer = CollectionComposer.of(words).with_input_element_to_string(lambda element: f'formatted {element}')

        # WHEN
        result = step(composer)

        # THEN
        assert_that(result.failures | beam.Map(lambda f: f.input_element), equal_to(['formatted bad']))


@pytest.mark.parametrize(
    'element_to_string',
    [
        # --- Case 1: function raising ---
        raise_error,
        # --- Case 2: function returning None ---
        lambda element: None,
    ],
)
def test_given_input_element_to_string_raising_or_returning_none_when_failure_then_default_conversion(element_to_string):
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['bad'])

        # WHEN
        result = (CollectionComposer.of(words)
                  .with_input_element_to_string(element_to_string)
                  .map('Step', fail_if_bad))

        # THEN
        assert_that(result.failures | beam.Map(lambda f: f.input_element), equal_to(['bad']))


def test_given_no_option_when_failure_then_default_conversion_and_no_encoded_element():
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['bad'])

        # WHEN
        result = CollectionComposer.of(words).map('Step', fail_if_bad)

        # THEN
        assert_that(result.failures | beam.Map(lambda f: (f.input_element, f.input_element_bytes, f.input_element_coder)),
                    equal_to([('bad', None, None)]))


@pytest.mark.parametrize(
    'step',
    [
        # --- Case 1: map ---
        lambda composer: composer.map('Step', fail_if_bad),
        # --- Case 2: filter ---
        lambda composer: composer.filter('Step', lambda word: fail_if_bad(word) != ''),
        # --- Case 3: apply with a Beam Map ---
        lambda composer: composer.apply('Step', beam.Map(fail_if_bad)),
    ],
)
def test_given_encoded_elements_when_failure_in_any_kind_of_step_then_input_element_decoded_back_with_its_coder(step):
    with TestPipeline() as p:
        # GIVEN
        words = p | 'Create words' >> beam.Create(['bad'])

        # WHEN
        result = step(CollectionComposer.of(words).with_encoded_elements())

        # THEN
        assert_that(result.failures | beam.Map(lambda f: coders.StrUtf8Coder().decode(f.input_element_bytes)),
                    equal_to(['bad']), label='CheckDecoded')
        assert_that(result.failures | beam.Map(lambda f: f.input_element_coder), equal_to(['StrUtf8Coder']),
                    label='CheckCoder')


def test_given_encoded_elements_of_named_tuples_when_failure_then_element_decoded_back_with_the_schema_coder():
    with TestPipeline() as p:
        # GIVEN
        teams = p | 'Create teams' >> beam.Create([Team('PSG', 0)]).with_output_types(Team)
        team_coder = coders.registry.get_coder(Team)

        # WHEN
        result = (CollectionComposer.of(teams)
                  .with_encoded_elements()
                  .map('Step', lambda team: 1 / team.score))

        # THEN
        assert_that(result.failures | beam.Map(lambda f: team_coder.decode(f.input_element_bytes)),
                    equal_to([Team('PSG', 0)]))


def test_given_encoded_elements_of_untyped_objects_when_failure_then_element_decoded_back_with_the_generic_coder():
    with TestPipeline() as p:
        # GIVEN: a PCollection without type hints, the generic coder is used.
        players = p | 'Create players' >> beam.Create([Player('Mbappe')])
        generic_coder = coders.registry.get_coder(typing.Any)

        # WHEN
        result = (CollectionComposer.of(players)
                  .map('To name', lambda player: player)
                  .with_encoded_elements()
                  .map('Step', lambda player: fail_if_bad('bad')))

        # THEN
        assert_that(result.failures | beam.Map(lambda f: generic_coder.decode(f.input_element_bytes).name),
                    equal_to(['Mbappe']))


def test_given_encoded_elements_and_origin_when_failure_then_value_and_origin_decoded_back_with_their_coders():
    with TestPipeline() as p:
        # GIVEN
        messages = p | 'Create messages' >> beam.Create(['psg,bad'])

        # WHEN
        result = (CollectionComposer.of(messages)
                  .with_origin_element(lambda message: f'origin {message}')
                  .with_encoded_elements()
                  .flat_map('To words', lambda line: line.split(','))
                  .filter('Validate', lambda word: fail_if_bad(word) != ''))

        # THEN
        generic_coder = coders.registry.get_coder(typing.Any)
        assert_that(result.failures | beam.Map(lambda f: (
            generic_coder.decode(f.input_element_bytes),
            coders.StrUtf8Coder().decode(f.origin_element_bytes),
            f.origin_element)),
                    equal_to([('bad', 'psg,bad', 'origin psg,bad')]))


def test_given_coder_failing_to_encode_when_encode_input_element_then_failure_unchanged_without_bytes():
    # GIVEN
    failure = Failure.from_exception('Step', 'element', ValueError('Error'))

    # WHEN
    result_failure = failure.with_encoded_input_element('element', FailingCoder())

    # THEN
    assert result_failure is failure
    assert (result_failure.input_element_bytes, result_failure.input_element_coder) == (None, None)


def test_given_failure_pickled_without_the_encoded_fields_when_unpickle_then_encoded_fields_are_none():
    """A failure pickled by a previous version (without the encoded fields), e.g. in flight during a streaming update."""
    # GIVEN
    failure = Failure.from_exception('Step', 'element', ValueError('Error'))
    for field in ['input_element_bytes', 'input_element_coder', 'origin_element_bytes', 'origin_element_coder']:
        del failure.__dict__[field]

    # WHEN
    result_failure = pickle.loads(pickle.dumps(failure))

    # THEN
    assert (result_failure.input_element_bytes, result_failure.origin_element_bytes) == (None, None)
    assert result_failure.to_dict_with_encoded_elements()['input_element_bytes'] is None


def test_given_failure_with_encoded_elements_when_to_dict_with_encoded_elements_then_base64_bytes_and_extended_schema():
    # GIVEN
    failure = (Failure.from_exception('Step', 'input', ValueError('Error'))
               .with_origin_element('origin')
               .with_encoded_input_element('input', coders.StrUtf8Coder())
               .with_encoded_origin_element('origin', coders.StrUtf8Coder()))

    # WHEN
    result = failure.to_dict_with_encoded_elements()

    # THEN
    assert list(result) == [field['name'] for field in FAILURE_BIGQUERY_SCHEMA_WITH_ENCODED_ELEMENTS['fields']]
    assert base64.b64decode(result['input_element_bytes']) == failure.input_element_bytes
    assert base64.b64decode(result['origin_element_bytes']) == failure.origin_element_bytes
    assert (result['input_element_coder'], result['origin_element_coder']) == ('StrUtf8Coder', 'StrUtf8Coder')


def test_given_failure_when_to_dict_then_schema_unchanged():
    # WHEN
    result = Failure.from_exception('Step', 'input', ValueError('Error')).to_dict()

    # THEN: the 1.3.0 format is unchanged, the tables created with it keep working.
    assert list(result) == [field['name'] for field in FAILURE_BIGQUERY_SCHEMA['fields']]
    assert [field['name'] for field in FAILURE_BIGQUERY_SCHEMA['fields']] == [
        'pipeline_step', 'input_element', 'origin_element', 'exception_type', 'exception_message', 'stack_trace',
        'timestamp']
