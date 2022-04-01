import apache_beam as beam
from apache_beam import PCollection
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty
from toolz.curried import pipe, map

from asgarde.collection_composer import CollectionComposer
from asgarde.failure import Failure
from asgarde.tests.player import Player
from asgarde.tests.team import Team
from asgarde.tests.testing_helper import log_element


class TestCollectionComposer:

    def test_given_input_string_pcollection_when_map_with_three_good_and_two_bad_then_have_three_outputs_and_two_failures_in_result(
            self):
        with TestPipeline() as p:
            # Given.
            inputs = [
                'zizou',
                'CR7',
                'Benzema',
                'Neymar',
                'Mbappe'
            ]

            # When.
            input_pcollection = (
                    p
                    | beam.Create(inputs)
            )

            result = (CollectionComposer.of(input_pcollection)
                      .map('Map Error 1', self.to_element_with_simulation_error_1)
                      .map('Map Error 2', self.to_element_with_simulation_error_2)
                      .map('Map output 1', lambda element: f'{element}_suffix')
                      .map('Map output 2', lambda element: f'{element}_other'))

            result_outputs: PCollection[str] = result.outputs
            result_failures: PCollection[Failure] = result.failures

            expected_outputs = [
                'zizou_suffix_other',
                'CR7_suffix_other',
                'Benzema_suffix_other'
            ]

            expected_failures = [
                Failure(
                    pipeline_step='Map Error 1',
                    input_element='"Mbappe"',
                    exception=ValueError('Mbappe is an old name')
                ),
                Failure(
                    pipeline_step='Map Error 2',
                    input_element='"Neymar"',
                    exception=ValueError('Neymar is a bad name')
                )
            ]

            print(expected_failures)

            result_outputs | "Print outputs" >> beam.Map(log_element)
            result_failures | "Print Failures" >> beam.Map(log_element)

            # Then.
            assert_that(result_outputs, equal_to(expected_outputs), label='CheckOutput')
            # assert_that(result_failures_as_string, equal_to(expected_failures), label='CheckFailures')

    def test_given_input_teams_pcollection_when_flatmap_to_players_without_error_then_expected_players_in_result_and_empty_failures(
            self):
        with TestPipeline() as p:
            input_players_team1 = [
                Player(firstName='Karim', lastName='Benzema')
            ]

            input_players_team2 = [
                Player(firstName='Kylian', lastName='Mbappe'),
                Player(firstName='Cristiano', lastName='Ronaldo')
            ]

            input_players_team3 = [
                Player(firstName='Thiago', lastName='Silva'),
                Player(firstName='Carlos', lastName='Tevez'),
                Player(firstName='Thiago', lastName='Motta')
            ]

            # Given.
            input_teams = [
                Team(name='', score=2, players=input_players_team1),
                Team(name='', score=2, players=input_players_team2),
                Team(name='', score=2, players=input_players_team3)
            ]

            # When.
            input_pcollection = (
                    p
                    | beam.Create(input_teams)
            )

            result = (CollectionComposer.of(input_pcollection)
                      .flat_map('Map Error 1', lambda t: t.players))

            result_outputs: PCollection[Player] = result.outputs
            result_failures: PCollection[Failure] = result.failures

            result_outputs_as_dict = (result_outputs
                                      | "Output Dict" >> beam.Map(lambda p: str(p))
                                      | "Print outputs" >> beam.Map(log_element))

            result_failures | "Print Failures" >> beam.Map(log_element)

            # Then.
            expected_players = list(pipe(
                [*input_players_team1, *input_players_team2, *input_players_team3],
                map(lambda p: str(p)))
            )

            assert_that(result_outputs_as_dict, equal_to(expected_players), label='CheckOutput')
            assert_that(result_failures, is_empty(), label='CheckFailures')

    def test_given_input_teams_pcollection_when_flatmap_to_players_with_two_errors_then_expected_two_teams_as_failure_and_two_players_as_good_outputs(
            self):
        with TestPipeline() as p:
            input_players_team1 = [
                Player(firstName='Karim', lastName='Benzema')
            ]

            input_players_team2 = [
                Player(firstName='Kylian', lastName='Mbappe'),
                Player(firstName='Cristiano', lastName='Ronaldo')
            ]

            input_players_team3 = [
                Player(firstName='Thiago', lastName='Silva'),
                Player(firstName='Carlos', lastName='Tevez'),
                Player(firstName='Thiago', lastName='Motta')
            ]

            # Given.
            input_teams = [
                Team(name='Real', score=2, players=input_players_team1),
                Team(name='PSG', score=2, players=input_players_team2),
                Team(name='OM', score=2, players=input_players_team3)
            ]

            # When.
            input_pcollection = (
                    p
                    | beam.Create(input_teams)
            )

            result = (CollectionComposer.of(input_pcollection)
                      .flat_map('To players with error', self.to_players_with_simulation_error_on_team))

            result_outputs: PCollection[Player] = result.outputs
            result_failures: PCollection[Failure] = result.failures

            result_outputs_as_strings = (result_outputs
                                         | "Output Dict" >> beam.Map(lambda p: str(p))
                                         | "Print outputs" >> beam.Map(log_element))

            result_failures | "Print Failures" >> beam.Map(log_element)

            # Then.
            expected_players = list(pipe(
                input_players_team2,
                map(lambda p: str(p)))
            )

            expected_failures = [
                Failure(
                    'To players with error',
                    str(input_teams[0]),
                    ValueError(f'{input_teams[0].name} is bad')
                ),
                Failure(
                    'To players with error',
                    str(input_teams[2]),
                    ValueError(f'{input_teams[2].name} is bad')
                )
            ]

            assert_that(result_outputs_as_strings, equal_to(expected_players), label='CheckOutput')
            assert_that(result_failures, equal_to(
                expected=expected_failures,
                equals_fn=self.check_result_matches_expected), label='CheckResMatchExpected')

    def to_element_with_simulation_error_1(self, elem):
        if elem == 'Neymar':
            raise ValueError('Neymar is a bad name')

        return elem

    def to_element_with_simulation_error_2(self, elem):
        if elem == 'Mbappe':
            raise ValueError('Mbappe is an old name')

        return elem

    def to_players_with_simulation_error_on_team(self, team: Team):
        if team.name == 'Real' or team.name == 'OM':
            raise ValueError(f'{team.name} is bad')

        return team.players

    def check_result_matches_expected(self, expected_failure: Failure, result_failure: Failure):
        result_input_element: str = result_failure.input_element
        expected_input_element: str = expected_failure.input_element

        result_exception: Exception = result_failure.exception
        expected_exception: Exception = expected_failure.exception

        result_input_equals_expected: bool = result_input_element == expected_input_element
        result_exception_equals_expected: bool = result_exception.args == expected_exception.args
        expected_exception_is_value_error: bool = type(expected_exception) == ValueError

        return (result_input_equals_expected
                and result_exception_equals_expected
                and expected_exception_is_value_error)
