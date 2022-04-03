from dataclasses import dataclass
from typing import Dict

import apache_beam as beam
import pytest
from apache_beam import PCollection, DoFn, pvalue, ParDo
from apache_beam.pvalue import AsDict
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty
from toolz.curried import pipe, map

from asgarde.collection_composer import CollectionComposer
from asgarde.failure import Failure
from asgarde.tests.player import Player
from asgarde.tests.team import Team
from asgarde.tests.testing_helper import log_element

FAILURES = 'failures'

SETUP_ACTION_MESSAGE = 'Setup action is executed'
START_BUNDLE_ACTION_MESSAGE = 'Start bundle action is executed'
FINISH_BUNDLE_ACTION_MESSAGE = 'Finish bundle action is executed'
TEARDOWN_ACTION_MESSAGE = 'Teardown action is executed'

team_countries = {
    'PSG': 'France',
    'OL': 'France',
    'Real': 'Spain',
    'ManU': 'England'
}

team_cities = {
    'PSG': 'Paris',
    'OL': 'France',
    'Real': 'Madrid',
    'ManU': 'Manchester'
}


@dataclass
class TeamInfo:
    name: str
    country: str
    city: str


class MapToTeamWithCountry(DoFn):

    def process(self, element, *args, **kwargs):
        try:
            team_name: str = element

            yield TeamInfo(
                name=team_name,
                country=team_countries[team_name],
                city=''
            )
        except Exception as err:
            failure = Failure(
                pipeline_step="Map 1",
                input_element=element,
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class MapToTeamWithCity(DoFn):

    def process(self, element, *args, **kwargs):
        try:
            team_info: TeamInfo = element
            city: str = team_cities[team_info.name]

            yield TeamInfo(
                name=team_info.name,
                country=team_info.country,
                city=city
            )
        except Exception as err:
            failure = Failure(
                pipeline_step="Map 2",
                input_element=element,
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class FilterFranceTeams(DoFn):

    def process(self, element, *args, **kwargs):
        try:
            team_info: TeamInfo = element

            if team_info.country == 'France':
                yield element
        except Exception as err:
            failure = Failure(
                pipeline_step="Filter France teams",
                input_element=element,
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


@pytest.fixture
def run_after_tests_setup_action(capsys):
    yield

    assert capsys.readouterr().out == f"{SETUP_ACTION_MESSAGE}\n"


@pytest.fixture
def run_after_tests_start_bundle_action(capsys):
    yield

    assert capsys.readouterr().out == f"{START_BUNDLE_ACTION_MESSAGE}\n"


@pytest.fixture
def run_after_tests_finish_bundle_action(capsys):
    yield

    assert capsys.readouterr().out == f"{FINISH_BUNDLE_ACTION_MESSAGE}\n"


@pytest.fixture
def run_after_tests_teardown_action(capsys):
    yield

    assert capsys.readouterr().out == f"{TEARDOWN_ACTION_MESSAGE}\n"


@pytest.fixture
def run_after_tests_all_actions(capsys):
    yield

    capture_out = capsys.readouterr().out
    assert SETUP_ACTION_MESSAGE in capture_out
    assert START_BUNDLE_ACTION_MESSAGE in capture_out
    assert FINISH_BUNDLE_ACTION_MESSAGE in capture_out
    assert TEARDOWN_ACTION_MESSAGE in capture_out


@pytest.fixture
def run_after_tests_no_action(capsys):
    yield

    assert capsys.readouterr().out == ''


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
                    input_element='Mbappe',
                    exception=ValueError('Mbappe is an old name')
                ),
                Failure(
                    pipeline_step='Map Error 2',
                    input_element='Neymar',
                    exception=ValueError('Neymar is a bad name')
                )
            ]

            print(expected_failures)

            result_outputs | "Print outputs" >> beam.Map(log_element)
            result_failures | "Print Failures" >> beam.Map(log_element)

            # Then.
            assert_that(result_outputs, equal_to(expected_outputs), label='CheckOutput')
            assert_that(result_failures, equal_to(
                expected=expected_failures,
                equals_fn=self._check_result_matches_expected), label='CheckResMatchExpected')

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
                equals_fn=self._check_result_matches_expected), label='CheckResMatchExpected')

    def test_given_input_team_names_when_filter_simulating_error_on_psg_team_name_then_expected_three_output_and_one_failure_on_psg_in_result(
            self):
        with TestPipeline() as p:
            # Given.
            teams = [
                'PSG',
                'OL',
                'Real',
                'ManU'
            ]

            input_teams: PCollection[str] = p | 'Read' >> beam.Create(teams)

            # When.
            result = (CollectionComposer.of(input_teams)
                      .filter('Filter with error', self.filter_element_with_simulation_error))

            result_outputs: PCollection[str] = result.outputs
            result_failures: PCollection[Failure] = result.failures

            result_outputs | "Print outputs" >> beam.Map(log_element)
            result_failures | "Print Failures" >> beam.Map(log_element)

            expected_team_names = [
                'OL',
                'Real',
                'ManU'
            ]

            expected_failures = [
                Failure(
                    'Filter with error',
                    'PSG',
                    ValueError('PSG filter error')
                )
            ]

            # Then.
            assert_that(result_outputs, equal_to(expected_team_names), label='CheckOutput')
            assert_that(result_failures, equal_to(
                expected=expected_failures,
                equals_fn=self._check_result_matches_expected), label='CheckResMatchExpected')

    def test_given_input_team_names_when_map_to_teams_info_with_side_input_and_without_error_then_expected_outputs_and_no_failures_in_result(
            self):
        with TestPipeline() as p:
            # Given.
            teams = [
                'PSG',
                'OL',
                'Real',
                'ManU'
            ]

            input_teams: PCollection[str] = p | 'Read' >> beam.Create(teams)
            countries_side_inputs = p | 'Countries' >> beam.Create(team_countries)

            # When.
            result = (CollectionComposer.of(input_teams)
                      .map('Map with country', self.to_team_with_city, team_countries=AsDict(countries_side_inputs))
                      .map('Map with city',
                           lambda ti: TeamInfo(name=ti.name, country=ti.country, city=team_cities[ti.name]))
                      .filter('Filter french team', lambda ti: ti.country == 'France'))

            result_outputs: PCollection[str] = result.outputs
            result_failures: PCollection[Failure] = result.failures

            result_outputs | "Print outputs" >> beam.Map(log_element)
            result_failures | "Print Failures" >> beam.Map(log_element)

            expected_teams_info = [
                TeamInfo(name='PSG', country='France', city='Paris'),
                TeamInfo(name='OL', country='France', city='France')
            ]

            # Then.
            assert_that(result_outputs, equal_to(expected_teams_info), label='CheckOutput')
            assert_that(result_failures, is_empty(), label='CheckFailures')

    def test_given_input_team_names_when_map_to_teams_info_native_beam_error_handling_without_error_then_expected_outputs_and_no_failures_in_result(
            self):
        with TestPipeline() as p:
            # Given.
            teams = [
                'PSG',
                'OL',
                'Real',
                'ManU'
            ]

            input_teams: PCollection[str] = p | 'Read' >> beam.Create(teams)

            # When.
            outputs_map1, failures_map1 = (input_teams | 'Map to team with country' >> ParDo(MapToTeamWithCountry())
                                           .with_outputs(FAILURES, main='outputs'))

            outputs_map2, failures_map2 = (outputs_map1 | 'Map to team with city' >> ParDo(MapToTeamWithCity())
                                           .with_outputs(FAILURES, main='outputs'))

            result_outputs, failures_filter = (outputs_map2 | 'Filter France teams' >> ParDo(FilterFranceTeams())
                                               .with_outputs(FAILURES, main='outputs'))

            result_all_failures = (
                    (failures_map1, failures_map2, failures_filter)
                    | 'All Failures PCollections' >> beam.Flatten()
            )

            expected_teams_info = [
                TeamInfo(name='PSG', country='France', city='Paris'),
                TeamInfo(name='OL', country='France', city='France')
            ]

            # Then.
            assert_that(result_outputs, equal_to(expected_teams_info), label='CheckOutput')
            assert_that(result_all_failures, is_empty(), label='CheckFailures')

    def test_when_call_dofn_lifecycle_setup_action_with_error_handling_then_expected_action_is_executed(
            self,
            run_after_tests_setup_action):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p, SETUP_ACTION_MESSAGE)

    def test_when_call_dofn_lifecycle_start_bundle_action_with_error_handling_then_expected_action_is_executed(
            self,
            run_after_tests_start_bundle_action):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p, START_BUNDLE_ACTION_MESSAGE)

    def test_when_call_dofn_lifecycle_finish_bundle_action_with_error_handling_then_expected_action_is_executed(
            self,
            run_after_tests_finish_bundle_action):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p, FINISH_BUNDLE_ACTION_MESSAGE)

    def test_when_call_dofn_lifecycle_teardown_action_with_error_handling_then_expected_action_is_executed(
            self,
            run_after_tests_teardown_action):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p, TEARDOWN_ACTION_MESSAGE)

    def test_when_call_dofn_lifecycle_all_actions_with_error_handling_then_expected_all_actions_are_executed(
            self,
            run_after_tests_all_actions):
        with TestPipeline() as p:
            # Given.
            teams = [
                'PSG',
                'OL',
                'Real',
                'ManU'
            ]

            input_teams: PCollection[str] = p | 'Read' >> beam.Create(teams)
            countries_side_inputs = p | 'Countries' >> beam.Create(team_countries)

            # When.
            (CollectionComposer.of(input_teams)
             .map('Map with country',
                  input_element_mapper=self.to_team_with_city,
                  setup_action=lambda: print(SETUP_ACTION_MESSAGE),
                  start_bundle_action=lambda: print(START_BUNDLE_ACTION_MESSAGE),
                  finish_bundle_action=lambda: print(FINISH_BUNDLE_ACTION_MESSAGE),
                  teardown_action=lambda: print(TEARDOWN_ACTION_MESSAGE),
                  team_countries=AsDict(countries_side_inputs))
             )

    def test_when_call_dofn_lifecycle_no_action_with_error_handling_then_no_action_executed(
            self,
            run_after_tests_no_action):
        with TestPipeline() as p:
            # Given.
            teams = [
                'PSG',
                'OL',
                'Real',
                'ManU'
            ]

            input_teams: PCollection[str] = p | 'Read' >> beam.Create(teams)
            countries_side_inputs = p | 'Countries' >> beam.Create(team_countries)

            # When.
            (CollectionComposer.of(input_teams)
             .map('Map with country',
                  input_element_mapper=self.to_team_with_city,
                  team_countries=AsDict(countries_side_inputs))
             )

    def execute_lifecyle_action_test(self, p, expected_action_message: str):
        # Given.
        teams = [
            'PSG',
            'OL',
            'Real',
            'ManU'
        ]

        input_teams: PCollection[str] = p | 'Read' >> beam.Create(teams)
        countries_side_inputs = p | 'Countries' >> beam.Create(team_countries)

        # When.
        (CollectionComposer.of(input_teams)
         .map('Map with country',
              input_element_mapper=self.to_team_with_city,
              setup_action=lambda: print(expected_action_message),
              team_countries=AsDict(countries_side_inputs))
         )

    def to_team_with_city(self, team_name: str, team_countries: Dict[str, str]) -> TeamInfo:
        return TeamInfo(name=team_name, country=team_countries[team_name], city='')

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

    def filter_element_with_simulation_error(self, team_name: str):
        if team_name == 'PSG':
            raise ValueError('PSG filter error')

        return team_name

    def _check_result_matches_expected(self, expected_failure: Failure, result_failure: Failure):
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
