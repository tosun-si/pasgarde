import time
from dataclasses import dataclass

import apache_beam as beam
import pytest
from apache_beam import DoFn, ParDo, PCollection, pvalue
from apache_beam.pvalue import AsDict
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty
from toolz.curried import map, pipe

from asgarde.collection_composer import CollectionComposer
from asgarde.failure import Failure
from tests.player import Player
from tests.team import Team
from tests.testing_helper import log_element

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


EXECUTED_ACTIONS: list[str] = []


def record_action(action: str) -> None:
    """
    Records a DoFn lifecycle action. A list instead of the captured stdout: the teardown is called asynchronously by
    the runner, and a print between two phases of pytest isn't captured by capsys, which made the tests flaky.
    """
    EXECUTED_ACTIONS.append(action)


def wait_for_action(action: str, timeout_seconds: float = 30) -> list[str]:
    """
    Waits for the action, the DoFn teardown being called asynchronously, possibly after the end of the pipeline.
    """
    deadline = time.monotonic() + timeout_seconds
    while action not in EXECUTED_ACTIONS and time.monotonic() < deadline:
        time.sleep(0.1)

    return list(EXECUTED_ACTIONS)


@pytest.fixture
def executed_actions():
    EXECUTED_ACTIONS.clear()
    yield EXECUTED_ACTIONS
    EXECUTED_ACTIONS.clear()


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
            executed_actions):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p, setup_action=lambda: record_action(SETUP_ACTION_MESSAGE))

        assert executed_actions == [SETUP_ACTION_MESSAGE]

    def test_when_call_dofn_lifecycle_start_bundle_action_with_error_handling_then_expected_action_is_executed(
            self,
            executed_actions):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p, start_bundle_action=lambda: record_action(START_BUNDLE_ACTION_MESSAGE))

        assert set(executed_actions) == {START_BUNDLE_ACTION_MESSAGE}

    def test_when_call_dofn_lifecycle_finish_bundle_action_with_error_handling_then_expected_action_is_executed(
            self,
            executed_actions):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p, finish_bundle_action=lambda: record_action(FINISH_BUNDLE_ACTION_MESSAGE))

        assert set(executed_actions) == {FINISH_BUNDLE_ACTION_MESSAGE}

    def test_when_call_dofn_lifecycle_teardown_action_with_error_handling_then_expected_action_is_executed(
            self,
            executed_actions):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p, teardown_action=lambda: record_action(TEARDOWN_ACTION_MESSAGE))

        assert wait_for_action(TEARDOWN_ACTION_MESSAGE) == [TEARDOWN_ACTION_MESSAGE]

    def test_when_call_dofn_lifecycle_all_actions_with_error_handling_then_expected_all_actions_are_executed(
            self,
            executed_actions):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(
                p,
                setup_action=lambda: record_action(SETUP_ACTION_MESSAGE),
                start_bundle_action=lambda: record_action(START_BUNDLE_ACTION_MESSAGE),
                finish_bundle_action=lambda: record_action(FINISH_BUNDLE_ACTION_MESSAGE),
                teardown_action=lambda: record_action(TEARDOWN_ACTION_MESSAGE))

        actions = wait_for_action(TEARDOWN_ACTION_MESSAGE)
        assert set(actions) == {
            SETUP_ACTION_MESSAGE, START_BUNDLE_ACTION_MESSAGE, FINISH_BUNDLE_ACTION_MESSAGE, TEARDOWN_ACTION_MESSAGE}
        assert actions[0] == SETUP_ACTION_MESSAGE
        assert actions[-1] == TEARDOWN_ACTION_MESSAGE

    def test_when_call_dofn_lifecycle_no_action_with_error_handling_then_no_action_executed(
            self,
            executed_actions):
        with TestPipeline() as p:
            self.execute_lifecyle_action_test(p)

        assert executed_actions == []

    def execute_lifecyle_action_test(self, p, **lifecycle_actions):
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
              team_countries=AsDict(countries_side_inputs),
              **lifecycle_actions)
         )

    def to_team_with_city(self, team_name: str, team_countries: dict[str, str]) -> TeamInfo:
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
        expected_exception_is_value_error: bool = type(expected_exception) is ValueError

        return (result_input_equals_expected
                and result_exception_equals_expected
                and expected_exception_is_value_error)
