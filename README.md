![Logo](asgarde_logo_small.gif)

# Asgarde

[![PyPI](https://img.shields.io/pypi/v/asgarde?logo=pypi&logoColor=white&label=PyPI&color=blue)](https://pypi.org/project/asgarde/)
[![Python versions](https://img.shields.io/pypi/pyversions/asgarde?logo=python&logoColor=white)](https://pypi.org/project/asgarde/)
[![CI](https://github.com/tosun-si/pasgarde/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/tosun-si/pasgarde/actions/workflows/ci.yml)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-%E2%89%A5%202.60.0-E25A1C?logo=apache&logoColor=white)](https://beam.apache.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Docs](https://img.shields.io/badge/docs-tosun--si.github.io%2Fasgarde-E25A1C?logo=astro&logoColor=white)](https://tosun-si.github.io/asgarde/)
[![License: MIT](https://img.shields.io/github/license/tosun-si/pasgarde)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/tosun-si/pasgarde?style=social)](https://github.com/tosun-si/pasgarde)

This module allows simplifying error handling with Apache Beam Python.

📖 **Documentation (Java, Kotlin and Python): https://tosun-si.github.io/asgarde/**

> Asgarde also exists for **Apache Beam Java and Kotlin**: [asgarde](https://github.com/tosun-si/asgarde)
> (Maven Central `fr.groupbees:asgarde`).

## Compatibility with Apache Beam

Starting with Asgarde `1.0.0`, Asgarde is **not tied to a specific Beam version**:

- `apache-beam` is declared as a minimum version (`>=2.60.0`), not pinned: your pipeline brings its own Beam version.
- The CI runs the whole test suite with the minimum Beam version and the **latest Beam release**, on every push
  and every week. A new Asgarde version is only released when a Beam release actually requires a change.
- Python `3.10` to `3.14`.

<details>
<summary>Legacy versions (before 1.0.0)</summary>

| Asgarde | Beam       |
|---------|------------|
| 0.16.0  | >= 2.37.0  |

</details>

## Installation of project

The project is hosted on [PyPI](https://pypi.org/project/asgarde/), the package is named `asgarde`.

```bash
# uv
uv add asgarde

# pip
pip install asgarde
```

## Example of native error handling with Beam

The following example shows error handling in each step with usual Beam code.

```python
@dataclass
class TeamInfo:
    name: str
    country: str
    city: str

@dataclass
class Failure:
    pipeline_step: str
    input_element: str
    exception: Exception

team_names = [
    'PSG',
    'OL',
    'Real',
    'ManU'
]
    
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

# In Beam pipeline.
input_teams: PCollection[str] = p | 'Read' >> beam.Create(team_names)

outputs_map1, failures_map1 = (input_teams | 'Map to team with country' >> ParDo(MapToTeamWithCountry())
                               .with_outputs(FAILURES, main='outputs'))

outputs_map2, failures_map2 = (outputs_map1 | 'Map to team with city' >> ParDo(MapToTeamWithCity())
                               .with_outputs(FAILURES, main='outputs'))

outputs_filter, failures_filter = (outputs_map2 | 'Filter France teams' >> ParDo(FilterFranceTeams())
                                   .with_outputs(FAILURES, main='outputs'))

all_failures = (failures_map1, failures_map2, failures_filter) | 'All Failures PCollections' >> beam.Flatten()
```

This example starts with an input `PCollection` containing team names.\
Then 3 operations and steps are applied : 2 maps and 1 filter.

For each operation a custom `DoFn` class is proposed and must override `process` function containing the 
transformation logic.\
A `try except bloc` is added to catch all the possible errors.\
In the `Except` bloc a `Failure` object is built with input element and current exception. This object is then added on 
a `tuple tag` dedicated to errors.\
This `tag` mechanism allows having multi sink in the pipeline and a dead letter queue for failures.

There are some inconveniences : 
- We have to repeat many technical codes and same logic like `try except bloc`, `tuple tags`, 
`failure logic` and all this logic can be centralized.
- If we want to intercept all the possible errors in the pipeline, we have to repeat the recovery of output and failure in each step.
- All the failures `PCollection` must be concatenated at end.
- The code is verbose.

The repetition of technical codes is error-prone and less maintainable.


## Example of error handling using Asgarde library

```python
# Beam pipeline with Asgarde library.
input_teams: PCollection[str] = p | 'Read' >> beam.Create(team_names)

result = (CollectionComposer.of(input_teams)
            .map('Map with country', lambda tname: TeamInfo(name=tname, country=team_countries[tname], city=''))
            .map('Map with city', lambda tinfo: TeamInfo(name=tinfo.name, country=tinfo.country, city=team_cities[tinfo.name]))
            .filter('Filter french team', lambda tinfo: tinfo.country == 'France'))

result_outputs: PCollection[TeamInfo] = result.outputs
result_failures: PCollection[Failure] = result.failures
```

### CollectionComposer class

Asgarde proposes a `CollectionComposer` wrapper class instantiated from a `PCollection`.

### Operators exposed by CollectionComposer class

The `CollectionComposer` class exposes the following operators : `map`, `flat_map` and `filter`.

These classical operators takes a function, the implementation can be : 
- A `lambda expression`
- A `method` having the same signature of the expected `function`

### Failure object exposed by Asgarde

Behind the scene, for each step the `CollectionComposer` class adds `try except` bloc and `tuple tag logic` with output
and failure `sinks`.

The bad sink is based on a `Failure` object proposed by the library : 

```python
@dataclass
class Failure:
    pipeline_step: str
    input_element: str
    exception: Exception
    stack_trace: str = ''
```

This object contains the current pipeline step name, input element with string form, current exception and its
stack trace as a string (a pickled exception loses its traceback).

In a custom `DoFn`, build it with `Failure.from_exception(pipeline_step, element, exception)`: it applies the rules
below and never raises.

Input element on Failure object are built following these rules :
- If the current element in the `PCollection` is a `dict`, the Json string form of this `dict` is retrieved
  (non JSON types like `datetime` or `bytes` are converted with `str`)
- For all others types, the `string` form of object is retrieved. If developers want to bring their own serialization
logic, they have to override `__str__` method in the object, example for a `dataclass` : 

```python
import dataclasses
import json
from dataclasses import dataclass

@dataclass
class Team:
    name: str

    def __str__(self) -> str:
        return json.dumps(dataclasses.asdict(self))
```

### Result of CollectionComposer flow

After applying and chaining different operations, the `CollectionComposer` class exposes :
- `outputs`: the output `PCollection`
- `failures`: the failures `PCollection` of all the steps

```python
result = (CollectionComposer.of(input_teams)
            .map('Map with country', lambda tname: TeamInfo(name=tname, country=team_countries[tname], city=''))
            .map('Map with city', lambda tinfo: TeamInfo(name=tinfo.name, country=tinfo.country, city=team_cities[tinfo.name]))
            .filter('Filter french team', lambda tinfo: tinfo.country == 'France'))

result_outputs: PCollection[TeamInfo] = result.outputs
result_failures: PCollection[Failure] = result.failures
```

### Example of a flow with side inputs

`Asgarde` allows applying transformations with error handling and passing `side inputs`.\
The syntax is the same as usual Beam pipeline with `AsDict` or `AsList` passed as function parameters,
as keyword or positional arguments.

```python
def to_team_with_city(self, team_name: str, team_countries: Dict[str, str]) -> TeamInfo:
    return TeamInfo(name=team_name, country=team_countries[team_name], city='')

team_countries = {
    'PSG': 'France',
    'OL': 'France',
    'Real': 'Spain',
    'ManU': 'England'
}

# Side inputs.
countries_side_inputs = p | 'Countries' >> beam.Create(team_countries)

# Beam Pipeline.
result = (CollectionComposer.of(input_teams)
            .map('Map with country', self.to_team_with_city, team_countries=AsDict(countries_side_inputs))
            .map('Map with city', lambda ti: TeamInfo(name=ti.name, country=ti.country, city=team_cities[ti.name]))
            .filter('Filter french team', lambda ti: ti.country == 'France'))

result_outputs: PCollection[str] = result.outputs
result_failures: PCollection[Failure] = result.failures
```

### Asgarde and error handling with Beam DoFn lifecyle

`Asgarde` allows interacting with `DoFn` lifecycle while chaining transformation with error handling, example : 

```python
(CollectionComposer.of(input_teams)
     .map('Map to Team info',
          input_element_mapper=lambda team_name: TeamInfo(name=team_name, country='test', city='test'),
          setup_action=lambda: print('Setup action'),
          start_bundle_action=lambda: print('Start bundle action'),
          finish_bundle_action=lambda: print('Finish bundle action'),
          teardown_action=lambda: print('Teardown action'))
     )
```

The `map` and `flat_map` methods of `CollectionComposer` class propose the following functions to interact with 
`DoFn` lifecycle :
- setup_action
- start_bundle_action
- finish_bundle_action
- teardown_action

These functions are keyword-only arguments, they take a `function` without input parameter and return `None`,
it corresponds to an action executed in the dedicated lifecycle method : 

https://beam.apache.org/documentation/transforms/python/elementwise/pardo/

## Failure handling guarantees

Asgarde must never make a job fail because of the error handling itself:

* **Non picklable exceptions**: failures are pickled by Beam. An exception that can't be pickled (holding a lock
  or a client, custom `__init__` signature...) is replaced by a `SerializableException` keeping the original type
  and message. Picklable exceptions are kept as is.
* **Input element**: the conversion to string never raises (dict with non JSON types, failing `__str__`...).
* **`MemoryError`** is re-raised: the worker is unstable and the runner must handle it, not the dead letter queue.
* **`flat_map`**: the outputs of an element are materialized before being emitted. An iterable failing in the
  middle gives a failure only, no partial outputs that would be duplicated when the failure is replayed.
* **Deterministic labels**: the failures are flattened once in a `Get all failures of <last step name>` transform,
  needed by Dataflow streaming updates (`--update`). Several composers can start from the same `PCollection`.

### Failure metrics

Each failure increments a Beam counter, visible in the runner UI (e.g. the Dataflow job metrics):

* namespace: `asgarde-failures` (`asgarde.FAILURES_METRICS_NAMESPACE`)
* name: the pipeline step name

```python
from apache_beam.metrics.metric import MetricsFilter

from asgarde import FAILURES_METRICS_NAMESPACE

counters = result.metrics().query(MetricsFilter().with_namespace(FAILURES_METRICS_NAMESPACE))['counters']
```

### Advantage of using Asgarde

`Asgarde` presents the following advantages :
- Simplifies error handling with less code and more expressive and concise code
- No need to repeat same technical code for error handling like `try except` bloc, `tuple tags` and concatenation of all the pipeline failures
- Allows interacting with Beam lifecycle while chaining the transformation and error handling

## Roadmap

Ideas for the next versions, feedback and contributions are welcome (see [Contributing](#contributing)):

- **Any Beam transform in the composer**: plug a custom `DoFn` or `PTransform` using the Beam native error
  handling (`with_exception_handling`), its failures converted to `Failure` objects.
- **Replayable input element**: pluggable element serializer (JSON, bytes...) instead of `str`, to replay the
  failures from the dead letter queue and avoid leaking sensitive data.
- **Beam schema for `Failure`**: write the failures directly to BigQuery.
- **Ready-to-use failure sinks**: BigQuery, GCS, Pub/Sub.

## Contributing

Contributions are welcome, see [CONTRIBUTING.md](CONTRIBUTING.md).
