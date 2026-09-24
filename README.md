<img src="https://raw.githubusercontent.com/tosun-si/asgarde/main/asgarde_logo.png" alt="Asgarde logo" width="200">

# Asgarde

[![PyPI](https://img.shields.io/pypi/v/asgarde?logo=pypi&logoColor=white&label=PyPI&color=blue)](https://pypi.org/project/asgarde/)
[![Python versions](https://img.shields.io/pypi/pyversions/asgarde?logo=python&logoColor=white)](https://pypi.org/project/asgarde/)
[![CI](https://github.com/tosun-si/pasgarde/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/tosun-si/pasgarde/actions/workflows/ci.yml)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-%E2%89%A5%202.60.0-E25A1C?logo=apache&logoColor=white)](https://beam.apache.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Docs](https://img.shields.io/badge/docs-tosun--si.github.io%2Fasgarde-E25A1C?logo=astro&logoColor=white)](https://tosun-si.github.io/asgarde/)
[![License: MIT](https://img.shields.io/github/license/tosun-si/pasgarde)](https://github.com/tosun-si/pasgarde/blob/main/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/tosun-si/pasgarde?style=social)](https://github.com/tosun-si/pasgarde)

**Error handling and dead letter queues for Apache Beam Python, without the boilerplate.** Also available for Java and
Kotlin with [Asgarde Java](https://github.com/tosun-si/asgarde).

📖 **Documentation: https://tosun-si.github.io/asgarde/**

## Why Asgarde

With plain Beam, each step needs its own `DoFn` with a `try/except` block and tagged outputs, and all the failures
must be flattened at the end. Asgarde keeps a fluent flow and gathers the failures of all the steps:

```python
from asgarde import CollectionComposer

result = (CollectionComposer.of(values)
          .map('Trim', lambda value: value.strip())
          .map('Parse', int)
          .filter('Keep even numbers', lambda number: number % 2 == 0))

outputs = result.outputs    # Output of the last step
failures = result.failures  # The failures of all the steps, for your dead letter queue
```

## Installation

Asgarde is published on [PyPI](https://pypi.org/project/asgarde/). `apache-beam` is a minimum version, not a pin:
your pipeline brings its own Beam version.

```bash
uv add asgarde      # or: pip install asgarde
```

## Features

- **One place for all the errors**: each step catches its errors in a
  [`Failure`](https://tosun-si.github.io/asgarde/concepts/failure/) (with the stack trace), the
  [`CollectionComposer`](https://tosun-si.github.io/asgarde/concepts/collection-composer/) gathers the failures of
  all the steps.
- **[`map`, `flat_map` and `filter` operators](https://tosun-si.github.io/asgarde/python/operators/)** with
  [side inputs and DoFn lifecycle actions](https://tosun-si.github.io/asgarde/python/side-inputs-lifecycle/), and
  `Failure.from_exception` for [your own DoFn](https://tosun-si.github.io/asgarde/python/custom-dofn/).
- **[Origin element](https://tosun-si.github.io/asgarde/concepts/origin-element/)**: with `with_origin_element`,
  the failures also give the element that entered the flow, to debug and replay from the start. Evaluated only when
  a failure occurs.
- **[Never breaks your job](https://tosun-si.github.io/asgarde/concepts/guarantees/)**: non picklable exceptions,
  non JSON dicts, partial `flat_map` outputs, deterministic labels for Dataflow updates.
- **[Failure metrics](https://tosun-si.github.io/asgarde/concepts/metrics/)**: a Beam counter per step.
- **Typed** (`py.typed`).

## Compatibility

Python `3.10` to `3.14`, `apache-beam>=2.60.0`. The CI tests the minimum Beam version and the latest Beam release on
every push and every week. See [Compatibility](https://tosun-si.github.io/asgarde/project/compatibility/).

## Roadmap

See the [roadmap](https://tosun-si.github.io/asgarde/project/roadmap/): replayable input element, Beam schema for the failures, ready-to-use failure sinks...

## Contributing

Contributions are welcome, see [CONTRIBUTING.md](https://github.com/tosun-si/pasgarde/blob/main/CONTRIBUTING.md).

## License

[MIT](https://github.com/tosun-si/pasgarde/blob/main/LICENSE)
