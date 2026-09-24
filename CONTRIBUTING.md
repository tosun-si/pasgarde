# Contributing to Asgarde (Python)

Thanks for your interest in Asgarde! Bug reports, ideas and pull requests are welcome.

## Reporting a bug or proposing a feature

Open a [GitHub issue](https://github.com/tosun-si/pasgarde/issues) with:

- the Asgarde, Beam and Python versions, and the runner (Direct, Prism, Dataflow, Flink...);
- a minimal pipeline reproducing the problem, and the expected vs actual behavior.

For a new feature, open an issue first to discuss the design before writing code.

## Development setup

The project uses [uv](https://docs.astral.sh/uv/).

```bash
# Create the virtual env with the dev dependencies
uv sync

# Lint
uv run ruff check src tests

# Tests with coverage
uv run pytest --cov

# Tests against another Beam version (e.g. the minimum supported version)
uv pip install "apache-beam==2.60.0" && uv run --no-sync pytest

# Build the package
uv build
```

The CI runs the tests with the minimum Beam version on Python 3.10, and with the latest Beam release on
Python 3.12, 3.13 and 3.14.

## Rules for code changes

- **Beam stays a minimum version** (`apache-beam>=X` in `pyproject.toml`), never pinned. Only use Beam APIs
  available in this minimum version.
- **No new runtime dependency** without discussing it first.
- **Backward compatibility**: the public API (`CollectionComposer`, `Failure`, the DoFn classes) must stay
  compatible in minor and patch versions. A breaking change needs a major version.
- **Tests**: every bug fix or feature comes with tests using the `test_given_<x>_when_<y>_then_<z>` naming and the
  `# GIVEN`, `# WHEN`, `# THEN` blocks, with `TestPipeline` and `assert_that`.
- **Documentation**: update the `README.md` when the behavior or the public API changes.

## Pull requests

1. Fork the repo and create a branch from `main` (e.g. `feature/replayable-input-element`).
2. Use descriptive commit messages, e.g. `Add failure counters per pipeline step` (no Conventional Commits
   prefixes).
3. Open a pull request to `main` describing what changed and why, and how it was tested.
4. The CI must be green. Maintainers add the labels used to generate the release notes
   (`feature`, `bug`, `breaking-change`, `documentation`, `dependencies`, `security`...).

## Release process (maintainers)

The version comes from the git tag ([hatch-vcs](https://github.com/ofek/hatch-vcs)): there is no version file to bump.

1. Merge to `main`, check that the CI is green.
2. Tag the merge commit: `git tag -a vX.Y.Z -m "..." && git push origin vX.Y.Z`.
3. The `Release` workflow tests and builds the package, publishes it to PyPI with
   [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (no API token), then creates the GitHub Release
   with the notes generated from the pull request labels.

A new Beam version doesn't need an Asgarde release: the weekly CI run tests the latest Beam release and opens an
issue if something breaks.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
