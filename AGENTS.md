<!-- agents-md-manager:start — managed section, do not edit by hand -->
# Asgarde (Python) — Project context

> Project-level context for AI coding agents (GitHub Copilot, Claude Code,
> Cursor…). **Conventions live in the referenced skills; this file only holds what
> is NOT in a skill and NOT derivable from code.**

## Identity

- **Domain**: open-source library (PyPI `asgarde`, repo `pasgarde`)
- **Project**: Asgarde for Apache Beam Python — Java/Kotlin counterpart: github.com/tosun-si/asgarde
- **Purpose**: error handling and dead letter queues for Apache Beam Python pipelines
- **Repo layout**: standalone uv project, `src/` layout

## Conventions — source of truth (referenced, NOT copied)

- `commit-open-source` — commit message style for this personal open-source repo
- `tag-opensource` — semver `vX.Y.Z` tags, PyPI Trusted Publishing; the version is derived from the tag (hatch-vcs), no version file to bump
- `unit-test-given-when-then` — pytest test naming and structure

## Project specifics (not in any skill, not derivable from code)

- **Beam decoupling (since 1.0.0)**: `apache-beam>=X` is a floor, never a pin. Never raise it just because Beam released — CI tests the latest Beam (push + weekly cron, opens an issue on regression). Release only when a Beam release forces a code change.
- **Parity with the Java lib**: same concepts, failure guarantees, metrics namespace (`asgarde-failures`) and roadmap; a change in one should be considered for the other.
- **Public API is the product**: `CollectionComposer`, `Failure`, the DoFn classes — keep changes backward compatible outside a major version.
<!-- agents-md-manager:end -->

<!-- Free zone — add durable, project-specific notes below. -->
<!-- Keep code-derivable facts (structure, deps, schedule) OUT. -->
