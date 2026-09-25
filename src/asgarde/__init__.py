"""
Asgarde: error handling and dead letter queues for Apache Beam Python pipelines.
"""
from importlib.metadata import PackageNotFoundError, version

from asgarde.collection_composer import CollectionComposer
from asgarde.failure import (
    FAILURE_BIGQUERY_SCHEMA,
    FAILURE_BIGQUERY_SCHEMA_WITH_ENCODED_ELEMENTS,
    Failure,
    SerializableException,
)
from asgarde.transforms.do_fns_error_handling import FAILURES, FAILURES_METRICS_NAMESPACE

try:
    __version__ = version('asgarde')
except PackageNotFoundError:
    __version__ = 'unknown'

__all__ = [
    'CollectionComposer',
    'Failure',
    'FAILURE_BIGQUERY_SCHEMA',
    'FAILURE_BIGQUERY_SCHEMA_WITH_ENCODED_ELEMENTS',
    'SerializableException',
    'FAILURES',
    'FAILURES_METRICS_NAMESPACE',
    '__version__',
]
