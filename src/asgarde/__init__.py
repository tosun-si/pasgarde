"""
Asgarde: error handling and dead letter queues for Apache Beam Python pipelines.
"""
from importlib.metadata import PackageNotFoundError, version

from asgarde.collection_composer import CollectionComposer
from asgarde.failure import Failure, SerializableException
from asgarde.transforms.do_fns_error_handling import FAILURES, FAILURES_METRICS_NAMESPACE

try:
    __version__ = version('asgarde')
except PackageNotFoundError:
    __version__ = 'unknown'

__all__ = [
    'CollectionComposer',
    'Failure',
    'SerializableException',
    'FAILURES',
    'FAILURES_METRICS_NAMESPACE',
    '__version__',
]
