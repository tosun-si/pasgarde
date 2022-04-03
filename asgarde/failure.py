from dataclasses import dataclass


@dataclass
class Failure:
    """
    Object returned in the failure sink.
    """
    pipeline_step: str
    input_element: str
    exception: Exception
