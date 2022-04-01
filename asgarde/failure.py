from dataclasses import dataclass


@dataclass
class Failure:
    pipeline_step: str
    input_element: str
    exception: Exception
