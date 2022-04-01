from dataclasses import dataclass
from typing import List

from asgarde.tests.player import Player


@dataclass
class Team:
    name: str
    score: int
    players: List[Player]
