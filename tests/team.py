from dataclasses import dataclass

from tests.player import Player


@dataclass
class Team:
    name: str
    score: int
    players: list[Player]
