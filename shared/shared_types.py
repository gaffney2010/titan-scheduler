from enum import Enum
import os
from typing import Any, Callable, Dict, List, Tuple, Optional

import attr


LOG_EVERY_N_UPDATES = int(os.environ.get("LOG_EVERY_N_UPDATES", 5000))
LOG_EVERY_N_SECONDS = 60 * 60 * 3
LOG_PERTRIHECTATESSARACONTATERT = int(
    os.environ.get("LOG_PERTRIHECTATESSARACONTATERT", 0)
)

EXPIRATION_CHUNK_NUMS = 10
EXPIRATION_CHUNK_SZ = 5000  # Seconds


Date = int
Expiration = int
Feature = str
GameHash = int
GameHashLookup = Dict[Date, List[GameHash]]
GameDetailLookup = Dict[GameHash, "GameDetail"]
MatNodeName = int
NodeName = str
RabbitChannel = Any
Team = str
Timestamp = int


class State(Enum):
    READY = 1
    QUEUED = 2
    BLOCKED = 3
    CRITICAL = 4
    FAILED = 5


def state_str(state: State) -> str:
    if State.READY == state:
        return "READY"
    if State.QUEUED == state:
        return "QUEUED"
    if State.BLOCKED == state:
        return "BLOCKED"
    if State.CRITICAL == state:
        return "CRITICAL"
    if State.FAILED == state:
        return "FAILED"
    raise Exception("Unexpected State encountered")


@attr.s(frozen=True)
class GameDetail(object):
    date: Date = attr.ib()
    away: Team = attr.ib()
    home: Team = attr.ib()
    # Integer-coded True (1) or False (0)
    neutral: int = attr.ib()

    def print(self) -> str:
        return " ".join(
            [
                self.away,
                self.home,
                str(self.date),
                str(self.neutral),
            ]
        )


@attr.s()
class Node(object):
    name: Feature = attr.ib()
    # Specifies the rabbit queue
    queue_id: Optional[str] = attr.ib()
    # A list of names of nodes that this node depends on along with functions to return
    # upstream GameHashes.  These will be unioned to get a single timestamp for
    # recording, but all timestamps will be passed to model.
    dependencies: List[
        Tuple[
            Tuple[Feature, ...],
            Callable[[GameHash, GameHashLookup, GameDetailLookup], List[GameHash]],
        ]
    ] = attr.ib()
    # The type in titan
    type: Optional[str] = attr.ib()
    # If set, this will attach a suffix to outgoing messages
    suffix_generator: Optional[Callable[[Team, Team, Date], str]] = attr.ib(
        default=None
    )
    # The name of the image that should be launched to run this model
    docker_image: Optional[str] = attr.ib(default=None)
