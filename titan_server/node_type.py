"""This base type is needed for dependencies across files."""

from enum import Enum
from typing import Optional

from shared.shared_types import Expiration, MatNodeName, Timestamp


class AbstractBaseNode(object):
    expiration: Optional[Expiration]
    name: MatNodeName
    output_ts: Timestamp

    def fail(self, _: Optional[int], __: bool = False) -> None:
        raise NotImplementedError


class NodeType(Enum):
    CONCRETE = 1
    PSEUDO = 2
