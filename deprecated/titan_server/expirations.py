import collections
import functools
import time
from typing import Dict, Set

from shared.shared_types import Expiration, EXPIRATION_CHUNK_NUMS, EXPIRATION_CHUNK_SZ
from titan_server import node_type


@functools.lru_cache(1)
def get_expirations() -> Dict[Expiration, Set[node_type.AbstractBaseNode]]:
    return collections.defaultdict(set)


def add_expiration(node: node_type.AbstractBaseNode) -> Expiration:
    key = int(time.monotonic() // EXPIRATION_CHUNK_SZ + EXPIRATION_CHUNK_NUMS)
    get_expirations()[key].add(node)
    return key


def rm_expiration(node: node_type.AbstractBaseNode) -> None:
    assert node.expiration
    get_expirations()[node.expiration].remove(node)


def flush_expirations() -> None:
    key = int(time.monotonic() // EXPIRATION_CHUNK_SZ)
    if key not in get_expirations():
        return
    for node in get_expirations()[key]:
        node.fail(None)
    del get_expirations()[key]
