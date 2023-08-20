import datetime
import functools
import os
from typing import Callable, List, Tuple

import titanpublic

from shared import lookups
from shared.shared_types import (
    Date,
    GameDetailLookup,
    GameHashLookup,
    GameHash,
    NodeName,
    Node,
)


def myhash(s: str) -> int:
    return hash(s)


def graph_sort(graph: List[Node]) -> List[Node]:
    # TODO: Topological sort, assume today already ordered.  Keep docker_images
    #  together as much as possible.
    return graph


def _date_parts(date: Date) -> Tuple[int, int, int]:
    year, month_day = divmod(date, 10000)
    month, day = divmod(month_day, 100)
    return year, month, day


def _date_int(date: datetime.datetime) -> int:
    return date.year * 10000 + date.month * 100 + date.day


@functools.lru_cache(None)
def _dependent_hashes_from_date_range(sport: str, st: Date, en: Date) -> List[GameHash]:
    """Function exists for caching reasons"""
    all_hashes = list()
    dt = datetime.datetime(*_date_parts(st))
    dti = st
    while dti < en:
        # game_hash_lookup will be defined by the time this is called.
        all_hashes += lookups.game_hash_lookup().get(dti, [])
        dt += datetime.timedelta(days=1)
        dti = _date_int(dt)
    return all_hashes


def _dependent_hashes_from_date_multi_range(
    sport: str, multi_range: titanpublic.shared_types.MultiRange
) -> List[GameHash]:
    result = list()
    for st, en in multi_range.ranges:
        result += _dependent_hashes_from_date_range(sport, st, en)
    return result


def dependent_hashes_from_date_map(
    date_map: Callable[[Date], Tuple[Date, Date]]
) -> Callable[[GameHash, GameHashLookup, GameDetailLookup], List[GameHash]]:
    def result(
        game_hash: GameHash, _: GameHashLookup, game_detail_lookup: GameDetailLookup
    ) -> List[GameHash]:
        game_date = game_detail_lookup[game_hash].date
        st, en = date_map(game_date)
        return _dependent_hashes_from_date_range(os.environ.get("SPORT"), st, en)

    return result


def dependent_hashes_from_date_map_multi_range(
    date_map: Callable[[Date], titanpublic.shared_types.MultiRange]
) -> Callable[[GameHash, GameHashLookup, GameDetailLookup], List[GameHash]]:
    def result(
        game_hash: GameHash, _: GameHashLookup, game_detail_lookup: GameDetailLookup
    ) -> List[GameHash]:
        game_date = game_detail_lookup[game_hash].date
        return _dependent_hashes_from_date_multi_range(
            os.environ["SPORT"], date_map(game_date)
        )

    return result


def same_game(game_hash: GameHash, *_) -> List[GameHash]:
    return [game_hash]


def dependencies(
    game_hash: GameHash, node: Node, node_by_name: Dict[NodeName, Node]
) -> Iterator[Tuple(int, Node, GameHash)]:
    ind = 0
    for dependent_features, dependent_hash_generator in node.dependencies:
        dependent_hashes = dependent_hash_generator(
            game_hash,
            lookups.game_hash_lookup(),
            lookups.game_detail_lookup(),
        )
        for d in dependent_hashes:
            d_node = node_by_name[d]
            for gh in dependent_hashes:
                yield (ind, d_node, gh)
        # Some arcane code cares about the different dependency families
        ind += 1
