import collections
import functools
from typing import Dict, Tuple

from shared import queuer, sql_connection
from shared.shared_types import (
    GameDetail,
    GameDetailLookup,
    GameHash,
    GameHashLookup,
    Node,
    Timestamp,
)


@functools.lru_cache(None)
def game_hash_lookup() -> GameHashLookup:
    result = collections.defaultdict(list)
    with sql_connection.titan() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT game_hash, date FROM games;
        """
        )
        for row in cur.fetchall():
            game_hash, date = row
            result[date].append(game_hash)
    return result


@functools.lru_cache(None)
def game_detail_lookup() -> GameDetailLookup:
    result = dict()
    with sql_connection.titan() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT game_hash, away, home, date, neutral FROM games;
        """
        )
        for row in cur.fetchall():
            game_hash, away, home, date, neutral = row
            result[game_hash] = GameDetail(
                away=away,
                home=home,
                date=date,
                neutral=1 if neutral else 0,
            )
    return result


@functools.lru_cache(None)
def timestamp_lookup_cache(
    node_name: str, node_type: str
) -> Dict[GameHash, Tuple[Timestamp, Timestamp]]:
    result = dict()
    with sql_connection.titan() as con:
        if "games" != node_name:
            # See if table needs creating
            cur = con.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {node_name} (
                    game_hash BIGINT,
                    value {node_type},
                    payload VARCHAR(32765),
                    input_timestamp BIGINT,
                    output_timestamp BIGINT,
                    PRIMARY KEY (game_hash)
                );
            """
            )
            con.commit()

        cur = con.cursor()
        input_ts_clause = (
            "0 as input_timestamp" if "games" == node_name else "input_timestamp"
        )
        output_ts_clause = (
            "timestamp as output_timestamp"
            if "games" == node_name
            else "output_timestamp"
        )
        cur.execute(
            f"""
            SELECT game_hash, {input_ts_clause}, {output_ts_clause} FROM {node_name};
        """
        )
        for row in cur.fetchall():
            game_hash, input_ts, output_ts = row
            result[game_hash] = (input_ts, output_ts)
    return result


def timestamp_lookup(node: Node) -> Dict[GameHash, Tuple[Timestamp, Timestamp]]:
    """This has a nicer API, but it isn't hashable."""
    # Hijack for some setup, if not done
    if node.queue_id is not None:
        queuer.get_redis_channel().queue_declare(node.queue_id)
        if node.suffix_generator is not None:
            queuer.get_redis_channel().exchange_declare(node.queue_id)

    return timestamp_lookup_cache(node.name, node.type)
