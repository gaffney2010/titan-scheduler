import logging
import os

logging_level = (
    logging.INFO if "prod" == os.environ.get("TITAN_ENV", "dev") else logging.DEBUG
)

logging.basicConfig(
    format=(
        "%(asctime)s "
        " %(levelname)s:\t%(module)s::%(funcName)s:%(lineno)d\t-\t%(message)s"
    ),
    level=logging_level,
)

#########################
# Everything above this line must be set before other imports, because logging is dumb.

import collections
import json
import traceback
from typing import DefaultDict, Dict, List, Tuple

import titanpublic
from tqdm import tqdm

import shared.log_manager as log_manager
import shared.rabbit as rabbit
from shared import shared_logic, timestamp_manager
from shared.shared_types import (
    Feature,
    GameHash,
    MatNodeName,
    Node,
    State,
    state_str,
)
from shared import lookups, sql_connection
from titan_server import nodes, node_type


def heartbeat(materialized_nodes) -> None:
    heartbeat_report: DefaultDict[str, DefaultDict[str, int]] = collections.defaultdict(
        lambda: collections.defaultdict(int)
    )

    for n in materialized_nodes.values():
        if n.current_heartbeat:
            if node_type.NodeType.CONCRETE == n.type:
                heartbeat_report[n.feature_node.name][state_str(n.state)] += 1
            n.current_heartbeat = False

    for n in materialized_nodes.values():
        # Nodes can get stuck sometimes, and all operations are idempotent.
        if State.FAILED == n.state:
            n.current_heartbeat = True
            n.queue_up()

    dev_suffix = ""
    if "dev" == os.environ.get("TITAN_ENV", "dev"):
        dev_suffix = "-dev"

    sport = os.environ.get("SPORT")
    log_manager.log_manager_singleton().free_log(
        f"{sport}{dev_suffix}:: heartbeat_report: {json.dumps(heartbeat_report)}"
    )


def _pseudo_node_key(dependent_feature: str, dependent_hashes: List[GameHash]) -> int:
    # Unusual key logic is for efficiency
    node_key = shared_logic.myhash(dependent_feature)
    for gh in dependent_hashes:
        node_key ^= shared_logic.myhash(str(gh))
    return node_key


def add_materialized_dependency(
    this_node: nodes.AbstractNode,
    dependent_feature: Feature,
    dependent_hashes: List[GameHash],
    materialized_nodes: Dict[Tuple[Feature, GameHash], nodes.MaterializedNode],
) -> MatNodeName:
    dependent_hash = dependent_hashes[0]
    this_node.add_backlink(materialized_nodes[(dependent_feature, dependent_hash)])
    return materialized_nodes[(dependent_feature, dependent_hash)].name


def add_pseudo_dependency(
    this_node: nodes.AbstractNode,
    dependent_feature: Feature,
    dependent_hashes: List[GameHash],
    materialized_nodes: Dict[Tuple[Feature, GameHash], nodes.MaterializedNode],
    pseudo_nodes: Dict[int, nodes.PseudoNode],
) -> MatNodeName:
    node_key = _pseudo_node_key(dependent_feature, dependent_hashes)
    if node_key not in pseudo_nodes:
        # Node doesn't yet exist
        node_group = [
            materialized_nodes[(dependent_feature, gh)] for gh in dependent_hashes
        ]
        pseudo_nodes[node_key] = nodes.PseudoNode(str(node_key), node_group)
        pseudo_nodes[
            node_key
        ].expected_input_ts = timestamp_manager.InputTimestampManager(
            [[n.name for n in node_group]]
        )
    this_node.add_backlink(pseudo_nodes[node_key])
    return pseudo_nodes[node_key].name


def add_dependencies(
    this_node: nodes.AbstractNode,
    dependent_features: Tuple[Feature, ...],
    dependent_hashes: List[GameHash],
    materialized_nodes: Dict[Tuple[Feature, GameHash], nodes.MaterializedNode],
    pseudo_nodes: Dict[int, nodes.PseudoNode],
) -> List[MatNodeName]:
    if len(dependent_hashes) == 0:
        return []

    dependent_nodes = list()
    for dependent_feature in dependent_features:
        if len(dependent_hashes) == 1:
            dependent_nodes.append(
                add_materialized_dependency(
                    this_node,
                    dependent_feature,
                    dependent_hashes,
                    materialized_nodes,
                )
            )
        else:
            dependent_nodes.append(
                add_pseudo_dependency(
                    this_node,
                    dependent_feature,
                    dependent_hashes,
                    materialized_nodes,
                    pseudo_nodes,
                )
            )
    return dependent_nodes


def materialize_graph(
    graph: List[Node],
) -> Dict[Tuple[Feature, GameHash], nodes.MaterializedNode]:
    graph = shared_logic.graph_sort(graph)

    # 1. Make a MaterializedNode for each / feature combo.
    logging.info("1. Make a MaterializedNode for each / feature combo.")
    materialized_nodes: Dict[Tuple[Feature, GameHash], nodes.MaterializedNode] = dict()
    for node in graph:
        for game_hash in lookups.game_detail_lookup().keys():
            n = nodes.MaterializedNode(
                "{}:{}".format(node.name, game_hash),
                game_hash,
                node,
            )
            n.actual_input_ts, n.output_ts = lookups.timestamp_lookup(node).get(
                game_hash, (0, 0)
            )
            materialized_nodes[(node.name, game_hash)] = n

    # 2. Use dependency logic to build to / from nodes.
    logging.info("2. Use dependency logic to build to / from nodes.")
    pseudo_nodes: Dict[int, nodes.PseudoNode] = dict()
    for node in graph:
        logging.info(f"Building {node.name}...")
        if not node.dependencies:
            # This should really only happen for "games"
            for game_hash in lookups.game_detail_lookup().keys():
                materialized_nodes[
                    (node.name, game_hash)
                ].expected_input_ts = timestamp_manager.EmptyInputTimestampManager()
            continue

        for game_hash in tqdm(lookups.game_detail_lookup().keys()):
            this_node = materialized_nodes[(node.name, game_hash)]
            dependent_nodes_lists = list()
            for dependent_features, dependent_hash_generator in node.dependencies:
                dependent_hashes = dependent_hash_generator(
                    game_hash,
                    lookups.game_hash_lookup(),
                    lookups.game_detail_lookup(),
                )
                dependent_nodes_lists.append(
                    add_dependencies(
                        this_node,
                        dependent_features,
                        dependent_hashes,
                        materialized_nodes,
                        pseudo_nodes,
                    )
                )

            this_node.expected_input_ts = timestamp_manager.InputTimestampManager(
                dependent_nodes_lists
            )

    # sorted_graph = graph_sort_materialized_nodes(materialized_nodes, pseudo_nodes)

    # 3. For each node, propagate output timestamp one step to get expected input
    #  timestamp.
    logging.info(
        "3. For each node, propagate output timestamp one step to get expected input"
        " timestamp."
    )
    for game_hash in tqdm(lookups.game_detail_lookup().keys()):
        this_node = materialized_nodes[("games", game_hash)]
        this_node.fail(0)

    # 4. Send a heartbeat to try the failures.
    logging.info("4. Set states")
    heartbeat(materialized_nodes)

    return materialized_nodes


def update(node: nodes.MaterializedNode) -> None:
    """Reload timestamps from DB, and mark for rerun."""
    assert "games" == node.feature_node.name
    game_hash = node.game_hash

    with sql_connection.titan() as con:
        cur = con.cursor()
        cur.execute(f"SELECT timestamp FROM games WHERE game_hash = {game_hash};")
        new_ts = cur.fetchone()[0]

    node.output_ts = new_ts
    node.fail(0, allow_fail_on_ready=True)


def process_message(body, materialized_nodes) -> None:
    body = body.decode()

    if body == "heartbeat":
        log_manager.log_manager_singleton().free_log("heartbeat")
        heartbeat(materialized_nodes)
        return

    if body[:11] == "clear_games":
        game_hash = int(body.split()[1])
        node = materialized_nodes[("games", game_hash)]
        update(node)
        return

    (
        sport,
        model_name,
        input_timestamp,
        away,
        home,
        date,
        neutral,
        output_timestamp,
        status,
    ) = body.split()

    input_timestamp = input_timestamp.split(",")
    input_timestamp = [int(x) for x in input_timestamp]
    input_timestamp = max(input_timestamp)
    output_timestamp = int(output_timestamp)

    game_hash = titanpublic.hash.game_hash(away, home, date)
    node = materialized_nodes[(model_name, game_hash)]
    if "success" == status:
        node.succeed(int(input_timestamp), int(output_timestamp))
    elif "failure" == status:
        node.fail(int(input_timestamp))
    elif "critical" == status:
        node.critical()


if __name__ == "__main__":
    sport = os.environ.get("SPORT")
    if "ncaam" == sport:
        from dags import ncaam

        dag = ncaam.graph

    if "ncaaw" == sport:
        from dags import ncaaw

        dag = ncaaw.graph

    if "ncaaf" == sport:
        from dags import ncaaf

        dag = ncaaf.graph

    logging.info("Building graph")
    log_manager.log_manager_singleton().free_log("Start")
    materialized_nodes = materialize_graph(dag)

    def callback(ch, method, properties, body):
        logging.info(f"Found {body}")

        if "prod" == os.environ.get("TITAN_ENV", "dev"):
            try:
                process_message(body, materialized_nodes)
            except:  # noqa: E722
                # Lots of reasons this would happen.
                logging.error(f"ERROR on: {body}")
                logging.error(traceback.format_exc())
        else:
            # Let it crash
            process_message(body, materialized_nodes)

    rabbit.get_rabbit_channel().consume_to_death(callback)
