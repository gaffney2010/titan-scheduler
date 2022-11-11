import logging
import os
from typing import Dict, Set

import docker
import titanpublic

from shared import lookups, rabbit, timestamp_manager
from shared.shared_types import GameHash, Node, NodeName


class DockerContainer(object):
    def __init__(self, docker_image: str):
        client = docker.from_env()
        self.model_container = client.containers.run(docker_image, detach=True)

    def __enter__(self) -> docker.Container:
        return self.model_container

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.model_container.stop()


# Return timestamp object
def get_expected_input_ts(
    game_hash: GameHash, node: Node, node_by_name: Dict[NodeName, Node]
) -> timestamp_manager.InputTimestampManager:
    tss = list()
    for dependent_features, dependent_hash_generator in node.dependencies:
        max_ts = 0
        dependent_hashes = dependent_hash_generator(
            game_hash,
            lookups.game_hash_lookup(),
            lookups.game_detail_lookup(),
        )
        for f in dependent_features:
            node = node_by_name[f]
            for gh in dependent_hashes:
                max_ts = max(max_ts, lookups.timestamp_lookup(node)[gh][1])
        tss.append(max_ts)
    return timestamp_manager.ts_manager_factory(tss)


def run_feature(node: Node, node_by_name: Dict[NodeName, Node]) -> None:
    assert node.docker_image is not None
    with DockerContainer(node.docker_image):
        queued_games: Set[GameHash] = set()
        for game_hash in lookups.game_detail_lookup().keys():
            expected_input_ts = get_expected_input_ts(game_hash, node, node_by_name)
            actual_input_ts = lookups.timestamp_lookup(node)[game_hash][0]
            if actual_input_ts < expected_input_ts.max_ts():
                queued_games.add(game_hash)
                rabbit.compose_rabbit_msg(node, game_hash, expected_input_ts)

        def mark_success(ch, method, properties, body):
            (
                _,
                model_name,
                _,
                away,
                home,
                date,
                _,
                _,
                status,
            ) = body.split()
            this_game_hash = titanpublic.hash.game_hash(away, home, date)
            assert model_name == node.name
            assert "success" == status
            queued_games.remove(this_game_hash)

        def still_waiting() -> bool:
            return len(queued_games) != 0

        # Wait here until we've gotten success messages for each game.
        rabbit.get_rabbit_channel().consume_while_condition(mark_success, still_waiting)


if __name__ == "__main__":
    sport = os.environ.get("SPORT")
    if "ncaam" == sport:
        from dags import ncaam

        dag = ncaam.graph

    # if "ncaaw" == sport:
    #     from dags import ncaaw

    #     dag = ncaaw.graph

    # if "ncaaf" == sport:
    #     from dags import ncaaf

    #     dag = ncaaf.graph

    node_by_name = {node.name: node for node in dag}

    # Run all the features in order.
    for node in dag:
        logging.error(f"Running {node.name}")
        run_feature(node, node_by_name)
