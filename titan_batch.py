import logging
import os

logging_level = (
    logging.INFO if "dev" == os.environ.get("TITAN_ENV", "dev") else logging.ERROR
)

logging.basicConfig(
    format=(
        "%(asctime)s "
        " %(levelname)s:\t%(module)s::%(funcName)s:%(lineno)d\t-\t%(message)s"
    ),
    level=logging.INFO,  # logging_level,
)

#########################
# Everything above this line must be set before other imports, because logging is dumb.

from typing import Any, Dict, Set

import docker
import retrying
import titanpublic
from tqdm import tqdm

from shared import lookups, timestamp_manager, queuer
from shared.shared_types import GameHash, Node, NodeName


WAIT_FIXED_SECS = 3
NUM_RETRIES = 5

DContainer = Any  # docker-py doesn't expose Container type


class DockerContainer(object):
    def __init__(self, docker_image: str):
        logging.error(docker_image)
        client = docker.from_env()
        # TODO: Pass this in
        self.model_container = client.containers.run(
            docker_image,
            environment={
                "TITAN_ENV": os.environ.get("TITAN_ENV", "dev"),
                "SPORT": os.environ.get("SPORT", "ncaam"),
            },
            volumes={
                "/var/lib/redis": {
                    "bind": "/data",
                    "mode": "rw",
                }
            },
            ports={
                "3306/tcp": 3306,
            },
            detach=True,
        )

    def __enter__(self) -> DContainer:
        return self.model_container

    # TODO: Delete image somehow
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
            f_node = node_by_name[f]
            for gh in dependent_hashes:
                if dependent_node_ts := lookups.timestamp_lookup(f_node).get(gh, None):
                    max_ts = max(max_ts, dependent_node_ts[1])
        tss.append(max_ts)
    return timestamp_manager.ts_manager_factory(tss)


@retrying.retry(
    wait_fixed=WAIT_FIXED_SECS * 1000,
    stop_max_attempt_number=NUM_RETRIES,
)
def run_feature(node: Node, node_by_name: Dict[NodeName, Node]) -> None:
    logging.info(f"Starting feature {node.name}...")

    queued_games: Set[GameHash] = set()
    for game_hash in lookups.game_detail_lookup().keys():
        expected_input_ts = get_expected_input_ts(game_hash, node, node_by_name)
        actual_input_ts = lookups.timestamp_lookup(node).get(game_hash, (0, 0))[0]
        if actual_input_ts < expected_input_ts.max_ts():
            queued_games.add(game_hash)
            queuer.compose_queued_msg(
                titanpublic.queuer.get_redis_channel(), game_hash, expected_input_ts
            )

    logging.info(f"Queued up {len(queue_games)} games...")
    t = tqdm(total=len(queued_games))

    def mark_success(ch, method, properties, body):
        if "heartbeat" == body:
            # These are floating around because of previous server runs.
            return

        # logging.debug("New success")
        if len(body.split()) != 9:
            logging.error("Length error, should never happen")
            logging.error(body)

        (
            _,
            model,
            input_timestamp,
            away,
            home,
            date,
            _,
            _,
            status,
        ) = body.decode().split()

        if model != node.name:
            logging.debug(f"Skipping model {model}, expected {node.name}")
            return

        this_game_hash = titanpublic.hash.game_hash(away, home, date)
        if "success" == status:
            # print(str(len(queued_games)) + " " + node.name + " remaining")
            if this_game_hash in queued_games:
                # Tolerate resends
                queued_games.remove(this_game_hash)
                tqdm.update(1)
        elif "failure" == status:
            # Requeue
            print(f"Retrying failed model: {body}")
            queuer.compose_queued_msg(
                titanpublic.queuer.RedisChannel(),
                node,
                this_game_hash,
                timestamp_manager.SimpleTimesampWrapper(input_timestamp),
            )
        else:
            # Probably critical
            raise Exception(f"Got bad stuff: {body}")

    def still_waiting() -> bool:
        print(str(len(queued_games)) + " " + node.name + " remaining (C)")
        return len(queued_games) > 0

    # Wait here until we've gotten success messages for each game.
    titanpublic.queuer.get_redis_channel().consume_while_condition(
        mark_success, still_waiting
    )

    t.close()


if __name__ == "__main__":
    sport = os.environ.get("SPORT")
    logging.info(f"Running for sport = {sport}")
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

    # Run all the features in order, grouping together consecutive docker_image where
    #  possible.
    st, en = 0, 0
    while st < len(dag):
        while en < len(dag) and dag[en].docker_image == dag[st].docker_image:
            en += 1

        this_image = dag[st].docker_image
        if this_image is not None:
            with DockerContainer(this_image):
                for i in range(st, en):
                    logging.error(f"Running {dag[i].name}")
                    run_feature(dag[i], node_by_name)

        st = en
