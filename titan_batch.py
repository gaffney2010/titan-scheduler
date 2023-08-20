from earthlings.titan_logging import titan_logger
import logging
import os

level = logging.DEBUG if "dev" == os.environ.get("TITAN_ENV", "dev") else logging.INFO
tlogger = titan_logger("batch_scheduler", False, file_level=level, console_level=level)

#########################
# Everything above this line must be set before other imports, because logging is dumb.

from typing import Any, Dict, Set

import docker
import retrying
import titanpublic
from tqdm import tqdm

from shared import lookups, shared_logic, timestamp_manager, queuer
from shared.shared_types import GameHash, Node, NodeName


WAIT_FIXED_SECS = 3
NUM_RETRIES = 1

TITAN_RECEIVER = "titan-receiver"

DContainer = Any  # docker-py doesn't expose Container type


def get_ipaddr() -> int:
    return os.system(
        "ifconfig | sed -n 's/.*\(192\.[0-9]\+\.[0-9]\+\.[0-9]\+\).*/\1/p'"
    )


class DockerContainer(object):
    def __init__(self, docker_image: str, lazy: bool = False):
        self.started = False
        self.docker_image = docker_image
        if not lazy:
            self.start_container()

    def start_container(self):
        if self.started:
            return

        tlogger.console_logger.error(self.docker_image)
        client = docker.from_env()
        # TODO: Pass this in
        self.model_container = client.containers.run(
            self.docker_image,
            environment={
                "TITAN_ENV": os.environ.get("TITAN_ENV", "dev"),
                "SPORT": os.environ.get("SPORT", "ncaam"),
                "PARENT_IP": get_ipaddr(),
            },
            # This uses local ports, along with passing IP, I think...
            network="host",
            volumes={
                "/home/tjg/logs": {
                    "bind": "/logs",
                    "mode": "rw",
                }
            },
            detach=True,
        )
        self.started = True

    def __enter__(self) -> DContainer:
        return self

    # TODO: Delete image somehow
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.started:
            self.model_container.stop()
            self.model_container.remove()


# Return timestamp object
def get_expected_input_ts(
    game_hash: GameHash, node: Node, node_by_name: Dict[NodeName, Node]
) -> timestamp_manager.InputTimestampManager:
    max_tss = list()
    for ind, d_node, gh in shared_logic.dependencies(game_hash, node, node_by_name):
        while ind >= len(max_tss):
            max_tss.append(0)
        if dependent_node_ts := lookups.timestamp_lookup(f_node).get(gh, None):
            max_tss[ind] = max(max_tss[ind], dependent_node_ts[1])
    return timestamp_manager.ts_manager_factory(max_tss)


@retrying.retry(
    wait_fixed=WAIT_FIXED_SECS * 1000,
    stop_max_attempt_number=NUM_RETRIES,
)
def run_feature(
    node: Node, node_by_name: Dict[NodeName, Node], container: DockerContainer
) -> None:
    tlogger.console_logger.info(f"Starting feature {node.name}...")
    channel = titanpublic.queuer.get_redis_channel()
    channel.queue_declare(node.name)

    # Clear out old messages, this is cleaner
    channel.queue_clear(TITAN_RECEIVER)
    channel.queue_clear(node.name)

    queued_games: Set[GameHash] = set()
    for game_hash in lookups.game_detail_lookup().keys():
        expected_input_ts = get_expected_input_ts(game_hash, node, node_by_name)
        actual_input_ts = lookups.timestamp_lookup(node).get(game_hash, (0, 0))[0]
        if actual_input_ts < expected_input_ts.max_ts():
            queued_games.add(game_hash)
            queuer.compose_queued_msg(
                channel,
                node,
                game_hash,
                expected_input_ts,
            )

    tlogger.console_logger.info(f"Queued up {len(queued_games)} games...")
    t = tqdm(total=len(queued_games))

    def mark_success(ch, method, properties, body):
        if "heartbeat" == body:
            # These are floating around because of previous server runs.
            return

        if len(body.split()) != 9:
            tlogger.log_both("Length error, should never happen", logging.ERROR)
            tlogger.log_both(body, logging.ERROR)

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
            tlogger.file_logger.debug(f"Skipping model {model}, expected {node.name}")
            return

        this_game_hash = titanpublic.hash.game_hash(away, home, date)
        if "success" == status:
            # print(str(len(queued_games)) + " " + node.name + " remaining")
            if this_game_hash in queued_games:
                # Tolerate resends
                queued_games.remove(this_game_hash)
                t.update(1)
        elif "failure" == status:
            # Requeue
            print(f"Retrying failed model: {body}")
            queuer.compose_queued_msg(
                channel,
                node,
                this_game_hash,
                timestamp_manager.SimpleTimesampWrapper(input_timestamp),
            )
        else:
            # Probably critical
            raise Exception(f"Got bad stuff: {body}")

    def still_waiting() -> bool:
        # print(str(len(queued_games)) + " " + node.name + " remaining (C)")
        return len(queued_games) > 0

    if not still_waiting():
        return

    container.start_container()

    # Wait here until we've gotten success messages for each game.
    channel.consume_while_condition(TITAN_RECEIVER, mark_success, still_waiting)

    t.close()


if __name__ == "__main__":
    sport = os.environ.get("SPORT")
    tlogger.console_logger.info(f"Running for sport = {sport}")
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

    # Build a single queue for everything
    titanpublic.queuer.get_redis_channel().queue_declare(TITAN_RECEIVER)

    # Run all the features in order, grouping together consecutive docker_image where
    #  possible.
    st, en = 0, 0
    while st < len(dag):
        while en < len(dag) and dag[en].docker_image == dag[st].docker_image:
            en += 1

        this_image = dag[st].docker_image
        if this_image is not None:
            with DockerContainer(this_image, lazy=True) as container:
                for i in range(st, en):
                    tlogger.console_logger.error(f"Running {dag[i].name}")
                    run_feature(dag[i], node_by_name, container)

        st = en
