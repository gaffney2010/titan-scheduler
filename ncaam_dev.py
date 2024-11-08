# I need to hack this here, I'm sorry
import os

os.environ["TITAN_ENV"] = "dev"
os.environ["SPORT"] = "ncaam"

from earthlings.titan_logging import titan_logger
import logging
import os

level = logging.DEBUG if "dev" == os.environ.get("TITAN_ENV", "dev") else logging.INFO
tlogger = titan_logger("batch_scheduler", False, file_level=level, console_level=level)

#########################
# Everything above this line must be set before other imports, because logging is dumb.

from datetime import datetime, timedelta
from typing import Any, Dict, Set

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import titanpublic
from tqdm import tqdm

from dags import ncaam
from shared import lookups, shared_logic, timestamp_manager, queuer
from shared.shared_types import Date, GameHash, Node, NodeName


TITAN_RECEIVER = "titan-receiver"


# Return timestamp object
def get_expected_input_ts(
    game_hash: GameHash, node: Node, node_by_name: Dict[NodeName, Node]
) -> timestamp_manager.InputTimestampManager:
    max_tss = list()
    # This unpacks already
    for ind, d_node, gh in shared_logic.dependencies(game_hash, node, node_by_name):
        while ind >= len(max_tss):
            max_tss.append(0)
        dependent_node_ts = lookups.timestamp_lookup(d_node).get(gh, None)
        if dependent_node_ts:
            max_tss[ind] = max(max_tss[ind], dependent_node_ts[1])
    return timestamp_manager.ts_manager_factory(max_tss)


def run_feature(node: Node, node_by_name: Dict[NodeName, Node], for_date: Date) -> None:
    if not node.docker_image:
        tlogger.console_logger.info(f"Nothing to run for {node.name}")
        return

    tlogger.console_logger.info(f"Starting feature {node.name} on date {for_date}...")
    channel = titanpublic.queuer.get_redis_channel()
    channel.queue_declare(node.name)

    queued_games: Set[GameHash] = set()
    for game_hash in lookups.game_detail_lookup().keys():
        if game_hash not in lookups.game_hash_lookup()[for_date]:
            continue
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

    if not queued_games:
        tlogger.console_logger.info(f"No games to queue")
        return

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
        t.close()
        return

    # Wait here until we've gotten success messages for each game.
    channel.consume_while_condition(TITAN_RECEIVER, mark_success, still_waiting)

    t.close()


# Define your DAG
airflow_dag = DAG(
    "ncaam_dev",
    schedule_interval="@daily",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=32,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)


dag = ncaam.graph
titanpublic.queuer.get_redis_channel().queue_declare(TITAN_RECEIVER)
node_by_name = {node.name: node for node in dag}

daily_nodes = dict()
cumu_nodes = dict()

for node in dag:

    def run_this_node(**kwargs):
        date_string = kwargs["ds"]
        date_obj = datetime.strptime(date_string, "%Y-%m-%d")
        ds = int(date_obj.strftime("%Y%m%d"))
        run_feature(node, node_by_name, ds)

    daily_nodes[node.name] = PythonOperator(
        task_id=f"daily_{node.name}",
        python_callable=run_this_node,
        provide_context=True,
        depends_on_past=False,
        dag=airflow_dag,
    )

    cumu_nodes[node.name] = DummyOperator(
        task_id=f"cumu_{node.name}",
        depends_on_past=True,
        dag=airflow_dag,
    )

    daily_nodes[node.name] >> cumu_nodes[node.name]

    for node_names, _, depends_on_past in node.dependencies:
        for node_name in node_names:
            if depends_on_past:
                cumu_nodes[node_name] >> daily_nodes[node.name]
            else:
                daily_nodes[node_name] >> daily_nodes[node.name]


if __name__ == "__main__":
    airflow_dag.test()

