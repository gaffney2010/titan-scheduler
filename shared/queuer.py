import logging
import os

from titanpublic import queuer as titan_queuer

from shared import lookups, timestamp_manager
from shared.shared_types import GameHash, Node


def compose_queued_msg(
    queue_channel: titan_queuer.QueueChannel,
    feature_node: Node,
    game_hash: GameHash,
    expected_input_ts: timestamp_manager.InputTimestampManager,
) -> None:
    game_details = lookups.game_detail_lookup()[game_hash]

    msg = " ".join(
        [
            os.environ["SPORT"],
            feature_node.name,
            expected_input_ts.print(),
            game_details.print(),
        ]
    )
    suffix = ""
    if feature_node.suffix_generator is not None:
        suffix = feature_node.suffix_generator(
            game_details.away,
            game_details.home,
            game_details.date,
        )

    assert feature_node.queue_id is not None
    queue_channel.basic_publish(msg, feature_node.queue_id, suffix)

    logging.debug(f"Rabbit queue :: {feature_node.queue_id} : {msg}")
