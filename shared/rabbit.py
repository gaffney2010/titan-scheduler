import functools
import logging
import os
import ssl
import time
from typing import Callable, Set

import pika
import retrying
import titanpublic

from shared import lookups, timestamp_manager
from shared.shared_types import GameHash, Node

PREFETCH_COUNT = 100  # Minibatch size
ROLLOVER_WAIT_SEC = 3  # How long to wait before restarting on a Rabbit timeout
BIGGER_WAIT_SEC = 240  # How long to wait if the Rabbit node is down.

RETRIES = 1 if "dev" == os.environ.get("TITAN_ENV", "dev") else None


class RabbitChannel(object):
    def __init__(self):
        self.sport = os.environ.get("SPORT")
        self.env = os.environ.get("TITAN_ENV", "dev")

        self.all_queues: Set[str] = set()
        self.built_queues: Set[str] = set()
        self.all_exchanges: Set[str] = set()
        self.built_exchanges: Set[str] = set()
        self.build_channel()

        self.queue_declare("titan-receiver")
        self.queue_declare("titan-log")

    @retrying.retry(
        wait_fixed=BIGGER_WAIT_SEC * 1000,
        stop_max_attempt_number=RETRIES,
    )
    def build_channel(self) -> None:
        """This sets self.channel"""
        logging.error("Starting pika connection")

        # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")

        rabbitmq_user = titanpublic.shared_logic.get_secrets(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
        )["rabbitmq_user"]
        rabbitmq_password = titanpublic.shared_logic.get_secrets(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
        )["rabbitmq_password"]
        rabbitmq_broker_id = titanpublic.shared_logic.get_secrets(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
        )["rabbitmq_broker_id"]

        url = (
            f"amqps://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_broker_id}.mq."
            "us-east-2.amazonaws.com:5671"
        )
        parameters = pika.URLParameters(url)
        parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        parameters.connection_attempts = 3
        parameters.heartbeat = 600

        connection = pika.BlockingConnection(parameters)
        self._channel = connection.channel()

        # Rebuild the queues
        self.built_queues = set()
        for queue_id in self.all_queues:
            self.queue_declare(queue_id)

        # Rebuild the exchanges
        self.built_exchanges = set()
        for exchange_id in self.all_exchanges:
            self.exchange_declare(exchange_id)

    def _queue_declare(self, queue_id: str) -> None:
        if not queue_id:
            # Handle a special edge case, so that we don't have to handle outside of
            #  class.
            return

        if queue_id in self.built_queues:
            # This has already been built.
            return

        self._channel.queue_declare(
            queue=titanpublic.pod_helpers.routing_key_resolver(
                queue_id, self.sport, self.env
            )
        )
        self.all_queues.add(queue_id)
        self.built_queues.add(queue_id)

    def queue_declare(self, queue_id: str) -> None:
        if "prod" != self.env:
            # Don't restart on failure
            self._queue_declare(queue_id)
            return

        try:
            self._queue_declare(queue_id)
        except pika.AMQPError:
            self.build_channel()
            self._queue_declare(queue_id)

    def _exchange_declare(self, exchange_id: str) -> None:
        if not exchange_id:
            # Handle a special edge case, so that we don't have to handle outside of
            #  class.
            return

        if exchange_id in self.built_exchanges:
            # This has already been built.
            return

        self._channel.exchange_declare(exchange=exchange_id, exchange_type="direct")
        self.all_exchanges.add(exchange_id)
        self.built_exchanges.add(exchange_id)

    def exchange_declare(self, exchange_id: str) -> None:
        if "prod" != self.env:
            # Don't restart on failure
            self._exchange_declare(exchange_id)
            return

        try:
            self._exchange_declare(exchange_id)
        except pika.AMQPError:
            self.build_channel()
            self._exchange_declare(exchange_id)

    def _basic_publish(
        self, msg: str, exchange_id: str, queue_id: str, suffix: str
    ) -> None:
        exchange = titanpublic.pod_helpers.exchange_resolver(
            exchange_id, self.sport, self.env, suffixes=suffix
        )
        routing_key = titanpublic.pod_helpers.routing_key_resolver(
            queue_id, self.sport, self.env, suffix=suffix
        )

        self._channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=msg,
            properties=pika.BasicProperties(delivery_mode=1),
        )

    def basic_publish(
        self, msg: str, exchange_id: str, queue_id: str, suffix: str = ""
    ) -> None:
        try:
            self._basic_publish(msg, exchange_id, queue_id, suffix)
        except pika.AMQPError:
            self.build_channel()
            self._basic_publish(msg, exchange_id, queue_id, suffix)

    def _consume_while_condition(self, callback: Callable, condition: Callable) -> None:
        if not condition():
            return
        self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        for method, properties, body in self._channel.consume(
            queue=titanpublic.pod_helpers.routing_key_resolver(
                "titan-receiver", self.sport, self.env
            ),
            auto_ack=True,
            inactivity_timeout=300,
        ):
            if method is None and properties is None and body is None:
                # This is the timeout condition
                logging.debug("Pika timeout")
                return
            if condition():
                callback(None, method, properties, body)
            else:
                return

    def consume_while_condition(self, callback: Callable, condition: Callable) -> None:
        if "prod" != self.env:
            # Don't restart on failure, this runs until error
            self._consume_while_condition(callback, condition)
            return

        try:
            self._consume_while_condition(callback, condition)
        except pika.exceptions.AMQPError:
            logging.error("Pika exception")
            time.sleep(ROLLOVER_WAIT_SEC)
            self.build_channel()
            self.consume_while_condition(callback, condition)

    def _consume_to_death(self, callback: Callable) -> None:
        self._consume_while_condition(callback, lambda: True)

    def consume_to_death(self, callback: Callable) -> None:
        if "prod" != self.env:
            # Don't restart on failure, this runs until error
            self._consume_to_death(callback)
            return

        try:
            self._consume_to_death(callback)
        except pika.AMQPError:
            time.sleep(ROLLOVER_WAIT_SEC)
            self.build_channel()
            self.consume_to_death(callback)


@functools.lru_cache(1)
def get_rabbit_channel() -> RabbitChannel:
    """Creates a singleton"""
    return RabbitChannel()


def compose_rabbit_msg(
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
    get_rabbit_channel().basic_publish(
        msg, feature_node.queue_id, feature_node.queue_id, suffix
    )

    logging.info(f"Rabbit queue :: {feature_node.queue_id} : {msg}")
