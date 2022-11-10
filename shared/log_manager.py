import collections
import functools
import json
import logging
import os
import time
from typing import DefaultDict, Dict
import warnings

import shared.rabbit as rabbit
from shared.shared_types import (
    Feature,
    GameHash,
    LOG_EVERY_N_SECONDS,
    LOG_EVERY_N_UPDATES,
    LOG_PERTRIHECTATESSARACONTATERT,
    State,
)


class LogManager(object):
    def __init__(self):
        self.counter = 0
        self.pad: DefaultDict[Feature, Dict[GameHash, str]] = collections.defaultdict(
            dict
        )
        self.ts = time.monotonic()
        self.log_deprecated_warning_issued = False

    def free_log(self, free: str) -> None:
        sport = os.environ.get("SPORT")
        dev_suffix = ""
        if "dev" == os.environ.get("TITAN_ENV", "dev"):
            dev_suffix = "-dev"

        rabbit.get_rabbit_channel().basic_publish(
            f"{sport}{dev_suffix}:: {free}", "", "titan-log"
        )

    def log_state(self, feature_name: str, game_hash: GameHash, state: State) -> None:
        if "dev" == os.environ.get("TITAN_ENV", "dev"):
            logging.info(f"{feature_name} {game_hash} {state}")

        if game_hash % 343 < LOG_PERTRIHECTATESSARACONTATERT:
            # Log some portion of state updates for special updating.
            self.free_log(f"Sample state update: {feature_name} {game_hash} {state}")

        self.pad[feature_name][game_hash] = str(state)
        self.counter += 1

        counter_check = self.counter >= LOG_EVERY_N_UPDATES
        time_check = time.monotonic() - self.ts > LOG_EVERY_N_SECONDS
        if counter_check or time_check:
            self.counter = 0
            self.ts = time.monotonic()
            self.flush()

    def log(self, feature_name: str, game_hash: GameHash, state: State) -> None:
        if not self.log_deprecated_warning_issued:
            warnings.warn("LogManager's log endpoint is deprecated.  Use log_state.")
            self.log_deprecated_warning_issued = True
        self.log_state(feature_name, game_hash, state)

    def flush(self) -> None:
        # Non-defaultdict (I think) dumps to json easier
        summary: Dict[str, Dict[str, int]] = dict()
        for k, v in self.pad.items():
            for _, vv in v.items():
                if vv not in summary:
                    summary[vv] = dict()
                summary[vv][k] = 1 + summary[vv].get(k, 0)

        self.free_log(json.dumps(summary))


@functools.lru_cache(None)
def log_manager_singleton() -> LogManager:
    return LogManager()
