import argparse
from enum import Enum
import os
from typing import Dict, Set, Tuple

from shared import lookups, shared_logic
from shared.shared_types import Date, GameHash, Node, NodeName


class ReportBatchException(Exception):
    pass


class TjColor(Enum):
    DEFAULT = 0
    GREEN = 1
    YELLOW = 2
    RED = 3


def tj_color(c: TjColor) -> str:
    if TjColor.DEFAULT == c:
        return "\033[0m"
    if TjColor.RED == c:
        return "\033[31m"
    if TjColor.YELLOW == c:
        return "\033[33m"
    if TjColor.GREEN == c:
        return "\033[32m"
    raise ReportBatchException("I don't even, what kinda color...")


class Reporter(object):
    def __init__(self):
        # User is responsible for opening and closing.
        self.opened = False

    def _assert_open(self) -> None:
        if not self.opened:
            raise ReportBatchException("Reporter hasn't been opened")

    def open(self, filename: str) -> None:
        self.body = dict()
        self.order = list()
        # We actually don't open the file until we close everything will stay open until then.
        self.filename = filename
        self.opened = True

    def close(self) -> None:
        self._assert_open()
        with open(self.filename, "w") as f:
            for section in self.order:
                for line in self.body[section]:
                    f.write("".join(line) + "\n")
        self.open(None)  # Cheap flush
        self.opened = False

    def write(
        self,
        section: str,
        text: str,
        color: TjColor = TjColor.DEFAULT,
        new_line: bool = False,
    ) -> None:
        self._assert_open()
        if section not in self.body:
            self.body[section] = [[]]
            self.order.append(section)
        self.body[section][-1].append(tj_color(color) + text)
        if new_line:
            self.body[section].append([])


if __name__ == "__main__":
    sport = os.environ.get("SPORT", "ncaam")
    if "ncaam" == sport:
        from dags import ncaam

        dag = ncaam.graph

    node_by_name = {node.name: node for node in dag}

    def does_needs_refresh(game_hash: GameHash, node: Node, needs_refresh: Dict[Tuple[GameHash, NodeName], bool]) -> bool:
        actual_input_ts = lookups.timestamp_lookup(node).get(game_hash, (0, 0))[0]
        for _, d_node, gh in shared_logic.dependencies(
            game_hash, node, node_by_name
        ):
            if needs_refresh[(gh, d_node.name)]:
                return True
            dependent_node_ts = lookups.timestamp_lookup(d_node).get(gh, None)
            if dependent_node_ts:
                if dependent_node_ts[1] > actual_input_ts:
                    return True
        return False

    print("Main work loop...")
    needs_refresh: Dict[Tuple[GameHash, NodeName], bool] = dict()
    for node in dag:
        print(f"   Node: {node.name}")
        for game_hash in lookups.game_detail_lookup().keys():
            needs_refresh[(game_hash, node.name)] = does_needs_refresh(game_hash, node, needs_refresh)
    print("Main work loop complete.")

    min_needs: Dict[Tuple[Date, NodeName], bool] = dict()
    max_needs: Dict[Tuple[Date, NodeName], bool] = dict()
    all_dates: Set[Date] = set()
    for k, v in needs_refresh.items():
        gh, nn = k
        d = lookups.game_detail_lookup()[gh].date
        all_dates.add(d)
        new_k = (d, nn)
        if not new_k in min_needs:
            min_needs[new_k] = True
        if not new_k in max_needs:
            max_needs[new_k] = False
        min_needs[new_k] &= v
        max_needs[new_k] |= v

    print("Some post work complete, writing report")

    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="Path to write report to")
    parser.add_argument("--s", type=int, help="Start date YYYYMMDD")
    parser.add_argument("--e", type=int, help="End date YYYYMMDD")
    args = parser.parse_args()
    report_start_date = args.s
    report_end_date = args.e
    report_path = args.path

    reporter = Reporter()
    reporter.open(report_path)
    reporter.write("Header", f"Run from {report_start_date} to {report_end_date}")
    for node in dag:
        reporter.write(node.name, node.name, new_line=True)
    color_guide = {
        (False, False): TjColor.GREEN,
        (False, True): TjColor.YELLOW,
        (True, True): TjColor.RED,
    }
    for date in range(report_start_date, report_end_date):
        if date not in all_dates:
            continue
        for node in dag:
            a, z = min_needs[(date, node.name)], max_needs[(date, node.name)]
            reporter.write(node.name, "\u2588", color=color_guide[(a, z)])
    reporter.close()

    print("Thank you.")

