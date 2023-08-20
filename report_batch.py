from typing import Dict, Set, Tuple

import colorama

from shared import lookups, shared_logic
from shared.shared_types import Date, GameHash, Node, NodeName


class ReportBatchException(Exception):
    pass


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
        color: str = colorama.Fore.RESET,
        new_line: bool = False,
    ) -> None:
        self._assert_open()
        if section not in self.body:
            self.body[section] = [[]]
            self.order.append(section)
        self.body[section][-1].append(color + text)
        if new_line:
            self.body[section].append([])


if __name__ == "__main__":
    colorama.init()

    sport = os.environ.get("SPORT")
    tlogger.console_logger.info(f"Logging for sport = {sport}")
    if "ncaam" == sport:
        from dags import ncaam

        dag = ncaam.graph

    def does_needed_refresh(game_hash: GameHash, node: Node) -> bool:
        nonlocal node_by_name
        actual_input_ts = lookups.timestamp_lookup(node).get(game_hash, (0, 0))[0]
        for _, d_node, gh in shared_logic.timestamp_lookup(
            game_hash, node, node_by_name
        ):
            if needs_refresh[(d_node.name, gh)]:
                return True
            dependent_node_ts = lookups.timestamp_lookup(d_node).get(gh, None)
            if dependent_node_ts:
                if dependent_node_ts[1] > actual_input_ts:
                    return True
        return False

    needs_refresh: Dict[Tuple[GameHash, NodeName], bool] = dict()
    for node in dag:
        for game_hash in lookups.game_detail_lookup().keys():
            needs_refresh[(game_hash, node.name)] = does_needs_refresh(game_hash, node)

    min_needs = Dict[Tuple[Date, NodeName], bool] = dict()
    max_needs = Dict[Tuple[Date, NodeName], bool] = dict()
    all_dates: Set[Date] = list()
    for k, v in needs_refresh:
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

    reporter = Reporter()
    reporter.open(report_path)
    for node in dag:
        report.write(node.name, node.name, new_line=True)
    color_guide = {
        (False, False): colorama.Fore.RED,
        (False, True): colorama.Fore.YELLOW,
        (True, True): colorama.Fore.GREEN,
    }
    for date in range(report_start_date, report_end_date):
        if date not in all_dates:
            continue
        for node in dag:
            a, z = min_needs(date, node.name), max_needs(date, node.name)
            reporter.write(node.name, "\u2588", color=color_guide[(a, z)])
    reporter.close()
