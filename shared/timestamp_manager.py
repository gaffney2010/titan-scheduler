from typing import Dict, List

from shared.shared_types import MatNodeName, Timestamp
from titan_server import node_type


class InputTimestampManager(object):
    def __init__(self, nodes: List[List[MatNodeName]]):
        self.anonymous = False
        self.ts = list()
        self.class_lookup: Dict[MatNodeName, int] = dict()
        # Maintaining order matters here
        for i, node_class in enumerate(nodes):
            self.ts.append(0)
            for node_name in node_class:
                self.class_lookup[node_name] = i

    def advance_ts(self, node: node_type.AbstractBaseNode) -> None:
        assert not self.anonymous
        self.ts[self.class_lookup[node.name]] = max(
            self.ts[self.class_lookup[node.name]], node.output_ts
        )

    def max_ts(self) -> Timestamp:
        return max(self.ts)

    def print(self) -> str:
        return ",".join([str(ts) for ts in self.ts])


def ts_manager_factory(tss: List[Timestamp]) -> InputTimestampManager:
    """Creates anonymous classes"""
    result = InputTimestampManager([])
    result.ts = tss
    result.anonymous = True
    return result


class EmptyInputTimestampManager(InputTimestampManager):
    def __init__(self):
        pass

    def advance_ts(self, _: node_type.AbstractBaseNode) -> None:
        raise Exception("Empty timestamp managers should never be advanced")

    def max_ts(self) -> Timestamp:
        return 0

    def print(self) -> str:
        return "0"
