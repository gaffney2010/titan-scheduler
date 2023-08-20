import os
from typing import Iterator, List, Optional, Sequence, Tuple

import shared.log_manager as log_manager
import shared.rabbit as rabbit
from shared import shared_logic, timestamp_manager
from shared.shared_types import (
    GameHash,
    MatNodeName,
    Node,
    NodeName,
    State,
    Timestamp,
)
from titan_server import expirations, node_type

# TODO: Make a np.int16 as a space optimization
# Where in the list of incoming nodes
IncomingIndex = int


class AbstractNode(node_type.AbstractBaseNode):
    def __init__(self, name: NodeName):
        # This must be universally unique
        self.name: MatNodeName = shared_logic.myhash(name)
        self.state: Optional[State] = None  # 1B
        # outgoing's unusual setup is a trade-off between time and space efficiency...
        self._outgoing: List[Tuple["AbstractNode", IncomingIndex]] = list()  # 8B per
        if "dev" == os.environ.get("TITAN_ENV", "dev"):
            # Nice for debugging.
            self.incoming: List["AbstractNode"] = list()  # 8B per
        self.blocking: List[bool] = list()  # Should be in same order as incoming
        # self.expected_input_ts needs to be set elsewhere to an InputTimestampManager
        self._expected_input_ts: Optional[timestamp_manager.InputTimestampManager] = (
            None  # 2B
        )
        self.actual_input_ts: Timestamp = 0  # 2B
        self.output_ts: Timestamp = 0  # 2B
        self.last_queued_ts: Optional[Timestamp] = None
        self.expiration = None
        self.current_heartbeat = False

    @property
    def outgoing(self) -> Iterator[Tuple["AbstractNode", IncomingIndex]]:
        for n, ni in self._outgoing:
            yield (n, ni)

    @property
    def expected_input_ts(self) -> timestamp_manager.InputTimestampManager:
        assert self._expected_input_ts is not None
        return self._expected_input_ts

    @expected_input_ts.setter
    def expected_input_ts(self, rhs: timestamp_manager.InputTimestampManager) -> None:
        self._expected_input_ts = rhs

    def send_rabbit_msg(self) -> None:
        raise NotImplementedError()

    def log(self, _: State) -> None:
        raise NotImplementedError()

    def add_backlink(self, node: "AbstractNode") -> None:
        ind = len(self.blocking)
        self.blocking.append(False)
        if "dev" == os.environ.get("TITAN_ENV", "dev"):
            for n in self.incoming:
                assert n.name != node.name
            self.incoming.append(node)
        node._outgoing.append((self, ind))

    def should_retry(self) -> bool:
        # I may change this later.  For now, always retry transient errors
        return True

    def set_state(self, state: State) -> None:
        self.current_heartbeat = True

        if State.QUEUED == self.state:
            self.last_queued_ts = self.expected_input_ts.max_ts()
        else:
            # Probably redundant but extra safety.
            self.last_queued_ts = None

        if state != self.state:
            # Logging is weirdly expensive, so don't do unless we have to.
            self.log(state)
        self.state = state

    def queue_up(self) -> None:
        # The success case, may happen on first queue as well.
        if self.actual_input_ts >= self.expected_input_ts.max_ts():
            if State.READY != self.state:
                # Check is for efficiency
                self.set_state(State.READY)
                for n, ni in self.outgoing:
                    n.unblock(self, ni)
            return

        is_queued = State.QUEUED == self.state
        later_queued = False
        if self.last_queued_ts is not None:
            later_queued = self.last_queued_ts >= self.expected_input_ts.max_ts()
        if is_queued and later_queued:
            # Don't requeue
            return

        # Needs to run with `send_rabbit_msg`
        self.set_state(State.QUEUED)
        for n, ni in self.outgoing:
            n.block(ni)
        self.send_rabbit_msg()

    def block(
        self,
        blocking_ind: IncomingIndex,
    ) -> None:
        """
        If queued, block.  A success message will come in, but we'll stay blocked.
        If blocked, add new blockers.  But stay blocked.
        If ready, this is the happy path.  Add blocker and propagate.
        If failed, let this be blocked.  Let the unblock message run this instead of
            the heartbeat.
        """
        if State.CRITICAL == self.state:
            return

        # Update timestamps always
        self.blocking[blocking_ind] = True
        # Let unblock advance timestamps.

        # Propagate blocks if first time being blocked
        # When already block, timestamps don't get propagated, so the unblock will do
        #  this.
        if State.BLOCKED != self.state:
            self.set_state(State.BLOCKED)
            for n, ni in self.outgoing:
                n.block(ni)

    def unblock(
        self, blocking_node: "AbstractNode", blocking_ind: IncomingIndex
    ) -> None:
        """
        If queued, assert that this is a no-op.  Things should only be queued if there
            are no blockers.
        If blocked, this is the happy path; see if everything has unblocked and
            continue.
        If ready, assert that this is a no-op.  Things should only be ready if there
            are no blockers.
        If failed, this should never happen.  Blocked node no-op on failures.  And
            blocks overwrite failures.
        """
        if State.CRITICAL == self.state:
            return
        if self.state in (State.QUEUED, State.READY):
            assert not any(self.blocking)
            return
        assert not State.FAILED == self.state

        # Propagate timestamps
        if "dev" == os.environ.get("TITAN_ENV", "dev"):
            # Multiple unblocks shouldn't happen in a dev env.
            assert self.blocking[blocking_ind]
        self.blocking[blocking_ind] = False
        self.expected_input_ts.advance_ts(blocking_node)

        # Ready to finally go to work?
        if not any(self.blocking):
            self.queue_up()

    def succeed(self, input_ts: Timestamp, output_ts: Timestamp) -> None:
        """
        If queued, this is the happy path; it has finished.
        If blocked, wait for the unblock to come through.
        If ready, a second sucess has come through.  Update timestamp, and let queue_up
            do nothing.
        If failed, probably we thought this timed out, but it didn't.  Let queue_up
            override the failed state.
        """

        if self.state in (State.CRITICAL, State.BLOCKED):
            return

        if input_ts < self.expected_input_ts.max_ts():
            # This is old signal, updating here is only gonna fuck things up.
            return

        # Update timestamps
        self.actual_input_ts = max(self.actual_input_ts, input_ts)
        self.output_ts = max(self.output_ts, output_ts)

        # TODO: Is this redundant?
        # Always need to propagate timestamps.
        for n, _ in self.outgoing:
            n.expected_input_ts.advance_ts(self)

        if self.expiration:
            expirations.rm_expiration(self)
            self.expiration = None

        # This handles the success or keeps trying if a timestamp race.
        self.queue_up()

    def fail(self, input_ts: Optional[int], allow_fail_on_ready: bool = False) -> None:
        """
        If queued, this is the happy path.  Set state to failure and block downstream.
            Heartbeats will rerun these.
        If blocked, wait for the unblock to come through.
        If ready, this is probably an old message that came in late.  Keep the W.
            Something else will queue if that's needed.
        If failed, multiple failures came through.  There's nothing more to do.
        """
        if self.state in (State.CRITICAL, State.BLOCKED, State.FAILED):
            return

        if input_ts and input_ts < self.expected_input_ts.max_ts():
            # This is old signal, updating here is only gonna fuck things up.
            return

        if State.READY == self.state and not allow_fail_on_ready:
            return

        if not self.should_retry():
            self.critical()

        if self.expiration:
            expirations.rm_expiration(self)
            self.expiration = None

        # Heartbeat will come through and retry these.
        self.set_state(State.FAILED)
        for n, ni in self.outgoing:
            n.block(ni)

    def critical(self) -> None:
        self.set_state(State.CRITICAL)
        for n, _ in self.outgoing:
            n.critical()


class MaterializedNode(AbstractNode):
    def __init__(self, name: NodeName, game_hash: GameHash, feature_node: Node):
        self.game_hash = game_hash
        self.feature_node = feature_node
        self.type = node_type.NodeType.CONCRETE
        super().__init__(name)

    def send_rabbit_msg(self) -> None:
        rabbit.compose_rabbit_msg(
            self.feature_node, self.game_hash, self.expected_input_ts
        )
        self.expiration = expirations.add_expiration(self)

    def log(self, state: State) -> None:
        log_manager.log_manager_singleton().log(
            self.feature_node.name, self.game_hash, state
        )


class PseudoNode(AbstractNode):
    def __init__(self, name: str, node_group: Sequence[AbstractNode]):
        # self.node_group = frozenset([n.name for n in node_group])
        super().__init__(f"PseudoNode {name}")
        self.type = node_type.NodeType.PSEUDO
        for n in node_group:
            self.add_backlink(n)

    def send_rabbit_msg(self) -> None:
        # Just immediately succeed these.
        self.succeed(self.expected_input_ts.max_ts(), self.expected_input_ts.max_ts())

    def log(self, state: State) -> None:
        """Helpful for debugging"""
        log_manager.log_manager_singleton().log("pseudo", self.name, state)
