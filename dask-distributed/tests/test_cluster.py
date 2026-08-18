import socket

import pytest

from ocs_stack_dask_distributed import ClusterSession, connect, scheduler_reachable, wait_for_scheduler
from ocs_stack_dask_distributed.cluster import _split_address


class TestSplitAddress:
    def test_with_scheme(self):
        assert _split_address("tcp://127.0.0.1:8786") == ("127.0.0.1", 8786)

    def test_without_scheme(self):
        assert _split_address("scheduler:8786") == ("scheduler", 8786)

    @pytest.mark.parametrize("address", ["tcp://127.0.0.1", "no-port", "tcp://host:abc", ""])
    def test_rejects_malformed(self, address: str):
        with pytest.raises(ValueError, match="tcp://host:port"):
            _split_address(address)


class TestSchedulerReachable:
    def test_false_on_closed_port(self):
        # Port 1 is reserved and never has a dask scheduler on it.
        assert scheduler_reachable("tcp://127.0.0.1:1", timeout=0.25) is False

    def test_false_on_malformed_address(self):
        assert scheduler_reachable("not-an-address", timeout=0.25) is False

    def test_true_when_something_listens(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            sock.listen(1)
            port = sock.getsockname()[1]
            assert scheduler_reachable(f"tcp://127.0.0.1:{port}", timeout=1.0) is True


class TestWaitForScheduler:
    def test_times_out_when_nothing_answers(self):
        # A zero timeout means the loop body never runs, so this is the
        # give-up path on its own, without a wall-clock wait.
        with pytest.raises(TimeoutError, match="no scheduler answered"):
            wait_for_scheduler("tcp://127.0.0.1:1", timeout=0.0)


class TestConnectWithoutFallback:
    def test_refuses_to_substitute_a_local_cluster(self):
        # allow_fallback=False is for examples that only mean something
        # against real containers; they must fail loudly, not quietly run
        # in-process and look like they passed.
        with pytest.raises(ConnectionError, match="make up"):
            connect("tcp://127.0.0.1:1", allow_fallback=False)


class TestClusterSession:
    def test_compose_banner_names_the_address(self):
        session = ClusterSession(client=None, mode="compose", address="tcp://1.2.3.4:8786")
        assert session.is_compose is True
        assert "tcp://1.2.3.4:8786" in session.banner()

    def test_local_banner_explains_the_fallback(self):
        session = ClusterSession(client=None, mode="local", address="inproc://local")
        assert session.is_compose is False
        assert "make up" in session.banner()
