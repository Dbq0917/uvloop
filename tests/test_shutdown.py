import asyncio
import signal
import socket
import subprocess
import sys
import time

from uvloop import _testbase as tb
from uvloop.shutdown import GracefulServer, ShutdownState


class SlowEchoProto(asyncio.Protocol):
    """Echo protocol whose connections stay open until the peer leaves."""

    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)

    def connection_lost(self, exc):
        self.transport = None


class _TestGracefulShutdown:

    def _start_graceful_server(self, *, drain_timeout,
                               max_connections=None):
        loop = self.loop
        # A bound-but-not-listening server with tracking in place; start
        # serving only after the wrapper is fully wired up.
        graceful = loop.run_until_complete(
            GracefulServer.create(
                loop, SlowEchoProto, '127.0.0.1', 0,
                drain_timeout=drain_timeout,
                max_connections=max_connections))
        loop.run_until_complete(graceful.start_serving())
        return graceful, graceful.sockets[0].getsockname()

    def _raw_connect(self, addr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect(addr)
        except OSError:
            sock.close()
            raise
        return sock

    def _assert_rejected_immediately(self, sock):
        # An explicit rejection is observable right away: either an RST
        # (ConnectionResetError) or an orderly FIN (empty read).  What we
        # reject is the silent hang in which recv() blocks until the
        # client's own timeout.
        try:
            data = sock.recv(16)
        except ConnectionResetError:
            return
        self.assertEqual(data, b'')

    def test_shutdown_drains_in_flight_connections(self):
        graceful, addr = self._start_graceful_server(drain_timeout=5.0)

        client = self._raw_connect(addr)
        self.loop.run_until_complete(asyncio.sleep(0.1))
        self.assertEqual(graceful.active_connections, 1)

        # The peer leaves cleanly before the drain budget is used up.
        client.close()
        result = self.loop.run_until_complete(graceful.shutdown())

        self.assertEqual(graceful.state, ShutdownState.CLOSED)
        self.assertEqual(result.aborted, 0)
        self.assertEqual(result.drained, 1)
        self.assertFalse(result.timed_out)

    def test_shutdown_aborts_after_drain_timeout(self):
        graceful, addr = self._start_graceful_server(drain_timeout=0.2)

        client = self._raw_connect(addr)
        self.loop.run_until_complete(asyncio.sleep(0.1))

        started = time.monotonic()
        result = self.loop.run_until_complete(graceful.shutdown())
        elapsed = time.monotonic() - started

        # The shutdown is bounded; the stuck connection is force-aborted
        # and the client gets a definite, immediate close instead of a
        # silent hang.
        self.assertLess(elapsed, 2.0)
        self.assertEqual(result.aborted, 1)
        self.assertEqual(result.drained, 0)
        self.assertTrue(result.timed_out)

        self._assert_rejected_immediately(client)
        client.close()

    def test_shutdown_rejects_over_cap_connections(self):
        graceful, addr = self._start_graceful_server(
            drain_timeout=0.2, max_connections=1)

        first = self._raw_connect(addr)
        self.loop.run_until_complete(asyncio.sleep(0.1))
        self.assertEqual(graceful.active_connections, 1)

        # A second connection exceeds the cap and is rejected right away
        # instead of piling up in the accept backlog.
        second = self._raw_connect(addr)
        self.loop.run_until_complete(asyncio.sleep(0.1))
        self._assert_rejected_immediately(second)
        second.close()
        self.assertEqual(graceful.rejected_connections, 1)

        result = self.loop.run_until_complete(graceful.shutdown())
        self.assertEqual(result.aborted, 1)
        first.close()

    def test_shutdown_stops_accepting_first(self):
        graceful, addr = self._start_graceful_server(drain_timeout=5.0)

        client = self._raw_connect(addr)
        self.loop.run_until_complete(asyncio.sleep(0.1))

        async def shutdown_soon():
            task = asyncio.ensure_future(graceful.shutdown())
            await asyncio.sleep(0.1)
            # Draining: the listening socket is already closed, so a new
            # client is refused by the kernel rather than accepted into a
            # process that would never service it.
            new_client = None
            try:
                new_client = self._raw_connect(addr)
                self._assert_rejected_immediately(new_client)
            except ConnectionRefusedError:
                pass
            finally:
                if new_client is not None:
                    new_client.close()
            client.close()
            await asyncio.sleep(0)
            return await task

        result = self.loop.run_until_complete(shutdown_soon())
        self.assertEqual(result.drained, 1)
        self.assertEqual(result.aborted, 0)

    def test_shutdown_is_idempotent(self):
        graceful, addr = self._start_graceful_server(drain_timeout=0.2)

        client = self._raw_connect(addr)
        self.loop.run_until_complete(asyncio.sleep(0.1))

        first = self.loop.run_until_complete(graceful.shutdown())
        # A second call after completion is a no-op, and a concurrent
        # duplicate (e.g. a second signal) cannot start a second run.
        second = self.loop.run_until_complete(graceful.shutdown())
        self.assertEqual(second.aborted, 0)
        self.assertEqual(second.drained, 0)
        self.assertFalse(second.timed_out)
        self.assertEqual(first.aborted, 1)
        client.close()

    def test_concurrent_shutdown_is_rejected(self):
        graceful, addr = self._start_graceful_server(drain_timeout=0.3)

        client = self._raw_connect(addr)
        self.loop.run_until_complete(asyncio.sleep(0.1))

        async def race():
            task = asyncio.ensure_future(graceful.shutdown())
            await asyncio.sleep(0.05)
            with self.assertRaises(RuntimeError):
                await graceful.shutdown()
            result = await task
            return result

        result = self.loop.run_until_complete(race())
        self.assertEqual(result.aborted, 1)
        client.close()


class Test_UV_GracefulShutdown(_TestGracefulShutdown, tb.UVTestCase):
    pass


class Test_AIO_GracefulShutdown(_TestGracefulShutdown, tb.AIOTestCase):
    pass


class Test_UV_ShutdownSignal(tb.UVTestCase):

    def test_signal_shutdown_rejects_stuck_connections(self):
        stuck_clients = 8
        drain_timeout = 0.3

        prog = R"""\
import asyncio
import signal
import uvloop

from uvloop.shutdown import GracefulServer


class Proto(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)


async def main():
    loop = asyncio.get_running_loop()
    graceful = await GracefulServer.create(
        loop, Proto, '127.0.0.1', 0,
        drain_timeout=""" + repr(drain_timeout) + R""")
    await graceful.start_serving()
    port = graceful.sockets[0].getsockname()[1]
    print('READY', port, flush=True)

    loop.add_signal_handler(
        signal.SIGTERM,
        lambda: loop.create_task(graceful.shutdown()))

    while graceful.state.name == 'SERVING':
        await asyncio.sleep(0.05)
    while graceful.state.name != 'CLOSED':
        await asyncio.sleep(0.05)


loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main())
loop.close()
"""

        async def runner():
            proc = await asyncio.create_subprocess_exec(
                sys.executable, '-W', 'ignore', '-c', prog,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

            line = await proc.stdout.readline()
            port = int(line.decode().split()[1])
            addr = ('127.0.0.1', port)

            clients = []
            for _ in range(stuck_clients):
                sock = socket.socket(socket.AF_INET,
                                     socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect(addr)
                clients.append(sock)
            await asyncio.sleep(0.2)

            started = time.monotonic()
            proc.send_signal(signal.SIGTERM)

            rejected = 0
            for sock in clients:
                try:
                    data = sock.recv(16)
                except ConnectionResetError:
                    rejected += 1
                else:
                    # Orderly FIN is also an immediate, explicit
                    # rejection rather than a silent hang.
                    if data == b'':
                        rejected += 1
                finally:
                    sock.close()

            _, err = await proc.communicate()
            elapsed = time.monotonic() - started

            # The process exits within the bounded drain window, and
            # every stuck client learns its fate immediately instead of
            # hanging until its own read timeout.
            self.assertEqual(err, b'')
            self.assertLess(elapsed, drain_timeout * 10)
            self.assertEqual(rejected, stuck_clients)
            self.assertEqual(proc.returncode, 0)

        self.loop.run_until_complete(runner())
