import asyncio
import enum
import signal
import socket
import typing


__all__ = (
    'GracefulServer',
    'ShutdownResult',
    'ShutdownState',
    'install_signal_handlers',
    'serve',
)

_ProtocolFactory = typing.Callable[[], asyncio.Protocol]
_OpenCallback = typing.Callable[
    [asyncio.Transport, '_ConnectionTracker'], None]
_CloseCallback = typing.Callable[['_ConnectionTracker'], None]


class ShutdownState(enum.Enum):
    """Lifecycle states of a :class:`GracefulServer`."""

    SERVING = 'serving'
    DRAINING = 'draining'
    FORCE_CLOSING = 'force-closing'
    CLOSED = 'closed'


class ShutdownResult(typing.NamedTuple):
    """Outcome of a graceful-shutdown sequence.

    ``drained`` counts in-flight connections that finished on their own
    within the drain budget.  ``aborted`` counts connections still open
    when the budget was exhausted; they are aborted explicitly so the
    peer observes an immediate close (an RST or a FIN) instead of being
    left to hang until its own timeout.
    """

    drained: int
    aborted: int
    timed_out: bool

    @property
    def total(self) -> int:
        return self.drained + self.aborted


class _ConnectionTracker(asyncio.Protocol):
    """Protocol wrapper that tracks the server's in-flight connections."""

    def __init__(self, app_protocol: asyncio.Protocol,
                 on_open: _OpenCallback,
                 on_close: _CloseCallback) -> None:
        self._app_protocol = app_protocol
        self._on_open = on_open
        self._on_close = on_close
        self._transport: typing.Optional[asyncio.Transport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = typing.cast(asyncio.Transport, transport)
        self._on_open(self._transport, self)
        self._app_protocol.connection_made(transport)

    def connection_lost(self, exc: typing.Optional[Exception]) -> None:
        try:
            self._app_protocol.connection_lost(exc)
        finally:
            self._on_close(self)

    def __getattr__(self, name: str) -> typing.Any:
        return getattr(self._app_protocol, name)


class GracefulServer:
    """Wrap an ``asyncio.Server`` with an ordered, bounded shutdown.

    The shutdown sequence fixes the "event loop stops first, sockets die
    later" ordering in which kernel-accepted connections end up
    unserviced and the peer only notices after its own timeout:

    1. Stop accepting new connections first (``server.close()``), so the
       kernel stops queueing new clients for this process.
    2. Drain in-flight connections, bounded by ``drain_timeout``.
    3. Abort every connection still open when the budget is exhausted,
       giving the peer an immediate, definite close instead of a
       silent hang.

    The event loop keeps running for the whole sequence, so there is no
    window where an accepted connection has nobody to service it.
    """

    def __init__(self, server: asyncio.AbstractServer, *,
                 drain_timeout: float = 5.0,
                 max_connections: typing.Optional[int] = None) -> None:
        if drain_timeout < 0:
            raise ValueError('drain_timeout must be non-negative')
        if max_connections is not None and max_connections <= 0:
            raise ValueError('max_connections must be a positive integer')

        self._server = server
        self._init(drain_timeout=drain_timeout,
                   max_connections=max_connections)

    def _init(self, *, drain_timeout: float,
              max_connections: typing.Optional[int]) -> None:
        self._drain_timeout = drain_timeout
        self._max_connections = max_connections

        self._connections: typing.Set[_ConnectionTracker] = set()
        self._state: ShutdownState = ShutdownState.SERVING
        self._drain_event: typing.Optional[asyncio.Event] = None
        self._aborted_count = 0
        self._rejected_count = 0

    @classmethod
    async def create(
        cls,
        loop: asyncio.AbstractEventLoop,
        protocol_factory: _ProtocolFactory,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        *,
        drain_timeout: float = 5.0,
        max_connections: typing.Optional[int] = None,
        **server_kwargs: typing.Any,
    ) -> 'GracefulServer':
        """Create a bound, tracked server that is not listening yet.

        The returned wrapper owns an ``asyncio.Server`` created with
        ``start_serving=False`` and a connection-tracking wrapper around
        ``protocol_factory``; call :meth:`start_serving` once the caller
        is ready to accept.
        """
        wrapper = cls.__new__(cls)
        wrapper._init(drain_timeout=drain_timeout,
                      max_connections=max_connections)
        raw_server = await loop.create_server(
            wrapper.protocol_factory(protocol_factory), host, port,
            start_serving=False, **server_kwargs)
        wrapper._server = raw_server
        return wrapper

    async def start_serving(self) -> None:
        await self._server.start_serving()

    @property
    def state(self) -> ShutdownState:
        return self._state

    @property
    def active_connections(self) -> int:
        return len(self._connections)

    @property
    def rejected_connections(self) -> int:
        """Number of connections refused outright (cap or draining)."""
        return self._rejected_count

    @property
    def sockets(self) -> typing.Tuple[socket.socket, ...]:
        return typing.cast(
            typing.Tuple[socket.socket, ...],
            self._server.sockets)  # type: ignore[attr-defined]

    def is_serving(self) -> bool:
        return self._state == ShutdownState.SERVING

    @staticmethod
    def tracking_protocol_factory(
        app_factory: _ProtocolFactory,
        on_open: _OpenCallback,
        on_close: _CloseCallback,
    ) -> _ProtocolFactory:
        """Wrap a protocol factory with open/close tracking callbacks."""

        def factory() -> asyncio.Protocol:
            return _ConnectionTracker(app_factory(), on_open, on_close)

        return factory

    def protocol_factory(self, app_factory: _ProtocolFactory) -> (
            _ProtocolFactory):
        """Wrap a protocol factory with connection tracking.

        Pass the result as the ``protocol_factory`` argument of
        ``loop.create_server()``.
        """
        return self.tracking_protocol_factory(
            app_factory, self._connection_opened, self._connection_closed)

    def _connection_opened(self, transport: asyncio.Transport,
                           tracker: _ConnectionTracker) -> None:
        if self._state != ShutdownState.SERVING:
            # Draining has already begun: refuse outright instead of
            # admitting work nobody will finish.
            self._rejected_count += 1
            transport.abort()
            return

        if (self._max_connections is not None
                and len(self._connections) >= self._max_connections):
            # Over the concurrency cap: reject explicitly rather than
            # buffering an unbounded number of half-open connections.
            self._rejected_count += 1
            transport.abort()
            return

        self._connections.add(tracker)

    def _connection_closed(self, tracker: _ConnectionTracker) -> None:
        if tracker in self._connections:
            self._connections.discard(tracker)
            if not self._connections and self._drain_event is not None:
                self._drain_event.set()

    async def shutdown(self) -> ShutdownResult:
        """Run the stop-accept, drain, force-abort sequence."""
        if self._state in (ShutdownState.DRAINING,
                           ShutdownState.FORCE_CLOSING):
            raise RuntimeError('shutdown() is already in progress')
        if self._state == ShutdownState.CLOSED:
            return ShutdownResult(drained=0, aborted=0, timed_out=False)

        total = len(self._connections)

        # 1. Stop accepting first.  Existing connections keep running and
        #    the event loop stays up to service them.
        self._state = ShutdownState.DRAINING
        self._server.close()

        # 2. Bounded drain of in-flight connections.
        self._drain_event = asyncio.Event()
        if not self._connections:
            self._drain_event.set()

        timed_out = False
        if self._drain_timeout > 0:
            try:
                await asyncio.wait_for(
                    self._drain_event.wait(),
                    timeout=self._drain_timeout)
            except asyncio.TimeoutError:
                timed_out = True
        else:
            timed_out = bool(self._connections)

        # 3. Explicitly reject whatever survived the drain window, so the
        #    peer observes an immediate close rather than a silent hang.
        remaining = list(self._connections)
        self._aborted_count = 0
        for tracker in remaining:
            transport = tracker._transport
            if transport is not None:
                self._aborted_count += 1
                transport.abort()
        self._state = ShutdownState.FORCE_CLOSING

        # Let the aborts propagate through connection_lost callbacks.
        for _ in range(2):
            if not self._connections:
                break
            await asyncio.sleep(0)

        # Now that every tracked connection is gone, wait_closed() can
        # complete on every loop implementation (some asyncio builds
        # make wait_closed() wait for active client connections too).
        await self._server.wait_closed()

        aborted = self._aborted_count
        drained = total - aborted
        self._state = ShutdownState.CLOSED
        self._drain_event = None
        return ShutdownResult(
            drained=drained,
            aborted=aborted,
            timed_out=timed_out or aborted > 0)

    async def __aenter__(self) -> 'GracefulServer':
        return self

    async def __aexit__(self, *exc: typing.Any) -> None:
        await self.shutdown()


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    graceful: GracefulServer,
    signals: typing.Iterable[int] = (signal.SIGTERM, signal.SIGINT),
) -> typing.Callable[[], None]:
    """Wire process signals to the graceful shutdown sequence.

    The signal handler only schedules
    :meth:`GracefulServer.shutdown`; the ordering (stop accepting,
    drain, abort) lives in that coroutine instead of being hard-coded
    inside the handler.  Returns a callable that removes the handlers.
    """
    installed: typing.List[int] = []

    def trigger() -> None:
        if graceful.state == ShutdownState.SERVING:
            loop.create_task(graceful.shutdown())

    for sig in signals:
        try:
            loop.add_signal_handler(sig, trigger)
        except NotImplementedError:
            continue
        installed.append(sig)

    def remove() -> None:
        for sig in installed:
            loop.remove_signal_handler(sig)

    return remove


async def serve(
    protocol_factory: _ProtocolFactory,
    host: typing.Optional[str] = None,
    port: typing.Optional[int] = None,
    *,
    drain_timeout: float = 5.0,
    max_connections: typing.Optional[int] = None,
    signals: typing.Iterable[int] = (signal.SIGTERM, signal.SIGINT),
    **server_kwargs: typing.Any,
) -> ShutdownResult:
    """Create a server and serve until a shutdown signal is received.

    Runs the wrapped server until one of ``signals`` arrives, then
    performs the bounded graceful shutdown and returns its
    :class:`ShutdownResult`.
    """
    loop = asyncio.get_running_loop()
    graceful = await GracefulServer.create(
        loop, protocol_factory, host, port,
        drain_timeout=drain_timeout,
        max_connections=max_connections,
        **server_kwargs)
    await graceful.start_serving()

    shutdown_triggered = asyncio.Event()

    def trigger() -> None:
        if graceful.state == ShutdownState.SERVING:
            shutdown_triggered.set()

    installed: typing.List[int] = []
    for sig in signals:
        try:
            loop.add_signal_handler(sig, trigger)
        except NotImplementedError:
            continue
        installed.append(sig)

    try:
        await shutdown_triggered.wait()
        result = await graceful.shutdown()
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)

    return result
