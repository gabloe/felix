"""Type stubs for the native module.

Kept by hand and checked against the binding by `tests/test_api_surface.py`,
so a signature cannot drift here without a test noticing.
"""

from types import TracebackType
from typing import AsyncIterator, Awaitable, Iterator, Literal, Sequence

__version__: str

AckMode = Literal["none", "per_message", "per_batch"]
StartPosition = Literal["latest", "earliest"] | int

class FelixError(Exception): ...
class ConnectionError(FelixError): ...
class AuthError(FelixError): ...
class NotFoundError(FelixError): ...
class CursorError(FelixError): ...

class Event:
    tenant_id: str
    namespace: str
    stream: str
    payload: bytes
    offset: int | None

class SubscriptionHandle:
    closed: bool
    def next_event(self, timeout: float | None = None) -> Event | None: ...
    def close(self) -> None: ...
    def __iter__(self) -> Iterator[Event]: ...
    def __next__(self) -> Event: ...
    def __enter__(self) -> "SubscriptionHandle": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class Client:
    def __init__(
        self,
        addrs: str | Sequence[str],
        *,
        tenant_id: str,
        token: str,
        server_name: str = "localhost",
        ca_file: str | None = None,
    ) -> None: ...
    def publish(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payload: bytes,
        *,
        ack: AckMode = "per_message",
        at_least_once: bool = False,
    ) -> None: ...
    def subscribe(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        *,
        start: StartPosition | None = None,
    ) -> SubscriptionHandle: ...
    def cache_put(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        key: str,
        value: bytes,
        *,
        ttl: float | None = None,
    ) -> None: ...
    def cache_get(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> bytes | None: ...
    def cache_delete(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> bytes | None: ...
    def counter_add(
        self, tenant_id: str, namespace: str, cache: str, key: str, delta: int
    ) -> int: ...
    def counter_get(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> int | None: ...
    def endpoints(self) -> list[str]: ...
    def __enter__(self) -> "Client": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class AsyncSubscription:
    """A subscription consumed with ``async for``."""

    @property
    def closed(self) -> Awaitable[bool]: ...
    async def next_event(self, timeout: float | None = None) -> Event | None: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[Event]: ...
    async def __anext__(self) -> Event: ...
    async def __aenter__(self) -> "AsyncSubscription": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class AsyncClient:
    """The asyncio surface. Construct with ``await AsyncClient.connect(...)``."""

    @staticmethod
    async def connect(
        addrs: str | Sequence[str],
        *,
        tenant_id: str,
        token: str,
        server_name: str = "localhost",
        ca_file: str | None = None,
    ) -> "AsyncClient": ...
    async def publish(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        payload: bytes,
        *,
        ack: AckMode = "per_message",
        at_least_once: bool = False,
    ) -> None: ...
    async def subscribe(
        self,
        tenant_id: str,
        namespace: str,
        stream: str,
        *,
        start: StartPosition | None = None,
    ) -> AsyncSubscription: ...
    async def cache_put(
        self,
        tenant_id: str,
        namespace: str,
        cache: str,
        key: str,
        value: bytes,
        *,
        ttl: float | None = None,
    ) -> None: ...
    async def cache_get(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> bytes | None: ...
    async def cache_delete(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> bytes | None: ...
    async def counter_add(
        self, tenant_id: str, namespace: str, cache: str, key: str, delta: int
    ) -> int: ...
    async def counter_get(
        self, tenant_id: str, namespace: str, cache: str, key: str
    ) -> int | None: ...
    async def endpoints(self) -> list[str]: ...
    async def __aenter__(self) -> "AsyncClient": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...
