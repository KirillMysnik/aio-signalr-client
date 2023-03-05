import asyncio
import logging
from typing import Any

from aiohttp import ClientSession
from aiohttp import ClientWebSocketResponse
from aiohttp.typedefs import DEFAULT_JSON_DECODER
from aiohttp.typedefs import DEFAULT_JSON_ENCODER
from aiohttp.typedefs import JSONDecoder
from aiohttp.typedefs import JSONEncoder
from aiohttp.typedefs import StrOrURL

from aio_signalr_client.exceptions import SignalRNegotiationError
from aio_signalr_client.types import CompletionMessage
from aio_signalr_client.types import MessageType
from aio_signalr_client.types import InvocationMessage
from aio_signalr_client.types import SignalRMessage
from aio_signalr_client.types import UnknownMessage


PROTOCOL_VERSION = 1
_SEPARATOR = '\x1e'

logger = logging.getLogger("SignalR")


class SignalRJSONClient:
    def __init__(
        self,
        session: ClientSession,
        *,
        loads: JSONDecoder = DEFAULT_JSON_DECODER,
        dumps: JSONEncoder = DEFAULT_JSON_ENCODER,
    ):

        self._session: ClientSession = session
        self._loads = loads
        self._dumps = dumps

        self._invocation_id: int = 0
        self._ws: ClientWebSocketResponse | None = None

        self._closed: bool = False
        self._connected: bool = False
        self._ping_message: str = dumps({'type': MessageType.PING}) + _SEPARATOR
        self._ping_task: asyncio.Task | None = None
        self._ev_ping: asyncio.Event = asyncio.Event()
        self._recv_task: asyncio.Task | None = None
        self._recv_queue: asyncio.Queue[SignalRMessage | None] = asyncio.Queue()
        self._invoc_futs: dict[str, asyncio.Future] = {}

        self._con_token: str | None = None
        self._con_id: str | None = None

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def connected(self) -> bool:
        return self._connected

    async def _ws_send(self, data: list[Any]) -> None:
        payloads = list(map(self._dumps, data))
        for payload in payloads:
            logger.debug(f"OUT {payload}")

        await self._ws.send_str(_SEPARATOR.join(payloads + ['']))

    async def _ws_receive(self) -> list[Any]:
        try:
            text = await self._ws.receive_str()
        except TypeError as e:
            logger.debug(f"Terminating connection because underlying WebSocket transport failed to receive: {e}")
            await self.close()
            return []

        payloads = text.split(_SEPARATOR)[:-1]

        for payload in payloads:
            logger.debug(f"IN {payload}")

        data = list(map(self._loads, payloads))
        return data

    async def _negotiate(self, url: StrOrURL, **kwargs) -> None:
        logger.debug(f"Negotiating at {url}...")

        async with self._session.post(
            url.rstrip('/') + f'/negotiate?negotiateVersion={PROTOCOL_VERSION}',
            **kwargs,
        ) as resp:
            obj = await resp.json()

        con_token, con_id, version = obj['connectionToken'], obj['connectionId'], obj['negotiateVersion']

        if version != PROTOCOL_VERSION:
            logger.error(f"Server reports unsupported protocol version: {version}")
            raise SignalRNegotiationError(f"Unsupported server protocol version: {version}")

        # TODO: Raise if websockets is not an available transport

        logger.debug(f"Negotiation successful, connectionToken={con_token}, connectionId={con_id}")

        self._con_token = con_token
        self._con_id = con_id

    async def _handshake(self, version: int, protocol: str = 'json') -> None:
        logger.debug(f"Handshaking for protocol version {version} ({protocol})...")
        await self._ws_send([{
            'protocol': protocol,
            'version': version,
        }])

    async def connect(
            self,
            url: StrOrURL,
            *,
            negotiate: bool = True,
            **kwargs: Any
    ) -> None:

        if negotiate:
            await self._negotiate(url, **kwargs)
        else:
            logger.debug("Negotiation skipped")

        if self._con_token is not None:
            params = {'id': self._con_token}
        else:
            logger.debug(f"Connecting to {url} without `id`...")
            params = None

        self._ws = await self._session.ws_connect(url, params=params, **kwargs)
        logger.debug("WebSocket transport established")

        await self._handshake(PROTOCOL_VERSION, 'json')

        # Receive response to our handshake
        await self._recv_once()

        self._ping_task = asyncio.create_task(self._ping_loop())
        self._recv_task = asyncio.create_task(self._recv_loop())

        self._connected = True
        logger.debug(f"Connected, listening to messages.")

    async def invoke(self, target: str, arguments: list[Any]) -> str:
        invocation_id = str(self._invocation_id)
        try:
            message = InvocationMessage(target=target, arguments=arguments, invocation_id=invocation_id)
            await self._ws_send([message.to_dict()])

        finally:
            self._invocation_id += 1

        return invocation_id

    async def invoke_and_wait(self, target: str, arguments: list[Any]) -> CompletionMessage:
        invocation_id: str = await self.invoke(target, arguments)
        assert invocation_id not in self._invoc_futs

        fut = self._invoc_futs[invocation_id] = asyncio.get_event_loop().create_future()
        try:
            return await fut

        finally:
            if invocation_id in self._invoc_futs:
                del self._invoc_futs[invocation_id]

    async def _ping_once(self) -> None:
        await self._ws.send_str(self._ping_message)

    async def _ping_loop(self) -> None:
        try:
            while not self._closed:
                await self._ev_ping.wait()
                self._ev_ping.clear()

                if not self._closed:
                    await self._ping_once()

        except Exception:
            logger.exception("Unhandled exception in _ping_loop! Connection will close.")
            raise

        finally:
            await self.close()

    async def _recv_once(self) -> None:
        datas = await self._ws_receive()

        for data in datas:
            if data is None:
                message = None
                await self.close()

            else:
                if 'type' not in data:
                    continue

                message_type = MessageType(data['type'])
                if message_type == MessageType.PING:
                    self._ev_ping.set()
                    continue

                if message_type == MessageType.INVOCATION:
                    message = InvocationMessage.from_dict(data)

                elif message_type == MessageType.COMPLETION:
                    message = CompletionMessage.from_dict(data)

                    fut = self._invoc_futs.pop(message.invocation_id, None)
                    if fut is not None:
                        fut.set_result(message)
                        continue

                else:
                    if message_type == MessageType.CLOSE:
                        await self.close()

                    message = UnknownMessage.from_dict(data)

            self._recv_queue.put_nowait(message)

    async def _recv_loop(self) -> None:
        try:
            while not self._closed:
                await self._recv_once()

        except Exception:
            logger.exception("Unhandled exception in _recv_loop! Connection will close.")
            raise

        finally:
            await self.close()

    async def close(self) -> None:
        self._connected = False

        if self._closed:
            return

        try:
            if self._ping_task is not None:
                self._ping_task.cancel()

        finally:
            self._ping_task = None

            try:
                if self._recv_task is not None:
                    self._recv_task.cancel()

            finally:
                self._recv_task = None

                try:
                    if self._ws is not None:
                        try:
                            await self._ws.close()
                        finally:
                            self._ws = None

                finally:
                    self._closed = True

                    # Unblock anybody awaiting `receive`
                    if self._recv_queue.empty():
                        self._recv_queue.put_nowait(None)

    async def receive(self) -> SignalRMessage | None:
        if self._closed and self._recv_queue.empty():
            return None

        return await self._recv_queue.get()
