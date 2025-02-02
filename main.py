"""KCN2 trading bot for kucoin."""

import asyncio
from base64 import b64encode
from decimal import Decimal
from hashlib import sha256
from hmac import new as hmac_new
from time import time
from typing import Self
from urllib.parse import urljoin
from uuid import uuid4

from aiohttp import ClientConnectorError, ClientSession
from decouple import Csv, config
from loguru import logger
from orjson import JSONDecodeError, JSONEncodeError, dumps, loads
from result import Err, Ok, Result, do, do_async
from websockets import ClientConnection, connect
from websockets import exceptions as websockets_exceptions


class Encrypt:
    """All methods for encrypt data."""

    def get_uuid4(self: Self) -> Result[str, Exception]:
        """Get uuid4 in str without - symbols.

        8e7c653b-7faf-47fe-b6d3-e87c277e138a -> 8e7c653b7faf47feb6d3e87c277e138a
        """
        return Ok(f"{str(uuid4()).replace('-', '')}")

    def base64encode(self: Self, data: bytes) -> Result[str, Exception]:
        """Convert `data` to base64."""
        try:
            return Ok(b64encode(data).decode())
        except TypeError as exc:
            return Err(exc)

    def encode(self: Self, data: str) -> Result[bytes, Exception]:
        """Return Ok(bytes) from str data."""
        try:
            return Ok(data.encode())
        except AttributeError as exc:
            return Err(exc)

    def decode(self: Self, data: bytes) -> Result[str, Exception]:
        """Return Ok(str) from bytes data."""
        try:
            return Ok(data.decode())
        except AttributeError as exc:
            return Err(exc)

    def hmac(self: Self, secret: bytes, data: bytes) -> Result[bytes, Exception]:
        """Convert `data` to hmac."""
        try:
            return Ok(hmac_new(secret, data, sha256).digest())
        except TypeError as exc:
            return Err(exc)

    def encrypt_data(self: Self, secret: bytes, data: bytes) -> Result[str, Exception]:
        """Encript `data` to hmac."""
        return do(
            Ok(b64data)
            for hmac in self.hmac(secret, data)
            for b64data in self.base64encode(hmac)
        )

    def dumps_dict_to_bytes(self: Self, data: dict) -> Result[bytes, Exception]:
        """Dumps dict to bytes[json].

        {"qaz":"edc"} -> b'{"qaz":"wsx"}'
        """
        try:
            return Ok(dumps(data))
        except JSONEncodeError as exc:
            return Err(exc)

    def parse_bytes_to_dict(self: Self, data: bytes) -> Result[dict, Exception]:
        """Parse bytes[json] to dict.

        b'{"qaz":"wsx"}' -> {"qaz":"wsx"}
        """
        try:
            return Ok(loads(data))
        except JSONDecodeError as exc:
            return Err(exc)


class Request(Encrypt):
    """All methods for http actions."""

    def __init__(self: Self) -> None:
        """Init settings."""
        # All about excange
        self.KEY = config("KEY", cast=str)
        self.SECRET = config("SECRET", cast=str)
        self.PASSPHRASE = config("PASSPHRASE", cast=str)
        self.BASE_URL = config(
            "BASE_URL",
            cast=str,
            default="https://api.kucoin.com",
        )

        # all about tokens
        self.ALL_CURRENCY = config("ALLCURRENCY", cast=Csv(str))
        self.BASE_KEEP = Decimal(config("BASE_KEEP", cast=int))

        # All about tlg
        self.TELEGRAM_BOT_API_KEY = config("TELEGRAM_BOT_API_KEY", cast=str)
        self.TELEGRAM_BOT_CHAT_ID = config("TELEGRAM_BOT_CHAT_ID", cast=Csv(str))

        logger.success("Settings are OK!")

    async def post_api_v1_margin_order(
        self: Self,
        data: dict,
    ) -> Result[dict, Exception]:
        """Make margin order.

        https://www.kucoin.com/docs/rest/margin-trading/orders/place-margin-order

        data =  {
            "clientOid": str(uuid4()).replace("-", ""),
            "side": side,
            "symbol": symbol,
            "price": price,
            "size": size,
            "type": "limit",
            "timeInForce": "GTC",
            "autoBorrow": True,
            "autoRepay": True,
        }

        """
        uri = "/api/v1/margin/order"
        method = "POST"
        return await do_async(
            Ok(checked_dict)
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for dumps_data_bytes in self.dumps_dict_to_bytes(data)
            for dumps_data_str in self.decode(dumps_data_bytes)
            for now_time in self.get_now_time()
            for headers in self.get_headers_auth(
                f"{now_time}{method}{uri}{dumps_data_str}",
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
                data=dumps_data_bytes,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for checked_dict in self.check_response_code(response_dict)
        )

    async def get_api_v1_accounts(self: Self, params: dict) -> Result[dict, Exception]:
        """Get account list with balance.

        https://www.kucoin.com/docs/rest/account/basic-info/get-account-list-spot-margin-trade_hf
        """
        uri = "/api/v1/accounts"
        method = "GET"
        return await do_async(
            Ok(checked_dict)
            for params_in_url in self.get_url_params_as_str(params)
            for full_url in self.get_full_url(self.BASE_URL, f"{uri}{params_in_url}")
            for now_time in self.get_now_time()
            for headers in self.get_headers_auth(
                f"{now_time}{method}{uri}{params_in_url}",
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for checked_dict in self.check_response_code(response_dict)
        )

    async def get_api_v2_symbols(self: Self) -> Result[dict, Exception]:
        """Get symbol list.

        https://www.kucoin.com/docs/rest/spot-trading/market-data/get-symbols-list
        """
        uri = "/api/v2/symbols"
        method = "GET"
        return await do_async(
            Ok(checked_dict)
            for headers in self.get_headers_not_auth()
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for checked_dict in self.check_response_code(response_dict)
        )

    async def get_api_v3_margin_accounts(
        self: Self,
        params: dict,
    ) -> Result[dict, Exception]:
        """Get margin account user data."""
        uri = "/api/v3/margin/accounts"
        method = "GET"
        return await do_async(
            Ok(checked_dict)
            for params_in_url in self.get_url_params_as_str(params)
            for full_url in self.get_full_url(self.BASE_URL, f"{uri}{params_in_url}")
            for now_time in self.get_now_time()
            for headers in self.get_headers_auth(
                f"{now_time}{method}{uri}{params_in_url}",
                now_time,
            )
            for headers in self.get_headers_auth(
                f"{now_time}{method}{uri}{params_in_url}",
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for checked_dict in self.check_response_code(response_dict)
        )

    async def get_api_v1_bullet_private(self: Self) -> Result[dict, Exception]:
        """Get tokens for private channel.

        https://www.kucoin.com/docs/websocket/basic-info/apply-connect-token/private-channels-authentication-request-required-
        """
        uri = "/api/v1/bullet-private"
        method = "POST"
        return await do_async(
            Ok(checked_dict)
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for now_time in self.get_now_time()
            for headers in self.get_headers_auth(
                f"{now_time}{method}{uri}",
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for checked_dict in self.check_response_code(response_dict)
        )

    async def get_api_v1_bullet_public(self: Self) -> Result[dict, Exception]:
        """Get public token.

        https://www.kucoin.com/docs/websocket/basic-info/apply-connect-token/public-token-no-authentication-required-
        """
        uri = "/api/v1/bullet-public"
        method = "POST"
        return await do_async(
            Ok(checked_dict)
            for headers in self.get_headers_not_auth()
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for checked_dict in self.check_response_code(response_dict)
        )

    async def get_public_url_for_websocket(self: Self) -> Result[str, Exception]:
        """."""
        return await do_async(
            Ok(f"{url}?token={token}&connectId={uuid_str}")
            for data in await self.get_api_v1_bullet_public()
            for token in self.export_token_from_api_v1_bullet_public(data)
            for url in self.export_url_from_api_v1_bullet_public(data)
            for uuid_str in self.get_uuid4()
        )

    def export_url_from_api_v1_bullet_public(
        self: Self,
        data: dict,
    ) -> Result[str, Exception]:
        """."""
        try:
            return Ok(data["data"]["instanceServers"][0]["endpoint"])
        except (KeyError, TypeError) as exc:
            return Err(Exception(f"Miss keys instanceServers in {exc} by {data}"))

    def export_token_from_api_v1_bullet_public(
        self: Self,
        data: dict,
    ) -> Result[str, Exception]:
        """."""
        try:
            return Ok(data["data"]["token"])
        except (KeyError, TypeError) as exc:
            return Err(Exception(f"Miss keys token in {exc} by {data}"))

    def get_url_params_as_str(self: Self, params: dict) -> Result[str, Exception]:
        """Get url params in str.

        if params is empty -> ''
        if params not empty -> ?foo=bar&zoo=net
        """
        params_in_url = "&".join([f"{key}={params[key]}" for key in sorted(params)])
        if len(params_in_url) == 0:
            return Ok("")
        return Ok("?" + params_in_url)

    def get_full_url(
        self: Self,
        base_url: str,
        next_url: str,
    ) -> Result[str, Exception]:
        """Right cancatinate base url and method url."""
        return Ok(urljoin(base_url, next_url))

    def get_headers_auth(
        self: Self,
        data: str,
        now_time: str,
    ) -> Result[dict, Exception]:
        """Get headers with encrypted data for http request."""
        return do(
            Ok(
                {
                    "KC-API-SIGN": kc_api_sign,
                    "KC-API-TIMESTAMP": now_time,
                    "KC-API-PASSPHRASE": kc_api_passphrase,
                    "KC-API-KEY": self.KEY,
                    "Content-Type": "application/json",
                    "KC-API-KEY-VERSION": "2",
                    "User-Agent": "kucoin-python-sdk/2",
                },
            )
            for secret in self.encode(self.SECRET)
            for passphrase in self.encode(self.PASSPHRASE)
            for data_in_bytes in self.encode(data)
            for kc_api_sign in self.encrypt_data(secret, data_in_bytes)
            for kc_api_passphrase in self.encrypt_data(secret, passphrase)
        )

    def get_headers_not_auth(self: Self) -> Result[dict, Exception]:
        """Get headers without encripted data for http request."""
        return Ok({"User-Agent": "kucoin-python-sdk/2"})

    def get_now_time(self: Self) -> Result[str, Exception]:
        """Get now time for encrypted data."""
        return Ok(f"{int(time()) * 1000}")

    def check_response_code(self: Self, data: dict) -> Result[dict, Exception]:
        """Check if key `code`.

        If key `code` in dict == '200000' then success
        """
        if isinstance(data, dict) and "code" in data and data["code"] == "200000":
            return Ok(data)
        return Err(Exception(data))

    async def request(
        self: Self,
        url: str,
        method: str,
        headers: dict,
        data: bytes | None = None,
    ) -> Result[bytes, Exception]:
        """Base http request."""
        try:
            async with (
                ClientSession(
                    headers=headers,
                ) as session,
                session.request(
                    method,
                    url,
                    data=data,
                ) as response,
            ):
                res = await response.read()  # bytes
                return Ok(res)
        except ClientConnectorError as exc:
            return Err(exc)


class WebSocket(Encrypt):
    """Class for working with websockets."""

    def get_websocket(self: Self, url: str) -> Result[connect, Exception]:
        """Get connect for working with websocket by url."""
        return Ok(
            connect(
                url,
                max_queue=1024,
            ),
        )

    def check_welcome_msg_from_websocket(
        self: Self,
        data: dict,
    ) -> Result[str, Exception]:
        """."""
        match data:
            case {"id": _, "type": "welcome"}:
                return Ok("welcome")
            case _:
                return Err(Exception(f"Error parse welcome from websocket:{data}"))

    def get_tunnel_websocket(self: Self) -> Result[dict, Exception]:
        """."""
        return Ok(
            {
                "id": str(int(time() * 1000)),
                "type": "openTunnel",
                "newTunnelId": "all_klines2",
            },
        )

    def get_klines(self: Self) -> Result[dict, Exception]:
        """."""
        return Ok(
            {
                "id": str(int(time() * 1000)),
                "type": "subscribe",
                "topic": "/market/candles:BTC-USDT_1hour",
                "privateChannel": False,
                "tunnelId": "all_klines2",
            },
        )

    async def send_data_to_ws(
        self: Self,
        ws: ClientConnection,
        data: dict,
    ) -> Result[None, Exception]:
        """."""
        return await do_async(
            Ok(response)
            for data_bytes in self.dumps_dict_to_bytes(data)
            for response in await self.send_data_to_websocket(ws, data_bytes)
        )

    async def welcome_processing_websocket(
        self: Self,
        dd: ClientConnection,
    ) -> Result[bytes, Exception]:
        """."""
        return await do_async(
            Ok(_)
            for welcome_data_websocket in await self.recv_data_from_websocket(dd)
            for welcome in self.parse_bytes_to_dict(welcome_data_websocket)
            for _ in self.check_welcome_msg_from_websocket(welcome)
        )

    async def runtime_ws(self: Self, ws: connect) -> Result[str, Exception]:
        """Runtime listen websocket all time."""
        async for s in ws:
            try:
                await do_async(
                    Ok("aa")
                    for _ in await self.welcome_processing_websocket(s)
                    for tunnel_dict in self.get_tunnel_websocket()
                    for _ in await self.send_data_to_ws(s, tunnel_dict)
                    for klines_dict in self.get_klines()
                    for _ in await self.send_data_to_ws(s, klines_dict)
                    for asd in await self.recv_data_from_websocket(s)
                )
            except websockets_exceptions.ConnectionClosed as exc:
                return Err(exc)
        return Ok("")

    async def recv_data_from_websocket(
        self: Self,
        ws: ClientConnection,
    ) -> Result[bytes, Exception]:
        """."""
        try:
            res = await ws.recv()
            if isinstance(res, bytes):
                return Ok(res)
            return Err(Exception(f"Bad data type:{res}"))

        except (
            websockets_exceptions.ConnectionClosed,
            websockets_exceptions.ConcurrencyError,
        ) as exc:
            return Err(exc)

    async def send_data_to_websocket(
        self: Self,
        ws: ClientConnection,
        data: bytes,
    ) -> Result[None, Exception]:
        """."""
        try:
            await ws.send(data, text=True)
            return Ok(None)
        except (
            websockets_exceptions.ConnectionClosed,
            websockets_exceptions.ConcurrencyError,
            TypeError,
        ) as exc:
            return Err(exc)


class KCN(Request, WebSocket):
    """Main class collect all logic."""

    def __init__(self) -> None:
        """Init parents."""
        super().__init__()

    def create_book(self: Self) -> Result[None, Exception]:
        """Build own structure.

        build inside book for tickets

        "TICKET":{}
        """
        self.book: dict[str, dict] = {ticket: {} for ticket in self.ALL_CURRENCY}
        return Ok(None)

    def export_baseincrement_from_symbols(
        self: Self,
        data: dict,
    ) -> Result[dict, Exception]:
        """Export from get_api_v2_symbols baseIncrement by baseCurrency."""
        return Ok({d["baseCurrency"]: d["baseIncrement"] for d in data["data"]})

    async def alertest(self: Self) -> Result[None, Exception]:
        """Alert statistic."""
        logger.info("alertest")
        t = await self.get_api_v3_margin_accounts(
            params={
                "quoteCurrency": "USDT",
            },
        )
        logger.info(t)
        await asyncio.sleep(1000)
        return Ok(None)

    async def balancer(self: Self) -> Result[None, Exception]:
        """Monitoring of balance."""
        logger.info("balancer")
        return Ok(None)

    async def matching(self: Self) -> Result[None, Exception]:
        """Monitoring of matching order."""
        logger.info("matching")
        return Ok(None)


async def main() -> Result[None, Exception]:
    """Collect of major func."""
    kcn = KCN()
    kcn.create_book()
    async with asyncio.TaskGroup() as tg:
        await tg.create_task(kcn.alertest())
        await tg.create_task(kcn.balancer())
        await tg.create_task(kcn.matching())

    return Ok(None)


if __name__ == "__main__":
    """Main enter."""
    asyncio.run(main())
