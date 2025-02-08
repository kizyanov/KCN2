"""KCN2 trading bot for kucoin."""

import asyncio
from base64 import b64decode, b64encode
from decimal import Decimal
from hashlib import sha256
from hmac import HMAC
from hmac import new as hmac_new
from os import environ
from time import time
from typing import Any, Self
from urllib.parse import urljoin
from uuid import UUID, uuid4

from aiohttp import ClientConnectorError, ClientSession
from loguru import logger
from orjson import JSONDecodeError, JSONEncodeError, dumps, loads
from result import Err, Ok, Result, do, do_async
from websockets import ClientConnection, connect
from websockets import exceptions as websockets_exceptions


class Base:
    """Base class for all classes."""

    def logger_info[T](self: Self, data: T) -> Result[T, Exception]:
        """Info logger for Pipes."""
        logger.info(data)
        return Ok(data)


class Encrypt(Base):
    """All methods for encrypt data."""

    def cancatinate_str(self: Self, *args: str) -> Result[str, Exception]:
        """Cancatinate to str."""
        try:
            return Ok("".join(args))
        except TypeError as exc:
            logger.exception(exc)
            return Err(exc)

    def get_default_uuid4(self: Self) -> Result[UUID, Exception]:
        """Get default uuid4."""
        return Ok(uuid4())

    def format_to_str_uuid(self: Self, data: UUID) -> Result[str, Exception]:
        """Get str UUID4 and replace `-` symbol to spaces."""
        return do(
            Ok(result) for result in self.cancatinate_str(str(data).replace("-", ""))
        )

    def get_uuid4(self: Self) -> Result[str, Exception]:
        """Get uuid4 as str without `-` symbols.

        8e7c653b-7faf-47fe-b6d3-e87c277e138a -> 8e7c653b7faf47feb6d3e87c277e138a

        get_default_uuid4 -> format_to_str_uuid
        """
        return do(
            Ok(str_uuid)
            for default_uuid in self.get_default_uuid4()
            for str_uuid in self.format_to_str_uuid(default_uuid)
        )

    def convert_bytes_to_base64(self: Self, data: bytes) -> Result[bytes, Exception]:
        """Convert bytes to base64."""
        try:
            return Ok(b64encode(data))
        except TypeError as exc:
            logger.exception(exc)
            return Err(exc)

    def convert_base64_to_bytes(self: Self, data: bytes) -> Result[bytes, Exception]:
        """Convert base64 to bytes."""
        try:
            return Ok(b64decode(data))
        except TypeError as exc:
            logger.exception(exc)
            return Err(exc)

    def encode(self: Self, data: str) -> Result[bytes, Exception]:
        """Return Ok(bytes) from str data."""
        try:
            return Ok(data.encode())
        except AttributeError as exc:
            logger.exception(exc)
            return Err(exc)

    def decode(self: Self, data: bytes) -> Result[str, Exception]:
        """Return Ok(str) from bytes data."""
        try:
            return Ok(data.decode())
        except AttributeError as exc:
            logger.exception(exc)
            return Err(exc)

    def get_default_hmac(
        self: Self,
        secret: bytes,
        data: bytes,
    ) -> Result[HMAC, Exception]:
        """Get default HMAC."""
        return Ok(hmac_new(secret, data, sha256))

    def convert_hmac_to_digest(
        self: Self,
        hmac_object: HMAC,
    ) -> Result[bytes, Exception]:
        """Convert HMAC to digest."""
        return Ok(hmac_object.digest())

    def encrypt_data(self: Self, secret: bytes, data: bytes) -> Result[str, Exception]:
        """Encript `data` to hmac."""
        return do(
            Ok(result)
            for hmac_object in self.get_default_hmac(secret, data)
            for hmac_data in self.convert_hmac_to_digest(hmac_object)
            for base64_data in self.convert_bytes_to_base64(hmac_data)
            for result in self.decode(base64_data)
        )

    def dumps_dict_to_bytes(
        self: Self,
        data: dict[str, Any],
    ) -> Result[bytes, Exception]:
        """Dumps dict to bytes[json].

        {"qaz":"edc"} -> b'{"qaz":"wsx"}'
        """
        try:
            return Ok(dumps(data))
        except JSONEncodeError as exc:
            logger.exception(exc)
            return Err(exc)

    def parse_bytes_to_dict(
        self: Self,
        data: bytes,
    ) -> Result[dict[str, Any], Exception]:
        """Parse bytes[json] to dict.

        b'{"qaz":"wsx"}' -> {"qaz":"wsx"}
        """
        try:
            return Ok(loads(data))
        except JSONDecodeError as exc:
            logger.exception(exc)
            return Err(exc)


class Request(Encrypt):
    """All methods for http actions."""

    def get_env(self: Self, key: str) -> Result[str, ValueError]:
        """Just get key from EVN."""
        try:
            return Ok(environ[key])
        except ValueError as exc:
            logger.exception(exc)
            return Err(exc)

    def _env_convert_to_list(self: Self, data: str) -> Result[list[str], Exception]:
        """Split str by ',' character."""
        return Ok(data.split(","))

    def get_list_env(self: Self, key: str) -> Result[list[str], Exception]:
        """Get value from ENV in list[str] format.

        in .env
        KEYS=1,2,3,4,5,6

        to
        KEYS = ['1','2','3','4','5','6']
        """
        return do(
            Ok(value_in_list)
            for value_by_key in self.get_env(key)
            for value_in_list in self._env_convert_to_list(value_by_key)
        )

    def __init__(self: Self) -> None:
        """Init settings."""
        # All about excange
        self.KEY = self.get_env("KEY").unwrap()
        self.SECRET = self.get_env("SECRET").unwrap()
        self.PASSPHRASE = self.get_env("PASSPHRASE").unwrap()
        self.BASE_URL = self.get_env("BASE_URL").unwrap()

        # all about tokens
        self.ALL_CURRENCY = self.get_list_env("ALLCURRENCY").unwrap()
        self.BASE_KEEP = Decimal(self.get_env("BASE_KEEP").unwrap())

        # All about tlg
        self.TELEGRAM_BOT_API_KEY = self.get_env("TELEGRAM_BOT_API_KEY").unwrap()
        self.TELEGRAM_BOT_CHAT_ID = self.get_list_env("TELEGRAM_BOT_CHAT_ID").unwrap()

        logger.success("Settings are OK!")

    async def post_api_v1_margin_order(
        self: Self,
        data: dict[str, str],
    ) -> Result[dict[str, Any], Exception]:
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
            for data_to_sign in self.cancatinate_str(
                now_time,
                method,
                uri,
                dumps_data_str,
            )
            for headers in self.get_headers_auth(
                data_to_sign,
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

    async def get_api_v1_accounts(
        self: Self,
        params: dict[str, str],
    ) -> Result[dict[str, Any], Exception]:
        """Get account list with balance.

        https://www.kucoin.com/docs/rest/account/basic-info/get-account-list-spot-margin-trade_hf
        """
        uri = "/api/v1/accounts"
        method = "GET"
        return await do_async(
            Ok(checked_dict)
            for params_in_url in self.get_url_params_as_str(params)
            for uri_params in self.cancatinate_str(uri, params_in_url)
            for full_url in self.get_full_url(self.BASE_URL, uri_params)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri_params)
            for headers in self.get_headers_auth(
                data_to_sign,
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

    async def get_api_v1_orders(
        self: Self,
        params: dict[str, str],
    ) -> Result[dict[str, Any], Exception]:
        """Get all orders by params."""
        uri = "/api/v1/orders"
        method = "GET"
        return await do_async(
            Ok(checked_dict)
            for params_in_url in self.get_url_params_as_str(params)
            for uri_params in self.cancatinate_str(uri, params_in_url)
            for full_url in self.get_full_url(self.BASE_URL, uri_params)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri_params)
            for headers in self.get_headers_auth(
                data_to_sign,
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

    async def delete_api_v1_order(
        self: Self,
        order_id: str,
    ) -> Result[dict[str, list[str]], Exception]:
        """."""
        uri = f"/api/v1/orders/{order_id}"
        method = "DELETE"
        return await do_async(
            Ok(checked_dict)
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri)
            for headers in self.get_headers_auth(
                data_to_sign,
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

    async def get_api_v2_symbols(
        self: Self,
    ) -> Result[dict[str, Any], Exception]:
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
        params: dict[str, str],
    ) -> Result[dict[str, dict[str, str]], Exception]:
        """Get margin account user data."""
        uri = "/api/v3/margin/accounts"
        method = "GET"
        return await do_async(
            Ok(checked_dict)
            for params_in_url in self.get_url_params_as_str(params)
            for uri_params in self.cancatinate_str(uri, params_in_url)
            for full_url in self.get_full_url(self.BASE_URL, uri_params)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri_params)
            for headers in self.get_headers_auth(
                data_to_sign,
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

    async def get_api_v1_bullet_private(
        self: Self,
    ) -> Result[dict[str, str], Exception]:
        """Get tokens for private channel.

        https://www.kucoin.com/docs/websocket/basic-info/apply-connect-token/private-channels-authentication-request-required-
        """
        uri = "/api/v1/bullet-private"
        method = "POST"
        return await do_async(
            Ok(checked_dict)
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri)
            for headers in self.get_headers_auth(
                data_to_sign,
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

    async def get_api_v1_bullet_public(
        self: Self,
    ) -> Result[dict[str, dict[str, str]], Exception]:
        """Get public token for websocket.

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
        data: dict[str, Any],
    ) -> Result[str, Exception]:
        """Get endpoint for public websocket."""
        try:
            return Ok(data["data"]["instanceServers"][0]["endpoint"])
        except (KeyError, TypeError) as exc:
            return Err(Exception(f"Miss keys instanceServers in {exc} by {data}"))

    def export_token_from_api_v1_bullet_public(
        self: Self,
        data: dict[str, dict[str, str]],
    ) -> Result[str, Exception]:
        """Get token for public websocket."""
        try:
            return Ok(data["data"]["token"])
        except (KeyError, TypeError) as exc:
            return Err(Exception(f"Miss keys token in {exc} by {data}"))

    def get_url_params_as_str(
        self: Self,
        params: dict[str, str],
    ) -> Result[str, Exception]:
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
    ) -> Result[dict[str, str], Exception]:
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

    def get_headers_not_auth(self: Self) -> Result[dict[str, str], Exception]:
        """Get headers without encripted data for http request."""
        return Ok({"User-Agent": "kucoin-python-sdk/2"})

    def convert_to_int(self: Self, data: float) -> Result[int, Exception]:
        """Convert data to int."""
        try:
            return Ok(int(data))
        except ValueError as exc:
            logger.exception(exc)
            return Err(exc)

    def get_time(self: Self) -> Result[float, Exception]:
        """Get now time as float."""
        return Ok(time())

    def get_now_time(self: Self) -> Result[str, Exception]:
        """Get now time for encrypted data."""
        return do(
            Ok(f"{time_now_in_int*1000}")
            for time_now in self.get_time()
            for time_now_in_int in self.convert_to_int(time_now)
        )

    def check_response_code(
        self: Self,
        data: dict[str, Any],
    ) -> Result[dict[str, Any], Exception]:
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
        headers: dict[str, str],
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
                logger.info(f"{response.status}:{method}:{url}")
                return Ok(res)
        except ClientConnectorError as exc:
            logger.exception(exc)
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
        data: dict[str, str],
    ) -> Result[None, Exception]:
        """Check msg `welcome` from websocket connection.

        {
            "id": "hQvf8jkno",
            "type": "welcome"
        }
        """
        if "id" in data and "type" in data and data["type"] == "welcome":
            return Ok(None)
        return Err(Exception(f"Error parse welcome from websocket:{data}"))

    def get_tunnel_websocket(self: Self) -> Result[dict[str, str], Exception]:
        """."""
        return Ok(
            {
                "id": str(int(time() * 1000)),
                "type": "openTunnel",
                "newTunnelId": "all_klines2",
            },
        )

    def get_klines(self: Self) -> Result[dict[str, str | bool], Exception]:
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
        data: dict[str, Any],
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
        """When the connection on websocket is successfully established.

        the system will send a welcome message.

        {
            "id": "hQvf8jkno",
            "type": "welcome"
        }
        """
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
                logger.exception(exc)
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
            logger.exception(exc)
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
            logger.exception(exc)
            return Err(exc)


class KCN(Request, WebSocket):
    """Main class collect all logic."""

    def __init__(self) -> None:
        """Init parents."""
        super().__init__()

    def logger_info[T](self: Self, data: T) -> Result[T, Exception]:
        """Логгер уровня info для do и do_async функций."""
        logger.info(data)
        return Ok(data)

    def logger_exception[T](self: Self, data: T) -> Result[T, Exception]:
        """Логгер уровня exception для do и do_async функций."""
        logger.exception(data)
        return Ok(data)

    def logger_success[T](self: Self, data: T) -> Result[T, Exception]:
        """Логгер уровня success для do и do_async функций."""
        logger.success(data)
        return Ok(data)

    def create_book(self: Self) -> Result[None, Exception]:
        """Build own structure.

        build inside book for tickets

        {
            "ADA": {
                "balance": "",
                "increment": "",
                "sellorder": "",
                "buyorder": ""
            },
            "JUP": {
                "balance": "",
                "increment": "",
                "sellorder": "",
                "buyorder": ""
            },
            "SOL": {
                "balance": "",
                "increment": "",
                "sellorder": "",
                "buyorder": ""
            },
            "BTC": {
                "balance": "",
                "increment": "",
                "sellorder": "",
                "buyorder": ""
            }
        }
        """
        self.book: dict[str, dict[str, str]] = {
            ticket: {} for ticket in self.ALL_CURRENCY if isinstance(ticket, str)
        }
        return Ok(None)

    def export_account_usdt_from_api_v3_margin_accounts(
        self: Self,
        data: dict[str, Any],
    ) -> Result[dict[str, Any], Exception]:
        """."""
        try:
            for i in [i for i in data["data"]["accounts"] if i["currency"] == "USDT"]:
                return Ok(i)
            return Err(Exception("Not found USDT in accounts data"))
        except (AttributeError, KeyError) as exc:
            logger.exception(exc)
            return Err(exc)

    def export_liability_usdt(
        self: Self,
        data: dict[str, str],
    ) -> Result[Decimal, Exception]:
        """Export liability and available USDT from api_v3_margin_accounts."""
        return Ok(Decimal(data["liability"]))

    def export_available_usdt(
        self: Self,
        data: dict[str, str],
    ) -> Result[Decimal, Exception]:
        """Export liability and available USDT from api_v3_margin_accounts."""
        return Ok(Decimal(data["available"]))

    async def alertest(self: Self) -> Result[None, Exception]:
        """Alert statistic."""
        logger.info("alertest")
        await do_async(
            Ok(f"{liability=}:{available=}")
            for api_v3_margin_accounts in await self.get_api_v3_margin_accounts(
                params={
                    "quoteCurrency": "USDT",
                },
            )
            for _ in self.logger_info(api_v3_margin_accounts)
            for accounts_data in self.export_account_usdt_from_api_v3_margin_accounts(
                api_v3_margin_accounts,
            )
            for liability in self.export_liability_usdt(accounts_data)
            for available in self.export_available_usdt(accounts_data)
        )
        return Ok(None)

    async def massive_cancel_order(
        self: Self,
        data: list[str],
    ) -> Result[None, Exception]:
        """Cancel all order in data list."""
        for order_id in data:
            await self.delete_api_v1_order(order_id)
        return Ok(None)

    def export_order_id_from_orders_list(
        self: Self,
        orders: dict[str, list[dict[str, str]]],
    ) -> Result[list[str], Exception]:
        """Export id from orders list."""
        logger.info(f"{orders=}")
        return Ok([order["id"] for order in orders["data"]["items"]])

    async def balancer(self: Self) -> Result[None, Exception]:
        """Monitoring of balance.

        Get all active margin orders
        Cancel all active orders
        Get all balance
        Start listen websocket
        """
        logger.info("balancer")
        return Ok(None)

    async def matching(self: Self) -> Result[None, Exception]:
        """Monitoring of matching order."""
        logger.info("matching")
        return Ok(None)

    def _fill_balance(self: Self, data: dict) -> Result[None, Exception]:
        """."""
        for ticket in data["data"]:
            if ticket["currency"] in self.book:
                self.book[ticket["currency"]]["balance"] = Decimal(ticket["balance"])
        return Ok(None)

    async def fill_balance(self: Self) -> Result[None, Exception]:
        """."""
        return await do_async(
            Ok(None)
            for balance_accounts in await self.get_api_v1_accounts(
                params={"type": "margin"},
            )
            for _ in self._fill_balance(balance_accounts)
        )

        return Ok(None)

    def _fill_base_increment(self: Self, data: dict) -> Result[None, Exception]:
        """."""
        for out_side_ticket in data["data"]:
            base_currency = out_side_ticket["baseCurrency"]
            if (
                base_currency in self.book
                and out_side_ticket["quoteCurrency"] == "USDT"
            ):
                self.book[base_currency]["increment"] = Decimal(
                    out_side_ticket["baseIncrement"],
                )

    async def fill_base_increment(self: Self) -> Result[None, Exception]:
        """."""
        return await do_async(
            Ok(None)
            for ticket_info in await self.get_api_v2_symbols()
            for _ in self._fill_base_increment(ticket_info)
        )

        return Ok(None)

    async def pre_init(self: Self) -> Result[None, Exception]:
        """Pre-init.

        get all open orders
        close all open orders
        get balance by  all tickets
        get increment by all tickets
        """
        return await do_async(
            Ok(self)
            for _ in self.create_book()
            for orders_for_cancel in await self.get_api_v1_orders(
                params={
                    "status": "active",
                    "tradeType": "MARGIN_TRADE",
                },
            )
            for orders_list_str in self.export_order_id_from_orders_list(
                orders_for_cancel,
            )
            for _ in await self.massive_cancel_order(orders_list_str)
            for _ in await self.fill_balance()
            for _ in await self.fill_base_increment()
        )


# meow anton - baka des ^^


async def main() -> Result[None, Exception]:
    """Collect of major func."""
    match await KCN().pre_init():
        case Ok(kcn):
            kcn.logger_success("Pre-init OK!")
            # async with asyncio.TaskGroup() as tg:
            #     await tg.create_task(kcn.alertest())
            #     await tg.create_task(kcn.balancer())
            #     await tg.create_task(kcn.matching())
        case Err(exc):
            kcn.logger_exception(exc)
            return Err(exc)

    return Ok(None)


if __name__ == "__main__":
    """Main enter."""
    asyncio.run(main())
