"""KCN2 trading bot for kucoin."""

import asyncio
from base64 import b64encode
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from hashlib import sha256
from hmac import HMAC
from hmac import new as hmac_new
from os import environ
from time import time
from typing import Any, Self
from urllib.parse import urljoin
from uuid import UUID, uuid4

from aiohttp import ClientConnectorError, ClientSession
from dacite import (
    ForwardReferenceError,
    MissingValueError,
    StrictUnionMatchError,
    UnexpectedDataError,
    UnionMatchError,
    WrongTypeError,
    from_dict,
)
from loguru import logger
from orjson import JSONDecodeError, JSONEncodeError, dumps, loads
from result import Err, Ok, Result, do, do_async
from websockets import ClientConnection, connect
from websockets import exceptions as websockets_exceptions


@dataclass(frozen=True)
class OrderParam:
    """."""

    side: str = field(default="")
    price: str = field(default="")
    size: str = field(default="")


@dataclass(frozen=True)
class TelegramSendMsg:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        ok: bool = field(default=False)


@dataclass(frozen=True)
class ApiV1MarketAllTickers:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            @dataclass(frozen=True)
            class Ticker:
                """."""

                symbol: str = field(default="")
                last: str = field(default="")

            ticker: list[Ticker] = field(default_factory=list[Ticker])

        data: Data = field(default_factory=Data)
        code: str = field(default="")


@dataclass(frozen=True)
class ApiV1MarginOrderPOST:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        code: str = field(default="")
        orderId: str = field(default="")


@dataclass(frozen=True)
class ApiV2SymbolsGET:
    """https://www.kucoin.com/docs/rest/spot-trading/market-data/get-symbols-list."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            baseCurrency: str = field(default="")
            quoteCurrency: str = field(default="")
            baseIncrement: str = field(default="")

        data: list[Data] = field(default_factory=list[Data])
        code: str = field(default="")


@dataclass(frozen=True)
class ApiV1OrdersDELETE:
    """https://www.kucoin.com/docs/rest/spot-trading/orders/cancel-order-by-orderid."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        code: str = field(default="")


@dataclass(frozen=True)
class ApiV1OrdersGET:
    """https://www.kucoin.com/docs/rest/spot-trading/orders/get-order-list."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            @dataclass(frozen=True)
            class Item:
                """."""

                id: str = field(default="")

            items: list[Item] = field(default_factory=list[Item])

        data: Data = field(default_factory=Data)
        code: str = field(default="")


@dataclass(frozen=True)
class ApiV3MarginAccountsGET:
    """https://www.kucoin.com/docs/rest/funding/funding-overview/get-account-detail-cross-margin."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            @dataclass(frozen=True)
            class Account:
                """."""

                currency: str = field(default="")
                liability: str = field(default="")
                available: str = field(default="")

            accounts: list[Account] = field(default_factory=list[Account])

        data: Data = field(default_factory=Data)
        code: str = field(default="")


@dataclass(frozen=True)
class OrderChangeV2:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            type: str = field(default="")
            symbol: str = field(default="")
            side: str = field(default="")
            size: str = field(default="")
            price: str = field(default="")

        data: Data = field(default_factory=Data)


@dataclass(frozen=True)
class AccountBalanceChange:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            currency: str = field(default="")
            total: str = field(default="")

        data: Data = field(default_factory=Data)


@dataclass(frozen=True)
class ApiV1BulletPrivatePOST:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            @dataclass(frozen=True)
            class Instance:
                """."""

                endpoint: str = field(default="")

            instanceServers: list[Instance] = field(default_factory=list[Instance])
            token: str = field(default="")

        data: Data = field(default_factory=Data)
        code: str = field(default="")


@dataclass(frozen=True)
class ApiV1AccountsGET:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            currency: str = field(default="")
            balance: str = field(default="")

        data: list[Data] = field(default_factory=list[Data])
        code: str = field(default="")


class KCN:
    """Main class collect all logic."""

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

    def convert_to_dataclass_from_dict[T](
        self: Self,
        data_class: type[T],
        data: dict[str, Any],
    ) -> Result[T, Exception]:
        """Convert dict to dataclass."""
        try:
            return Ok(
                from_dict(
                    data_class=data_class,
                    data=data,
                ),
            )
        except (
            WrongTypeError,
            MissingValueError,
            UnionMatchError,
            StrictUnionMatchError,
            UnexpectedDataError,
            ForwardReferenceError,
        ) as exc:
            return Err(exc)

    def get_telegram_url(self: Self) -> Result[str, Exception]:
        """Get url for send telegram msg."""
        return Ok(
            f"https://api.telegram.org/bot{self.TELEGRAM_BOT_API_KEY}/sendMessage",
        )

    def get_telegram_msg(
        self: Self,
        chat_id: str,
        data: str,
    ) -> Result[dict[str, bool | str], Exception]:
        """Get msg for telegram in dict."""
        return Ok(
            {
                "chat_id": chat_id,
                "parse_mode": "HTML",
                "disable_notification": True,
                "text": data,
            },
        )

    def get_chat_ids_for_telegram(self: Self) -> Result[list[str], Exception]:
        """Get list chat id for current send."""
        return Ok(self.TELEGRAM_BOT_CHAT_ID)

    def check_telegram_response(
        self: Self,
        data: TelegramSendMsg.Res,
    ) -> Result[None, Exception]:
        """Check telegram response on msg."""
        if data.ok:
            return Ok(None)
        return Err(Exception(f"{data}"))

    async def send_msg_to_each_chat_id(
        self: Self,
        chat_ids: list[str],
        data: str,
    ) -> Result[TelegramSendMsg.Res, Exception]:
        """Send msg for each chat id."""
        method = "POST"
        for chat in chat_ids:
            await do_async(
                Ok(result)
                for telegram_url in self.get_telegram_url()
                for msg in self.get_telegram_msg(chat, data)
                for msg_bytes in self.dumps_dict_to_bytes(msg)
                for response_bytes in await self.request(
                    url=telegram_url,
                    method=method,
                    headers={
                        "Content-Type": "application/json",
                    },
                    data=msg_bytes,
                )
                for response_dict in self.parse_bytes_to_dict(response_bytes)
                for data_dataclass in self.convert_to_dataclass_from_dict(
                    TelegramSendMsg.Res,
                    response_dict,
                )
                for result in self.check_telegram_response(data_dataclass)
            )
        return Ok(TelegramSendMsg.Res())

    async def send_telegram_msg(self: Self, data: str) -> Result[str, Exception]:
        """Send msg to telegram."""
        return await do_async(
            Ok("checked_dict")
            for chat_ids in self.get_chat_ids_for_telegram()
            for _ in await self.send_msg_to_each_chat_id(chat_ids, data)
        )

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

    async def post_api_v1_margin_order(
        self: Self,
        data: dict[str, str],
    ) -> Result[ApiV1MarginOrderPOST.Res, Exception]:
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
            Ok(result)
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
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1MarginOrderPOST.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v1_accounts(
        self: Self,
        params: dict[str, str],
    ) -> Result[ApiV1AccountsGET.Res, Exception]:
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
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1AccountsGET.Res,
                response_dict,
            )
            for checked_dict in self.check_response_code(data_dataclass)
        )

    async def get_api_v1_orders(
        self: Self,
        params: dict[str, str],
    ) -> Result[ApiV1OrdersGET.Res, Exception]:
        """Get all orders by params.

        https://www.kucoin.com/docs/rest/spot-trading/orders/get-order-list
        """
        uri = "/api/v1/orders"
        method = "GET"
        return await do_async(
            Ok(result)
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
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1OrdersGET.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def delete_api_v1_order(
        self: Self,
        order_id: str,
    ) -> Result[ApiV1OrdersDELETE.Res, Exception]:
        """Cancel order by `id`.

        https://www.kucoin.com/docs/rest/spot-trading/orders/cancel-order-by-orderid
        """
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
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1OrdersDELETE.Res,
                response_dict,
            )
            for checked_dict in self.check_response_code(data_dataclass)
        )

    async def get_api_v2_symbols(
        self: Self,
    ) -> Result[ApiV2SymbolsGET.Res, Exception]:
        """Get symbol list.

        https://www.kucoin.com/docs/rest/spot-trading/market-data/get-symbols-list
        """
        uri = "/api/v2/symbols"
        method = "GET"
        return await do_async(
            Ok(result)
            for headers in self.get_headers_not_auth()
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV2SymbolsGET.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v3_margin_accounts(
        self: Self,
        params: dict[str, str],
    ) -> Result[ApiV3MarginAccountsGET.Res, Exception]:
        """Get margin account user data.

        https://www.kucoin.com/docs/rest/funding/funding-overview/get-account-detail-cross-margin
        """
        uri = "/api/v3/margin/accounts"
        method = "GET"
        return await do_async(
            Ok(result)
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
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV3MarginAccountsGET.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v1_bullet_private(
        self: Self,
    ) -> Result[ApiV1BulletPrivatePOST.Res, Exception]:
        """Get tokens for private channel.

        https://www.kucoin.com/docs/websocket/basic-info/apply-connect-token/private-channels-authentication-request-required-
        """
        uri = "/api/v1/bullet-private"
        method = "POST"
        return await do_async(
            Ok(result)
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
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1BulletPrivatePOST.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v1_market_all_tickers(
        self: Self,
    ) -> Result[ApiV1MarketAllTickers.Res, Exception]:
        """Get all tickers with last price.

        https://www.kucoin.com/docs/rest/spot-trading/market-data/get-all-tickers
        """
        uri = "/api/v1/market/allTickers"
        method = "GET"
        return await do_async(
            Ok(result)
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for headers in self.get_headers_not_auth()
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1MarketAllTickers.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    def get_url_for_websocket(
        self: Self,
        data: ApiV1BulletPrivatePOST.Res,
    ) -> Result[str, Exception]:
        """Get complete url for websocket.

        exp: wss://ws-api-spot.kucoin.com/?token=xxx&[connectId=xxxxx]
        """
        return do(
            Ok(complete_url)
            for url in self.export_url_from_api_v1_bullet(data)
            for token in self.export_token_from_api_v1_bullet(data)
            for uuid_str in self.get_uuid4()
            for complete_url in self.cancatinate_str(
                url,
                "?token=",
                token,
                "&connectId=",
                uuid_str,
            )
        )

    def get_first_item_from_list[T](self: Self, data: list[T]) -> Result[T, Exception]:
        """Get first item from list."""
        try:
            return Ok(data[0])
        except (TypeError, IndexError) as exc:
            return Err(exc)

    def export_url_from_api_v1_bullet(
        self: Self,
        data: ApiV1BulletPrivatePOST.Res,
    ) -> Result[str, Exception]:
        """Get endpoint for public websocket."""
        try:
            return do(
                Ok(instance.endpoint)
                for instance in self.get_first_item_from_list(data.data.instanceServers)
            )
        except (KeyError, TypeError) as exc:
            return Err(Exception(f"Miss keys instanceServers in {exc} by {data}"))

    def export_token_from_api_v1_bullet(
        self: Self,
        data: ApiV1BulletPrivatePOST.Res,
    ) -> Result[str, Exception]:
        """Get token for public websocket."""
        try:
            return Ok(data.data.token)
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

    def check_response_code[T](
        self: Self,
        data: T,
    ) -> Result[T, Exception]:
        """Check if key `code`.

        If key `code` in dict == '200000' then success
        """
        if hasattr(data, "code") and data.code == "200000":
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
                logger.success(f"{response.status}:{method}:{url}")
                return Ok(res)
        except ClientConnectorError as exc:
            logger.exception(exc)
            return Err(exc)

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

    async def send_data_to_ws(
        self: Self,
        ws: ClientConnection,
        data: dict[str, Any],
    ) -> Result[None, Exception]:
        """Send data to websocket."""
        return await do_async(
            Ok(response)
            for data_bytes in self.dumps_dict_to_bytes(data)
            for response in await self.send_data_to_websocket(ws, data_bytes)
        )

    async def welcome_processing_websocket(
        self: Self,
        cc: ClientConnection,
    ) -> Result[None, Exception]:
        """When the connection on websocket is successfully established.

        the system will send a welcome message.

        {
            "id": "hQvf8jkno",
            "type": "welcome"
        }
        """
        return await do_async(
            Ok(None)
            for welcome_data_websocket in await self.recv_data_from_websocket(cc)
            for welcome in self.parse_bytes_to_dict(welcome_data_websocket)
            for _ in self.check_welcome_msg_from_websocket(welcome)
        )

    def check_ack_websocket(
        self: Self,
        req: dict[str, Any],
        res: dict[str, Any],
    ) -> Result[None, Exception]:
        """Check ack from websocket on subscribe."""
        if req["id"] == res["id"]:
            return Ok(None)
        logger.exception(Exception(f"{req=} != {res}"))
        return Err(Exception(f"{req=} != {res}"))

    async def ack_processing_websocket(
        self: Self,
        ws_inst: ClientConnection,
        subsribe_msg: dict[str, str | bool],
    ) -> Result[None, Exception]:
        """Ack processing on websocket."""
        return await do_async(
            Ok(None)
            for _ in await self.send_data_to_ws(ws_inst, subsribe_msg)
            for ack_subscribe in await self.recv_data_from_websocket(ws_inst)
            for ack_subscribe_dict in self.parse_bytes_to_dict(ack_subscribe)
            for _ in self.check_ack_websocket(subsribe_msg, ack_subscribe_dict)
        )

    async def runtime_balance_ws(
        self: Self,
        ws: connect,
        subsribe_msg: dict[str, str | bool],
    ) -> Result[None, Exception]:
        """Runtime listen websocket all time."""
        async for ws_inst in ws:
            return await do_async(
                Ok(None)
                # get welcome msg
                for _ in await self.welcome_processing_websocket(ws_inst)
                # subscribe to topic
                for _ in await self.ack_processing_websocket(ws_inst, subsribe_msg)
                for _ in await self.listen_balance_msg(ws_inst)
            )
        return Ok(None)

    async def runtime_matching_ws(
        self: Self,
        ws: connect,
        subsribe_msg: dict[str, str | bool],
    ) -> Result[None, Exception]:
        """Runtime listen websocket all time."""
        async for ws_inst in ws:
            return await do_async(
                Ok(None)
                # get welcome msg
                for _ in await self.welcome_processing_websocket(ws_inst)
                # subscribe to topic
                for _ in await self.ack_processing_websocket(ws_inst, subsribe_msg)
                for _ in await self.listen_matching_msg(ws_inst)
            )
        return Ok(None)

    async def recv_data_from_websocket(
        self: Self,
        ws: ClientConnection,
    ) -> Result[str | bytes, Exception]:
        """Universal recive data from websocket."""
        try:
            res = await ws.recv()
            return Ok(res)

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
        """Universal send data to websocket."""
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

    def logger_info[T](self: Self, data: T) -> Result[T, Exception]:
        """Info logger for Pipes."""
        logger.info(data)
        return Ok(data)

    def logger_exception[T](self: Self, data: T) -> Result[T, Exception]:
        """Exception logger for Pipes."""
        logger.exception(data)
        return Ok(data)

    def logger_success[T](self: Self, data: T) -> Result[T, Exception]:
        """Success logger for Pipes."""
        logger.success(data)
        return Ok(data)

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
        data: bytes | str,
    ) -> Result[dict[str, Any], Exception]:
        """Parse bytes[json] to dict.

        b'{"qaz":"wsx"}' -> {"qaz":"wsx"}
        """
        try:
            return Ok(loads(data))
        except JSONDecodeError as exc:
            logger.exception(exc)
            return Err(exc)

    def create_book(self: Self) -> Result[None, Exception]:
        """Build own structure.

        build inside book for tickets

        {
            "ADA": {
                "balance": "",
                "last": "",
                "increment": "",
                "sellorder": "",
                "buyorder": ""
            },
            "JUP": {
                "balance": "",
                "last": "",
                "increment": "",
                "sellorder": "",
                "buyorder": ""
            },
            "SOL": {
                "balance": "",
                "last": "",
                "increment": "",
                "sellorder": "",
                "buyorder": ""
            },
            "BTC": {
                "balance": "",
                "last": "",
                "increment": "",
                "sellorder": "",
                "buyorder": ""
            }
        }
        """
        self.book: dict[str, dict[str, str | Decimal]] = {
            ticket: {} for ticket in self.ALL_CURRENCY if isinstance(ticket, str)
        }
        return Ok(None)

    def decimal_to_str(self: Self, data: Decimal) -> Result[str, Exception]:
        """Convert Decimal to str."""
        return Ok(str(data))

    def int_to_decimal(self: Self, data: float | str) -> Result[Decimal, Exception]:
        """Convert to Decimal format."""
        try:
            return Ok(Decimal(data))
        except (TypeError, InvalidOperation) as exc:
            return Err(exc)

    def event_fill_balance(
        self: Self,
        data: AccountBalanceChange.Res,
    ) -> Result[None, Exception]:
        """Fill balance from event balance websocket."""
        if data.data.currency in self.book:
            self.book[data.data.currency]["balance"] = Decimal(data.data.total)
        return Ok(None)

    async def listen_balance_msg(
        self: Self,
        ws_inst: ClientConnection,
    ) -> Result[None, Exception]:
        """Infinity loop for listen balance msgs."""
        while True:
            await do_async(
                Ok(None)
                for msg in await self.recv_data_from_websocket(ws_inst)
                for value in self.parse_bytes_to_dict(msg)
                for data_dataclass in self.convert_to_dataclass_from_dict(
                    AccountBalanceChange.Res,
                    value,
                )
                for _ in self.event_fill_balance(data_dataclass)
            )

    async def event_matching(
        self: Self,
        data: dict[str, dict[str, str]],
    ) -> Result[None, Exception]:
        """Event matching order."""
        if (
            "data" in data
            and "type" in data["data"]
            and data["data"]["type"] == "filled"
        ):
            self.logger_success(f"Order filled:{data}")
            # send telegram msg
            # cancel other order
            # make new orders on sell and buy
        return Ok(None)

    async def listen_matching_msg(
        self: Self,
        ws_inst: ClientConnection,
    ) -> Result[None, Exception]:
        """Infinity loop for listen matching msgs."""
        while True:
            await do_async(
                Ok(None)
                for msg in await self.recv_data_from_websocket(ws_inst)
                for value in self.parse_bytes_to_dict(msg)
                for data_dataclass in self.convert_to_dataclass_from_dict(
                    OrderChangeV2.Res,
                    value,
                )
                for _ in self.logger_info(data_dataclass)
            )

    def export_account_usdt_from_api_v3_margin_accounts(
        self: Self,
        data: ApiV3MarginAccountsGET.Res,
    ) -> Result[ApiV3MarginAccountsGET.Res.Data.Account, Exception]:
        """Get USDT available from margin account."""
        try:
            for i in [i for i in data.data.accounts if i.currency == "USDT"]:
                return Ok(i)
            return Err(Exception("Not found USDT in accounts data"))
        except (AttributeError, KeyError) as exc:
            logger.exception(exc)
            return Err(exc)

    def export_liability_usdt(
        self: Self,
        data: ApiV3MarginAccountsGET.Res.Data.Account,
    ) -> Result[Decimal, Exception]:
        """Export liability and available USDT from api_v3_margin_accounts."""
        return do(Ok(result) for result in self.int_to_decimal(data.liability))

    def export_available_usdt(
        self: Self,
        data: ApiV3MarginAccountsGET.Res.Data.Account,
    ) -> Result[Decimal, Exception]:
        """Export liability and available USDT from api_v3_margin_accounts."""
        return do(Ok(result) for result in self.int_to_decimal(data.available))

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
        orders: ApiV1OrdersGET.Res,
    ) -> Result[list[str], Exception]:
        """Export id from orders list."""
        return Ok([order.id for order in orders.data.items])

    def get_msg_for_subscribe_balance(
        self: Self,
    ) -> Result[dict[str, str | bool], Exception]:
        """Get msg for subscribe to balance kucoin."""
        return do(
            Ok(
                {
                    "id": uuid_str,
                    "type": "subscribe",
                    "topic": "/account/balance",
                    "privateChannel": True,
                    "response": True,
                },
            )
            for default_uuid4 in self.get_default_uuid4()
            for uuid_str in self.format_to_str_uuid(default_uuid4)
        )

    def get_msg_for_subscribe_matching(
        self: Self,
    ) -> Result[dict[str, str | bool], Exception]:
        """Get msg for subscribe to matching kucoin."""
        return do(
            Ok(
                {
                    "id": uuid_str,
                    "type": "subscribe",
                    "topic": "/spotMarket/tradeOrdersV2",
                    "privateChannel": True,
                    "response": True,
                },
            )
            for default_uuid4 in self.get_default_uuid4()
            for uuid_str in self.format_to_str_uuid(default_uuid4)
        )

    async def balancer(self: Self) -> Result[None, Exception]:
        """Monitoring of balance.

        Start listen websocket
        """
        logger.info("balancer")
        return await do_async(
            Ok(None)
            for private_token in await self.get_api_v1_bullet_private()
            for checked_dict in self.check_response_code(private_token)
            for url_ws in self.get_url_for_websocket(checked_dict)
            for ws in self.get_websocket(url_ws)
            for msg_subscribe_balance in self.get_msg_for_subscribe_balance()
            for _ in await self.runtime_balance_ws(
                ws,
                msg_subscribe_balance,
            )
        )

    async def matching(self: Self) -> Result[None, Exception]:
        """Monitoring of matching order.

        Start listen websocket
        """
        logger.info("matching")
        return await do_async(
            Ok(None)
            for private_token in await self.get_api_v1_bullet_private()
            for checked_dict in self.check_response_code(private_token)
            for url_ws in self.get_url_for_websocket(checked_dict)
            for ws in self.get_websocket(url_ws)
            for msg_subscribe_matching in self.get_msg_for_subscribe_matching()
            for _ in await self.runtime_matching_ws(
                ws,
                msg_subscribe_matching,
            )
        )

    async def start_up_orders(self: Self) -> Result[None, Exception]:
        """."""
        # wait while matcheer and balancer would be ready
        await asyncio.sleep(10)

        for ticket, params in self.book.items():
            self.logger_info(f"{ticket=} {params=}")

        return Ok(None)

    def _fill_balance(
        self: Self,
        data: ApiV1AccountsGET.Res,
    ) -> Result[None, Exception]:
        """Export current balance from data."""
        for ticket in data.data:
            if ticket.currency in self.book:
                self.book[ticket.currency]["balance"] = Decimal(ticket.balance)
        return Ok(None)

    async def fill_balance(self: Self) -> Result[None, Exception]:
        """Fill all balance by ENVs."""
        return await do_async(
            Ok(None)
            for balance_accounts in await self.get_api_v1_accounts(
                params={"type": "margin"},
            )
            for _ in self._fill_balance(balance_accounts)
        )

    # nu cho jopki kak dila
    def _fill_base_increment(
        self: Self,
        data: ApiV2SymbolsGET.Res,
    ) -> Result[None, Exception]:
        """Fill base increment by each token."""
        for out_side_ticket in data.data:
            if (
                out_side_ticket.baseCurrency in self.book
                and out_side_ticket.quoteCurrency == "USDT"
            ):
                self.book[out_side_ticket.baseCurrency]["increment"] = Decimal(
                    out_side_ticket.baseIncrement,
                )
        return Ok(None)

    async def fill_base_increment(self: Self) -> Result[None, Exception]:
        """Fill base increment from api."""
        return await do_async(
            Ok(None)
            for ticket_info in await self.get_api_v2_symbols()
            for _ in self._fill_base_increment(ticket_info)
        )

    def _fill_last_price(
        self: Self,
        data: ApiV1MarketAllTickers.Res,
    ) -> Result[None, Exception]:
        """Fill last price for each token."""
        for ticket in data.data.ticker:
            symbol = f"{ticket.symbol}-USDT"
            if symbol in self.book:
                self.book[symbol]["last"] = Decimal(ticket.last)
        return Ok(None)

    async def fill_last_price(self: Self) -> Result[None, Exception]:
        """Fill last price for first order init."""
        return await do_async(
            Ok(None)
            for market_ticket in await self.get_api_v1_market_all_tickers()
            for _ in self._fill_last_price(market_ticket)
        )

    def devide(
        self: Self,
        divider: Decimal,
        divisor: Decimal,
    ) -> Result[Decimal, Exception]:
        """Devide."""
        try:
            return Ok(divider / divisor)
        except ZeroDivisionError as exc:
            return Err(exc)

    def calc_down_change_balance(
        self: Self,
        balance: Decimal,
        need_balance: Decimal,
        last_price: Decimal,
    ) -> Result[Decimal, Exception]:
        """."""
        if balance > need_balance:
            return do(Ok(result) for result in self.devide(self.BASE_KEEP, last_price))
        return Ok(balance)

    def calc_up_change_balance(
        self: Self,
        balance: Decimal,
        need_balance: Decimal,
        last_price: Decimal,
    ) -> Result[Decimal, Exception]:
        """."""
        if balance < need_balance:
            return do(Ok(result) for result in self.devide(self.BASE_KEEP, last_price))
        return Ok(balance)

    def calc_up(
        self: Self,
        balance: Decimal,
        last_price: Decimal,
        increment: Decimal,
    ) -> Result[OrderParam, Exception]:
        """Calc up price and size tokens."""
        return do(
            Ok(
                OrderParam(
                    side="sell",
                    price=up_last_price_str,
                    size=f"{(balance - need_balance).quantize(increment, ROUND_DOWN)}",
                ),
            )
            for up_last_price in self.up_1_percent(last_price)
            for up_last_price_str in self.decimal_to_str(up_last_price)
            for need_balance in self.devide(self.BASE_KEEP, up_last_price)
            for balance in self.calc_up_change_balance(
                balance,
                need_balance,
                last_price,
            )
        )

    def calc_down(
        self: Self,
        balance: Decimal,
        last_price: Decimal,
        increment: Decimal,
    ) -> Result[OrderParam, Exception]:
        """Calc down price and size tokens."""
        return do(
            Ok(
                OrderParam(
                    side="buy",
                    price=down_last_price_str,
                    size=f"{(need_balance - balance).quantize(increment, ROUND_DOWN)}",
                ),
            )
            for down_last_price in self.down_1_percent(last_price)
            for down_last_price_str in self.decimal_to_str(down_last_price)
            for need_balance in self.devide(self.BASE_KEEP, down_last_price)
            for balance in self.calc_down_change_balance(
                balance,
                need_balance,
                last_price,
            )
        )

    def up_1_percent(self: Self, data: Decimal) -> Result[Decimal, Exception]:
        """Current price plus 1 percent."""
        return Ok(data * Decimal("1.01"))

    def down_1_percent(self: Self, data: Decimal) -> Result[Decimal, Exception]:
        """Current price minus 1 percent."""
        return Ok(data * Decimal("0.99"))

    async def pre_init(self: Self) -> Result[Self, Exception]:
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
            for _ in await self.fill_last_price()
        )

    async def infinity_task(self: Self) -> Result[None, Exception]:
        """."""
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self.balancer()),
                tg.create_task(self.matching()),
                tg.create_task(self.start_up_orders()),
            ]

        for task in tasks:
            return task.result()

        return Ok(None)


# meow anton - baka des ^^


async def main() -> Result[None, Exception]:
    """Collect of major func."""
    kcn = KCN()
    match await do_async(
        Ok(None)
        for _ in await kcn.pre_init()
        for _ in kcn.logger_success("Pre-init OK!")
        for _ in await kcn.send_telegram_msg("Settings are OK!")
        for _ in await kcn.infinity_task()
    ):
        case Ok(None):
            pass
        case Err(exc):
            logger.exception(exc)
            raise exc
    return Ok(None)


if __name__ == "__main__":
    """Main enter."""
    asyncio.run(main())
