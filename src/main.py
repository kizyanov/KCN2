#!/usr/bin/env python
"""KCN2 trading bot for kucoin."""

import asyncio
import socket
from base64 import b64encode
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_DOWN, ROUND_UP, Decimal, InvalidOperation
from hashlib import sha256
from hmac import HMAC
from hmac import new as hmac_new
from itertools import batched
from os import environ
from ssl import SSLError
from time import time
from typing import Any, Self
from urllib.parse import urljoin
from uuid import UUID, uuid4

from aiohttp import ClientConnectorError, ClientSession
from asyncpg import Pool, Record, create_pool
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


@dataclass
class AlertestToken:
    """."""

    all_tokens: list[str]
    deleted_tokens: list[str]
    new_tokens: list[str]


@dataclass
class Book:
    """Store data for each token."""

    balance: Decimal = field(default=Decimal("0"))
    last_price: Decimal = field(default=Decimal("0"))
    baseincrement: Decimal = field(default=Decimal("0"))
    priceincrement: Decimal = field(default=Decimal("0"))
    borrow: Decimal = field(default=Decimal("0"))


@dataclass(frozen=True)
class OrderParam:
    """."""

    side: str
    price: str
    size: str


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
                buy: str | None = field(default=None)

            ticker: list[Ticker] = field(default_factory=list[Ticker])

        data: Data = field(default_factory=Data)
        code: str = field(default="")
        msg: str = field(default="")


@dataclass(frozen=True)
class ApiV3HfMarginOrderPOST:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            orderId: str = field(default="")

        code: str = field(default="")
        msg: str = field(default="")
        data: Data = field(default_factory=Data)


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
            priceIncrement: str = field(default="")
            isMarginEnabled: bool = field(default=False)

        data: list[Data] = field(default_factory=list[Data])
        code: str = field(default="")
        msg: str = field(default="")


@dataclass(frozen=True)
class ApiV1OrdersDELETE:
    """https://www.kucoin.com/docs/rest/spot-trading/orders/cancel-order-by-orderid."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        code: str = field(default="")
        msg: str = field(default="")


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
                symbol: str = field(default="")

            items: list[Item] = field(default_factory=list[Item])

        data: Data = field(default_factory=Data)
        code: str = field(default="")
        msg: str = field(default="")


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
            debtRatio: str = field(default="")

        data: Data = field(default_factory=Data)
        code: str = field(default="")
        msg: str = field(default="")


@dataclass(frozen=True)
class ApiV3MarginRepayPOST:
    """https://www.kucoin.com/docs-new/rest/margin-trading/debit/repay."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        code: str = field(default="")
        msg: str = field(default="")


@dataclass(frozen=True)
class OrderChangeV2:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            symbol: str
            side: str
            orderId: str
            orderType: str
            type: str
            price: str | None  # fix for market order type
            size: str | None
            matchSize: str | None

        data: Data


@dataclass(frozen=True)
class MarketCandle:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            symbol: str
            price: str

        data: Data


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

                endpoint: str
                pingInterval: int
                pingTimeout: int

            instanceServers: list[Instance] = field(default_factory=list[Instance])
            token: str = field(default="")

        data: Data = field(default_factory=Data)
        code: str = field(default="")
        msg: str = field(default="")


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
        msg: str = field(default="")


class KCN:
    """Main class collect all logic."""

    def init_envs(self: Self) -> Result[None, Exception]:
        """Init settings."""
        # All about excange
        self.KEY = self.get_env("KEY").unwrap()
        self.SECRET = self.get_env("SECRET").unwrap()
        self.PASSPHRASE = self.get_env("PASSPHRASE").unwrap()
        self.BASE_URL = self.get_env("BASE_URL").unwrap()

        # all about tokens
        self.ALL_CURRENCY = self.get_list_env("ALLCURRENCY").unwrap()
        self.IGNORECURRENCY = self.get_list_env("IGNORECURRENCY").unwrap()
        self.BASE_KEEP = Decimal(self.get_env("BASE_KEEP").unwrap())

        # All about tlg
        self.TELEGRAM_BOT_API_KEY = self.get_env("TELEGRAM_BOT_API_KEY").unwrap()
        self.TELEGRAM_BOT_CHAT_ID = self.get_list_env("TELEGRAM_BOT_CHAT_ID").unwrap()

        # db store
        self.PG_USER = self.get_env("PG_USER").unwrap()
        self.PG_PASSWORD = self.get_env("PG_PASSWORD").unwrap()
        self.PG_DATABASE = self.get_env("PG_DATABASE").unwrap()
        self.PG_HOST = self.get_env("PG_HOST").unwrap()
        self.PG_PORT = self.get_env("PG_PORT").unwrap()

        logger.success("Settings are OK!")
        return Ok(None)

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

    async def send_telegram_msg(self: Self, data: str) -> Result[None, Exception]:
        """Send msg to telegram."""
        match await do_async(
            Ok(None)
            for chat_ids in self.get_chat_ids_for_telegram()
            for _ in await self.send_msg_to_each_chat_id(chat_ids, data)
        ):
            case Err(exc):
                logger.exception(exc)
        return Ok(None)

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

    async def post_api_v3_hf_margin_order(
        self: Self,
        data: dict[str, str | bool],
    ) -> Result[ApiV3HfMarginOrderPOST.Res, Exception]:
        """Make margin order.

        https://www.kucoin.com/docs-new/rest/margin-trading/orders/add-order

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
        uri = "/api/v3/hf/margin/order"
        method = "POST"
        return await do_async(
            Ok(result)
            for _ in self.logger_info(f"Margin order:{data}")
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
                ApiV3HfMarginOrderPOST.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def post_api_v3_margin_repay(
        self: Self,
        data: dict[str, str | int],
    ) -> Result[ApiV3MarginRepayPOST.Res, Exception]:
        """Repay borrowed .

        https://www.kucoin.com/docs-new/rest/margin-trading/debit/repay

        """
        uri = "/api/v3/margin/repay"
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
                ApiV3MarginRepayPOST.Res,
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

    def get_all_token_for_matching(self: Self) -> Result[list[str], Exception]:
        """."""
        return Ok([f"{symbol}-USDT" for symbol in self.book])

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

    async def get_api_v1_bullet_public(
        self: Self,
    ) -> Result[ApiV1BulletPrivatePOST.Res, Exception]:
        """Get tokens for private channel.

        https://www.kucoin.com/docs/websocket/basic-info/apply-connect-token/public-token-no-authentication-required-
        """
        uri = "/api/v1/bullet-public"
        method = "POST"
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

    def get_ping_interval_for_websocket(
        self: Self,
        data: ApiV1BulletPrivatePOST.Res,
    ) -> Result[float, Exception]:
        """Get ping interval for websocket."""
        try:
            return do(
                Ok(float(instance.pingInterval / 1000))
                for instance in self.get_first_item_from_list(data.data.instanceServers)
            )
        except (KeyError, TypeError) as exc:
            return Err(Exception(f"Miss keys instanceServers in {exc} by {data}"))

    def get_ping_timeout_for_websocket(
        self: Self,
        data: ApiV1BulletPrivatePOST.Res,
    ) -> Result[float, Exception]:
        """Get ping timeout for websocket."""
        try:
            return do(
                Ok(float(instance.pingTimeout / 1000))
                for instance in self.get_first_item_from_list(data.data.instanceServers)
            )
        except (KeyError, TypeError) as exc:
            return Err(Exception(f"Miss keys instanceServers in {exc} by {data}"))

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
            Ok(f"{time_now_in_int * 1000}")
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

    def get_websocket(
        self: Self,
        url: str,
        ping_interval: float,
        ping_timeout: float,
    ) -> Result[connect, Exception]:
        """Get connect for working with websocket by url."""
        return Ok(
            connect(
                uri=url,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
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
        ws_inst: ClientConnection,
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
            for welcome_data_websocket in await self.recv_data_from_websocket(ws_inst)
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

    async def runtime_matching_ws(
        self: Self,
        ws: connect,
        subsribe_msg: dict[str, str | bool],
    ) -> Result[None, Exception]:
        """Runtime listen websocket all time."""
        async with ws as ws_inst:
            match await do_async(
                Ok(None)
                # get welcome msg
                for _ in await self.welcome_processing_websocket(ws_inst)
                # subscribe to topic
                for _ in await self.ack_processing_websocket(ws_inst, subsribe_msg)
                for _ in await self.listen_matching_event(ws_inst)
            ):
                case Err(exc):
                    return Err(exc)

        return Ok(None)

    def get_tunnel(
        self: Self,
        tunnelid: str,
    ) -> Result[dict[str, str | bool], Exception]:
        """Working with tunnel."""
        return Ok(
            {
                "id": str(int(time() * 1000)),
                "type": "openTunnel",
                "newTunnelId": tunnelid,
            },
        )

    async def runtime_candle_ws(
        self: Self,
        ws: connect,
    ) -> Result[None, Exception]:
        """Runtime listen websocket all time."""
        tunnelid = "all_klines"
        logger.warning("runtime_candle_ws")
        async with ws as ws_inst:
            match await do_async(
                Ok(candles)
                # get welcome msg
                for _ in self.logger_info("welcome_processing_websocket")
                for _ in await self.welcome_processing_websocket(ws_inst)
                # tunnel create
                for _ in self.logger_info("get_tunnel")
                for tunnel_msg in self.get_tunnel(tunnelid)
                for _ in await self.send_data_to_ws(ws_inst, tunnel_msg)
                for _ in self.logger_info("send tunnel info")
                for candles in self.get_all_token_for_matching()
            ):
                case Ok(candles):
                    for msgs in batched(candles, 10, strict=False):
                        logger.info(f"{len(msgs)} {msgs}")
                        match await do_async(
                            Ok(_)
                            for msg_subscribe_candle in self.get_msg_for_subscribe_candle(
                                msgs,
                                tunnelid,
                            )
                            for _ in self.logger_success(msg_subscribe_candle)
                            # subscribe to topic
                            for _ in await self.send_data_to_ws(
                                ws_inst,
                                msg_subscribe_candle,
                            )
                        ):
                            case Err(exc):
                                logger.exception(exc)

                    match await do_async(
                        Ok(_) for _ in await self.listen_candle_event(ws_inst)
                    ):
                        case Err(exc):
                            return Err(exc)

        return Ok(None)

    async def recv_data_from_websocket(
        self: Self,
        ws: ClientConnection,
    ) -> Result[bytes | str, Exception]:
        """Universal recive data from websocket."""
        res = await ws.recv(decode=False)
        return Ok(res)

    async def send_data_to_websocket(
        self: Self,
        ws: ClientConnection,
        data: bytes,
    ) -> Result[None, Exception]:
        """Universal send data to websocket."""
        await ws.send(data, text=True)
        return Ok(None)

    def logger_info[T](self: Self, data: T) -> Result[T, Exception]:
        """Info logger for Pipes."""
        logger.info(data)
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
        book = {
            "ADA": {
                "balance": Decimal,
                "last": Decimal,
                "baseincrement": Decimal,
                "priceincrement": Decimal,
                "borrow": Decimal,
            },
            "JUP": {
                "balance": Decimal,
                "last": Decimal,
                "baseincrement": Decimal,
                "priceincrement": Decimal,
                "borrow": Decimal,
            }
        }
        book_orders = {
            "ADA": {
                "sell": "",
                "buy": ""
            },
            "JUP": {
                "sell": "",
                "buy": ""
            }
        }
        """
        self.book: dict[str, Book] = {
            ticket: Book() for ticket in self.ALL_CURRENCY if isinstance(ticket, str)
        }
        self.book_orders: dict[str, dict[str, str]] = {
            ticket: {
                "sell": "",
                "buy": "",
            }
            for ticket in self.ALL_CURRENCY
            if isinstance(ticket, str)
        }
        return Ok(None)

    def decimal_to_str(self: Self, data: Decimal) -> Result[str, Exception]:
        """Convert Decimal to str."""
        return Ok(str(data))

    def data_to_decimal(self: Self, data: float | str) -> Result[Decimal, Exception]:
        """Convert to Decimal format."""
        try:
            return Ok(Decimal(data))
        except (TypeError, InvalidOperation) as exc:
            return Err(exc)

    def replace_quote_in_symbol_name(self: Self, data: str) -> Result[str, Exception]:
        """Replace BTC-USDT to BTC."""
        return Ok(data.replace("-USDT", ""))

    def find_loses_orders(
        self: Self,
        symbol: str,
        order_id: str,
    ) -> Result[list[str], Exception]:
        """Find other orders not quals with order_id."""
        result: list[str] = []
        if symbol in self.book_orders:
            result = [
                saved_order_id
                for saved_order_id in self.book_orders[symbol].values()
                if order_id != saved_order_id
            ]
            self.book_orders[symbol] = {"sell": "", "buy": ""}
            return Ok(result)
        return Ok([])

    def update_last_price_to_book(
        self: Self,
        ticker: str,
        price: Decimal,
    ) -> Result[None, Exception]:
        """."""
        try:
            self.book[ticker].last_price = price
        except IndexError as exc:
            return Err(exc)
        return Ok(None)

    async def event_filled(
        self: Self,
        data: OrderChangeV2.Res.Data,
    ) -> Result[None, Exception]:
        """Event when order full filled."""
        return await do_async(
            Ok(None)
            for symbol_name in self.replace_quote_in_symbol_name(data.symbol)
            # update last price
            for price_decimal in self.data_to_decimal(data.price or "")
            for _ in self.update_last_price_to_book(symbol_name, price_decimal)
            # send data to db
            for _ in await self.insert_data_to_db(data)
            # cancel other order of symbol
            for loses_orders in self.find_loses_orders(
                symbol_name,
                data.orderId,
            )
            for _ in await self.massive_cancel_order(loses_orders)
            # create new orders
            for _ in await self.make_updown_margin_order(symbol_name)
        )

    async def event_matching(
        self: Self,
        data: OrderChangeV2.Res.Data,
    ) -> Result[None, Exception]:
        """Event when order parted filled."""
        logger.debug(data)
        return Ok(None)

    async def event_candll(
        self: Self,
        data: MarketCandle.Res,
    ) -> Result[None, Exception]:
        """Event matching order."""
        match do(
            Ok(symbol) for symbol in self.replace_quote_in_symbol_name(data.data.symbol)
        ):
            case Ok(symbol):
                if symbol in self.book and self.book[symbol].last_price > Decimal(
                    data.data.price
                ):
                    logger.warning(f"New low price:{symbol} to {data.data.price}")
                    self.book[symbol].last_price = Decimal(data.data.price)
                    # need create new order by new latest_price
                    # need delete old order
        return Ok(None)

    async def listen_candle_event(
        self: Self,
        ws_inst: ClientConnection,
    ) -> Result[None, Exception]:
        """Infinity loop for listen candle msgs."""
        logger.warning("listen_candle_event")
        async for msg in ws_inst:
            logger.debug(msg)
            match await do_async(
                Ok(None)
                for value in self.parse_bytes_to_dict(msg)
                for data_dataclass in self.convert_to_dataclass_from_dict(
                    MarketCandle.Res,
                    value,
                )
                for _ in await self.event_candll(data_dataclass)
            ):
                case Err(exc):
                    return Err(exc)
        return Ok(None)

    async def listen_matching_event(
        self: Self,
        ws_inst: ClientConnection,
    ) -> Result[None, Exception]:
        """Infinity loop for listen candle msgs."""
        async for msg in ws_inst:
            match await do_async(
                Ok(None)
                for value in self.parse_bytes_to_dict(msg)
                for data_dataclass in self.convert_to_dataclass_from_dict(
                    OrderChangeV2.Res,
                    value,
                )
                for _ in await self.event_matching(data_dataclass.data)
            ):
                case Err(exc):
                    return Err(exc)
        return Ok(None)

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

    def compile_telegram_msg_alertest(
        self: Self,
        finance: ApiV3MarginAccountsGET.Res.Data.Account,
        debt_ratio: str,
        tokens: AlertestToken,
    ) -> Result[str, Exception]:
        """."""
        return Ok(
            f"""<b>KuCoin</b>
<i>KEEP</i>:{self.BASE_KEEP}
<i>USDT</i>:{finance.available}
<i>BORROWING USDT</i>:{finance.liability}   ({debt_ratio}%))
<i>ALL TOKENS</i>:{len(tokens.all_tokens)}
<i>USED TOKENS</i>({len(self.ALL_CURRENCY)})
<i>DELETED</i>({len(tokens.deleted_tokens)}):{",".join(tokens.deleted_tokens)}
<i>NEW</i>({len(tokens.new_tokens)}):{",".join(tokens.new_tokens)}
<i>IGNORE</i>({len(self.IGNORECURRENCY)}):{",".join(self.IGNORECURRENCY)}""",
        )

    def parse_tokens_for_alertest(
        self: Self,
        data: ApiV2SymbolsGET.Res,
    ) -> Result[AlertestToken, Exception]:
        """."""
        all_tokens: list[str] = [
            exchange_token.baseCurrency
            for exchange_token in data.data
            if exchange_token.baseCurrency not in self.IGNORECURRENCY
            and exchange_token.isMarginEnabled
            and exchange_token.quoteCurrency == "USDT"
        ]

        deleted_tokens: list[str] = [
            own_token
            for own_token in self.ALL_CURRENCY
            if (
                len(
                    [
                        exchange_token
                        for exchange_token in data.data
                        if exchange_token.baseCurrency == own_token
                        and exchange_token.isMarginEnabled
                        and exchange_token.quoteCurrency == "USDT"
                    ],
                )
                == 0
            )
        ]

        new_tokens: list[str] = [
            exchange_token.baseCurrency
            for exchange_token in data.data
            if exchange_token.baseCurrency
            not in self.ALL_CURRENCY + self.IGNORECURRENCY
            and exchange_token.isMarginEnabled
            and exchange_token.quoteCurrency == "USDT"
        ]

        return Ok(
            AlertestToken(
                all_tokens=all_tokens,
                deleted_tokens=deleted_tokens,
                new_tokens=new_tokens,
            ),
        )

    def export_debt_ratio(
        self: Self,
        data: ApiV3MarginAccountsGET.Res,
    ) -> Result[str, Exception]:
        """."""
        return Ok(data.data.debtRatio)

    async def alertest(self: Self) -> Result[None, Exception]:
        """Alert statistic."""
        logger.info("alertest")
        while True:
            await do_async(
                Ok(None)
                for api_v3_margin_accounts in await self.get_api_v3_margin_accounts(
                    params={
                        "quoteCurrency": "USDT",
                    },
                )
                for account_data in self.export_account_usdt_from_api_v3_margin_accounts(
                    api_v3_margin_accounts,
                )
                for debt_ratio in self.export_debt_ratio(api_v3_margin_accounts)
                for ticket_info in await self.get_api_v2_symbols()
                for parsed_ticked_info in self.parse_tokens_for_alertest(ticket_info)
                for tlg_msg in self.compile_telegram_msg_alertest(
                    account_data,
                    debt_ratio,
                    parsed_ticked_info,
                )
                for _ in await self.send_telegram_msg(tlg_msg)
            )
            await asyncio.sleep(60 * 60)
        return Ok(None)

    async def massive_cancel_order(
        self: Self,
        data: list[str],
    ) -> Result[None, Exception]:
        """Cancel all order in data list."""
        for order_id in data:
            await self.delete_api_v1_order(order_id)
        return Ok(None)

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

    def get_candles_for_kline(
        self: Self,
        raw_candle: tuple[str, ...],
    ) -> Result[str, Exception]:
        """."""
        return Ok(",".join(raw_candle))

    def get_msg_for_subscribe_candle(
        self: Self,
        raw_candle: tuple[str, ...],
        tunnelid: str,
    ) -> Result[dict[str, str | bool], Exception]:
        """Get msg for subscribe to candle kucoin."""
        return do(
            Ok(
                {
                    "id": uuid_str,
                    "type": "subscribe",
                    "topic": f"/market/match:{candles}",
                    "privateChannel": False,
                    "response": False,
                    "tunnelId": tunnelid,
                },
            )
            for candles in self.get_candles_for_kline(raw_candle)
            for default_uuid4 in self.get_default_uuid4()
            for uuid_str in self.format_to_str_uuid(default_uuid4)
        )

    async def matching(self: Self) -> Result[None, Exception]:
        """Monitoring of matching order.

        Start listen websocket
        """
        reconnect_delay = 1
        max_reconnect_delay = 60
        while True:
            try:
                logger.info("matching start")
                match await do_async(
                    Ok(None)
                    for bullet_private in await self.get_api_v1_bullet_private()
                    for url_ws in self.get_url_for_websocket(bullet_private)
                    for ping_interval in self.get_ping_interval_for_websocket(
                        bullet_private,
                    )
                    for ping_timeout in self.get_ping_timeout_for_websocket(
                        bullet_private,
                    )
                    for ws in self.get_websocket(url_ws, ping_interval, ping_timeout)
                    for msg_subscribe_matching in self.get_msg_for_subscribe_matching()
                    for _ in await self.runtime_matching_ws(
                        ws,
                        msg_subscribe_matching,
                    )
                ):
                    case Err(exc):
                        logger.exception(exc)
                        await self.send_telegram_msg(
                            "Drop matching websocket: see logs",
                        )
            except (
                ConnectionResetError,
                websockets_exceptions.ConnectionClosed,
                TimeoutError,
                websockets_exceptions.WebSocketException,
                socket.gaierror,
                ConnectionRefusedError,
                SSLError,
                OSError,
            ) as exc:
                logger.exception(exc)
                await self.send_telegram_msg("Drop matching websocket: see logs")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            except Exception as exc:  # noqa: BLE001
                logger.exception(exc)
                await self.send_telegram_msg("Unexpected error in matching: see logs")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    async def candle(self: Self) -> Result[None, Exception]:
        """Monitoring of candle.

        Start listen websocket
        """
        reconnect_delay = 1
        max_reconnect_delay = 60
        while True:
            try:
                logger.info("candle start")
                match await do_async(
                    Ok(None)
                    for bullet_public in await self.get_api_v1_bullet_public()
                    for url_ws in self.get_url_for_websocket(bullet_public)
                    for ping_interval in self.get_ping_interval_for_websocket(
                        bullet_public,
                    )
                    for ping_timeout in self.get_ping_timeout_for_websocket(
                        bullet_public,
                    )
                    for ws in self.get_websocket(url_ws, ping_interval, ping_timeout)
                    for _ in await self.runtime_candle_ws(
                        ws,
                    )
                ):
                    case Err(exc):
                        logger.exception(exc)
                        await self.send_telegram_msg(
                            "Drop candle websocket: see logs",
                        )
            except (
                ConnectionResetError,
                websockets_exceptions.ConnectionClosed,
                TimeoutError,
                websockets_exceptions.WebSocketException,
                socket.gaierror,
                ConnectionRefusedError,
                SSLError,
                OSError,
            ) as exc:
                logger.exception(exc)
                await self.send_telegram_msg("Drop matching websocket: see logs")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            except Exception as exc:  # noqa: BLE001
                logger.exception(exc)
                await self.send_telegram_msg("Unexpected error in matching: see logs")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    def complete_margin_order(
        self: Self,
        side: str,
        symbol: str,
        price: str,
        size: str,
    ) -> Result[dict[str, str | bool], Exception]:
        """Complete data for margin order.

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
        return do(
            Ok(
                {
                    "clientOid": client_id,
                    "side": side,
                    "symbol": symbol,
                    "price": price,
                    "size": size,
                    "type": "limit",
                    "timeInForce": "GTC",
                    "autoBorrow": True,
                    "autoRepay": True,
                },
            )
            for default_uuid4 in self.get_default_uuid4()
            for client_id in self.format_to_str_uuid(default_uuid4)
        )

    def save_order_id(
        self: Self,
        symbol: str,
        side: str,
        order_id: str,
    ) -> Result[None, Exception]:
        """Save order id."""
        if symbol in self.book:
            if side == "sell":
                self.book_orders[symbol]["sell"] = order_id
            else:
                self.book_orders[symbol]["buy"] = order_id

        return Ok(None)

    async def wrap_post_api_v3_hf_margin_order(
        self: Self,
        params_order_up: dict[str, str | bool],
    ) -> Result[ApiV3HfMarginOrderPOST.Res, Exception]:
        """."""
        match await do_async(
            Ok(order_id)
            for order_id in await self.post_api_v3_hf_margin_order(params_order_up)
        ):
            case Ok(order_id):
                return Ok(order_id)
            case Err(exc):
                return Err(exc)
        error_msg = (
            f"Unexpected error with post_api_v3_hf_margin_order:{params_order_up}"
        )
        await self.send_telegram_msg(error_msg)
        return Err(Exception(error_msg))

    async def make_order_and_save_result(
        self: Self,
        ticket: str,
        params_order: dict[str, dict[str, str | bool]],
        side: str,
    ) -> Result[None, Exception]:
        """."""
        match await do_async(
            Ok(None)
            for order_id in await self.wrap_post_api_v3_hf_margin_order(
                params_order[side]
            )
            for _ in self.save_order_id(ticket, side, order_id.data.orderId)
        ):
            case Err(exc):
                logger.exception(exc)
        return Ok(None)

    async def make_updown_margin_order(
        self: Self,
        ticket: str,
    ) -> Result[None, Exception]:
        """Make up and down limit order."""
        match await do_async(
            Ok({"buy": params_order_down, "sell": params_order_up})
            # for up
            for order_up in self.calc_up(ticket)
            for params_order_up in self.complete_margin_order(
                side=order_up.side,
                symbol=f"{ticket}-USDT",
                price=order_up.price,
                size=order_up.size,
            )
            # for down
            for order_down in self.calc_down(ticket)
            for params_order_down in self.complete_margin_order(
                side=order_down.side,
                symbol=f"{ticket}-USDT",
                price=order_down.price,
                size=order_down.size,
            )
        ):
            case Ok(params_order):
                async with asyncio.TaskGroup() as tg:
                    if self.book[ticket].borrow > 0:
                        # has borrow of ticket
                        tg.create_task(
                            self.make_order_and_save_result(
                                ticket,
                                params_order,
                                "buy",
                            )
                        )
                    tg.create_task(
                        self.make_order_and_save_result(ticket, params_order, "sell")
                    )

            case Err(exc):
                await self.send_telegram_msg(f"{exc}")
                logger.exception(exc)
        return Ok(None)

    async def start_up_orders(self: Self) -> Result[None, Exception]:
        """Make init orders."""
        # wait while matcher and balancer would be ready
        await asyncio.sleep(5)

        for ticket in self.book:
            await self.make_updown_margin_order(ticket)

        return Ok(None)

    def fill_balance_to_current_token(
        self: Self,
        symbol: str,
        balance: Decimal,
    ) -> Result[None, Exception]:
        """Fill balance current symbol."""
        try:
            self.book[symbol].balance = balance
        except IndexError as exc:
            return Err(exc)
        return Ok(None)

    def fill_borrow_to_current_token(
        self: Self,
        symbol: str,
        borrow: Decimal,
    ) -> Result[None, Exception]:
        """Fill borrow current symbol."""
        try:
            self.book[symbol].borrow = borrow
        except IndexError as exc:
            return Err(exc)
        return Ok(None)

    def fill_balance_all_tokens(
        self: Self,
        data: list[ApiV1AccountsGET.Res.Data],
    ) -> Result[None, Exception]:
        """Fill balance to all tokens in book."""
        for ticket in data:
            match do(
                Ok(None)
                for balance_decimal in self.data_to_decimal(ticket.balance)
                for _ in self.fill_balance_to_current_token(
                    ticket.currency,
                    balance_decimal,
                )
            ):
                case Err(exc):
                    return Err(exc)

        return Ok(None)

    def fill_borrow_all_tokens(
        self: Self,
        data: list[ApiV3MarginAccountsGET.Res.Data.Account],
    ) -> Result[None, Exception]:
        """Fill borrow to all tokens in book."""
        for ticket in data:
            match do(
                Ok(None)
                for borrow_decimal in self.data_to_decimal(ticket.liability)
                for _ in self.fill_borrow_to_current_token(
                    ticket.currency,
                    borrow_decimal,
                )
            ):
                case Err(exc):
                    return Err(exc)

        return Ok(None)

    def filter_ticket_by_book_balance(
        self: Self,
        data: ApiV1AccountsGET.Res,
    ) -> Result[list[ApiV1AccountsGET.Res.Data], Exception]:
        """."""
        return Ok([ticket for ticket in data.data if ticket.currency in self.book])

    def filter_ticket_by_book_borrow(
        self: Self,
        data: ApiV3MarginAccountsGET.Res,
    ) -> Result[list[ApiV3MarginAccountsGET.Res.Data.Account], Exception]:
        """."""
        return Ok(
            [ticket for ticket in data.data.accounts if ticket.currency in self.book]
        )

    async def fill_balance(self: Self) -> Result[None, Exception]:
        """Fill all balance by ENVs."""
        return await do_async(
            Ok(None)
            for balance_accounts in await self.get_api_v1_accounts(
                params={"type": "margin"},
            )
            for ticket_to_fill in self.filter_ticket_by_book_balance(balance_accounts)
            for _ in self.fill_balance_all_tokens(ticket_to_fill)
        )

    async def fill_borrow(self: Self) -> Result[None, Exception]:
        """Fill all borrow by ENVs."""
        return await do_async(
            Ok(None)
            for borrow_balance_accounts in await self.get_api_v3_margin_accounts(
                params={
                    "quoteCurrency": "USDT",
                },
            )
            for borrow_to_fill in self.filter_ticket_by_book_borrow(
                borrow_balance_accounts
            )
            for _ in self.fill_borrow_all_tokens(borrow_to_fill)
        )

    def fill_one_symbol_base_increment(
        self: Self,
        symbol: str,
        base_increment: Decimal,
    ) -> Result[None, Exception]:
        """."""
        try:
            self.book[symbol].baseincrement = base_increment
        except IndexError as exc:
            return Err(exc)
        return Ok(None)

    # nu cho jopki kak dila
    def fill_all_base_increment(
        self: Self,
        data: list[ApiV2SymbolsGET.Res.Data],
    ) -> Result[None, Exception]:
        """Fill base increment by each token."""
        for ticket in data:
            match do(
                Ok(None)
                for base_increment_decimal in self.data_to_decimal(ticket.baseIncrement)
                for _ in self.fill_one_symbol_base_increment(
                    ticket.baseCurrency,
                    base_increment_decimal,
                )
            ):
                case Err(exc):
                    return Err(exc)

        return Ok(None)

    def fill_one_symbol_price_increment(
        self: Self,
        symbol: str,
        price_increment: Decimal,
    ) -> Result[None, Exception]:
        """."""
        try:
            self.book[symbol].priceincrement = price_increment
        except IndexError as exc:
            return Err(exc)
        return Ok(None)

    def fill_all_price_increment(
        self: Self,
        data: list[ApiV2SymbolsGET.Res.Data],
    ) -> Result[None, Exception]:
        """Fill price increment by each token."""
        for ticket in data:
            match do(
                Ok(None)
                for price_increment_decimal in self.data_to_decimal(
                    ticket.priceIncrement
                )
                for _ in self.fill_one_symbol_price_increment(
                    ticket.baseCurrency,
                    price_increment_decimal,
                )
            ):
                case Err(exc):
                    return Err(exc)
        return Ok(None)

    def filter_ticket_by_book_increment(
        self: Self,
        data: ApiV2SymbolsGET.Res,
    ) -> Result[list[ApiV2SymbolsGET.Res.Data], Exception]:
        """."""
        return Ok(
            [
                out_side_ticket
                for out_side_ticket in data.data
                if out_side_ticket.baseCurrency in self.book
                and out_side_ticket.quoteCurrency == "USDT"
            ]
        )

    async def fill_increment(self: Self) -> Result[None, Exception]:
        """Fill increment from api."""
        return await do_async(
            Ok(None)
            for ticket_info in await self.get_api_v2_symbols()
            for ticket_for_fill in self.filter_ticket_by_book_increment(ticket_info)
            for _ in self.fill_all_base_increment(ticket_for_fill)
            for _ in self.fill_all_price_increment(ticket_for_fill)
        )

    def filter_ticket_by_book_last_price(
        self: Self,
        data: ApiV1MarketAllTickers.Res,
    ) -> Result[list[ApiV1MarketAllTickers.Res.Data.Ticker], Exception]:
        """."""
        return Ok(
            [
                ticket
                for ticket in data.data.ticker
                if ticket.symbol.replace("-USDT", "") in self.book and ticket.buy
            ]
        )

    def fill_one_ticket_last_price(
        self: Self,
        symbol: str,
        last_price: Decimal,
    ) -> Result[None, Exception]:
        """."""
        try:
            self.book[symbol].last_price = last_price
        except IndexError as exc:
            return Err(exc)
        return Ok(None)

    def fill_all_last_price(
        self: Self,
        data: list[ApiV1MarketAllTickers.Res.Data.Ticker],
    ) -> Result[None, Exception]:
        """Fill last price for each token."""
        for ticket in data:
            match do(
                Ok(None)
                for last_price_decimal in self.data_to_decimal(ticket.buy or "")
                for replaced_symbol in self.replace_quote_in_symbol_name(ticket.symbol)
                for _ in self.fill_one_ticket_last_price(
                    replaced_symbol,
                    last_price_decimal,
                )
            ):
                case Err(exc):
                    return Err(exc)

        return Ok(None)

    async def fill_last_price(self: Self) -> Result[None, Exception]:
        """Fill last price for first order init."""
        return await do_async(
            Ok(None)
            for market_ticket in await self.get_api_v1_market_all_tickers()
            for ticket_for_fill in self.filter_ticket_by_book_last_price(market_ticket)
            for _ in self.fill_all_last_price(ticket_for_fill)
        )

    def divide(
        self: Self,
        divider: Decimal,
        divisor: Decimal,
    ) -> Result[Decimal, Exception]:
        """Devide."""
        if divisor == Decimal("0"):
            return Err(ZeroDivisionError("Divisor cannot be zero"))
        return Ok(divider / divisor)

    def quantize_minus(
        self: Self,
        data: Decimal,
        increment: Decimal,
    ) -> Result[Decimal, Exception]:
        """Quantize to down."""
        return Ok(data.quantize(increment, ROUND_DOWN))

    def quantize_plus(
        self: Self,
        data: Decimal,
        increment: Decimal,
    ) -> Result[Decimal, Exception]:
        """Quantize to up."""
        return Ok(data.quantize(increment, ROUND_UP))

    def calc_size(
        self: Self,
        balance: Decimal,
        need_balance: Decimal,
    ) -> Result[Decimal, Exception]:
        """Calc size token for limit order."""
        if balance > need_balance:
            return Ok(balance - need_balance)
        return Ok(need_balance - balance)

    def choise_side(
        self: Self,
        balance: Decimal,
        need_balance: Decimal,
    ) -> Result[str, Exception]:
        """Choise sell or buy side."""
        if balance > need_balance:
            return Ok("sell")
        return Ok("buy")

    def calc_up(
        self: Self,
        ticket: str,
    ) -> Result[OrderParam, Exception]:
        """Calc up price and size tokens."""
        return do(
            Ok(
                OrderParam(
                    side="sell",
                    price=last_price_str,
                    size=size_str,
                ),
            )
            for last_price in self.plus_1_percent(self.book[ticket].last_price)
            for last_price_quantize in self.quantize_plus(
                last_price,
                self.book[ticket].priceincrement,
            )
            for last_price_str in self.decimal_to_str(last_price_quantize)
            for raw_size in self.divide(
                self.BASE_KEEP * Decimal("1.01"),
                last_price_quantize,
            )
            for size in self.quantize_plus(
                raw_size,
                self.book[ticket].baseincrement,
            )
            for size_str in self.decimal_to_str(size)
        )

    def calc_down(
        self: Self,
        ticket: str,
    ) -> Result[OrderParam, Exception]:
        """Calc down price and size tokens."""
        return do(
            Ok(
                OrderParam(
                    side="buy",
                    price=last_price_str,
                    size=size_str,
                ),
            )
            for last_price in self.minus_1_percent(self.book[ticket].last_price)
            for last_price_quantize in self.quantize_minus(
                last_price,
                self.book[ticket].priceincrement,
            )
            for last_price_str in self.decimal_to_str(last_price_quantize)
            for raw_size in self.divide(self.BASE_KEEP, last_price_quantize)
            for size in self.quantize_plus(
                raw_size,
                self.book[ticket].baseincrement,
            )
            for size_str in self.decimal_to_str(size)
        )

    def plus_1_percent(self: Self, data: Decimal) -> Result[Decimal, Exception]:
        """Current price plus 1 percent."""
        try:
            if data < Decimal("0"):
                return Err(ValueError("data is negative"))
            result = data * Decimal("1.01")
            return Ok(result)
        except InvalidOperation as exc:
            return Err(exc)

    def minus_1_percent(self: Self, data: Decimal) -> Result[Decimal, Exception]:
        """Current price minus 1 percent."""
        return Ok(data * Decimal("0.99"))

    async def insert_data_to_db(
        self: Self,
        data: OrderChangeV2.Res.Data,
    ) -> Result[None, Exception]:
        """Insert data to db."""
        try:
            async with self.pool.acquire() as conn, conn.transaction():
                # Run the query passing the request argument.
                await conn.execute(
                    """INSERT INTO main(exchange, symbol, side, size, price, date) VALUES($1, $2, $3, $4, $5, $6)""",
                    "kucoin",
                    data.symbol,
                    data.side,
                    data.size or "",
                    data.price,
                    datetime.now(),  # noqa: DTZ005
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(exc)
        return Ok(None)

    async def create_db_pool(self: Self) -> Result[None, Exception]:
        """Create Postgresql connection pool."""
        try:
            self.pool: Pool[Record] = await create_pool(
                user=self.PG_USER,
                password=self.PG_PASSWORD,
                database=self.PG_DATABASE,
                host=self.PG_HOST,
                port=self.PG_PORT,
                timeout=5,
            )
            return Ok(None)
        except ConnectionRefusedError as exc:
            return Err(exc)

    def to_str(self: Self, data: float) -> Result[str, Exception]:
        """."""
        try:
            return Ok(str(data))
        except TypeError as exc:
            return Err(exc)

    async def get_all_open_orders(
        self: Self,
    ) -> Result[list[ApiV1OrdersGET.Res.Data.Item], Exception]:
        """Get all open orders."""
        open_orders: list[ApiV1OrdersGET.Res.Data.Item] = []
        pagesize = 500
        currentpage = 1
        while True:
            match await do_async(
                Ok(orders_for_cancel)
                for page_size_str in self.to_str(pagesize)
                for current_page_str in self.to_str(currentpage)
                for orders_for_cancel in await self.get_api_v1_orders(
                    params={
                        "status": "active",
                        "tradeType": "MARGIN_TRADE",
                        "pageSize": page_size_str,
                        "currentPage": current_page_str,
                    },
                )
            ):
                case Ok(res):
                    if len(res.data.items) == 0:
                        break

                    currentpage += 1
                    open_orders += res.data.items
                case Err(exc):
                    return Err(exc)
        return Ok(open_orders)

    def filter_open_order_by_symbol(
        self: Self,
        data: list[ApiV1OrdersGET.Res.Data.Item],
    ) -> Result[list[str], Exception]:
        """Filted open order by exist in book."""
        return Ok(
            [
                order.id
                for order in data
                if order.symbol.replace("-USDT", "") in self.book
            ]
        )

    def filter_open_order_by_symbol_for_compare(
        self: Self,
        data: list[ApiV1OrdersGET.Res.Data.Item],
    ) -> Result[dict[str, list[str]], Exception]:
        """Filted open order by exist in book."""
        result: dict[str, list[str]] = {}

        for order in data:
            symbol = order.symbol.replace("-USDT", "")
            if symbol in self.book:
                if symbol in result:
                    result[symbol].append(order.id)
                else:
                    result[symbol] = [order.id]

        return Ok(result)

    async def pre_init(self: Self) -> Result[Self, Exception]:
        """Pre-init.

        get all open orders
        close all open orders
        get balance by  all tickets
        get increment by all tickets
        """
        return await do_async(
            Ok(self)
            for _ in self.init_envs()
            for _ in await self.create_db_pool()
            for _ in self.create_book()
            for open_orders in await self.get_all_open_orders()
            for orders_for_cancel in self.filter_open_order_by_symbol(open_orders)
            for _ in await self.massive_cancel_order(orders_for_cancel)
            for _ in await self.sleep_to(sleep_on=5)
            for _ in await self.fill_balance()
            for _ in await self.fill_increment()
            for _ in await self.fill_last_price()
            for _ in await self.fill_borrow()
            for _ in self.logger_success(self.book)
        )

    async def sleep_to(self: Self, *, sleep_on: int = 1) -> Result[None, Exception]:
        """."""
        await asyncio.sleep(sleep_on)
        return Ok(None)

    def filter_balance_by_symbol(
        self: Self,
        symbol: str,
        data: ApiV1AccountsGET.Res,
    ) -> Result[ApiV1AccountsGET.Res.Data, Exception]:
        """."""
        for ticket in data.data:
            if symbol == ticket.currency:
                return Ok(ticket)
        return Err(Exception(f"Can't find {symbol=} in {data.data}"))

    def filter_last_price_by_symbol(
        self: Self,
        symbol: str,
        data: ApiV1MarketAllTickers.Res,
    ) -> Result[ApiV1MarketAllTickers.Res.Data.Ticker, Exception]:
        """."""
        for ticket in data.data.ticker:
            if symbol == ticket.symbol.replace("-USDT", ""):
                return Ok(ticket)
        return Err(Exception(f"Can't find {symbol=} in {data.data.ticker}"))

    async def reclaim_orders(self: Self) -> Result[None, Exception]:
        """UpKeep always 2 order (sell and buy)."""
        perfect_count_orders = 2
        await asyncio.sleep(60)
        while True:
            match await do_async(
                Ok(orders_for_compare)
                for open_orders in await self.get_all_open_orders()
                for orders_for_compare in self.filter_open_order_by_symbol_for_compare(
                    open_orders
                )
            ):
                case Ok(orders_for_compare):
                    for symbol, loses_orders in orders_for_compare.items():
                        if len(loses_orders) != perfect_count_orders:
                            await do_async(
                                Ok(None)
                                for _ in await self.massive_cancel_order(loses_orders)
                                for _ in await self.sleep_to(sleep_on=5)
                                # update balance
                                for balance_accounts in await self.get_api_v1_accounts(
                                    params={"type": "margin"},
                                )
                                for current_symbol_balance in self.filter_balance_by_symbol(
                                    symbol,
                                    balance_accounts,
                                )
                                for balance_decimal in self.data_to_decimal(
                                    current_symbol_balance.balance
                                )
                                for _ in self.fill_balance_to_current_token(
                                    symbol,
                                    balance_decimal,
                                )
                                # update last price
                                for market_ticket in await self.get_api_v1_market_all_tickers()
                                for current_symbol_last_price in self.filter_last_price_by_symbol(
                                    symbol,
                                    market_ticket,
                                )
                                for last_price_decimal in self.data_to_decimal(
                                    current_symbol_last_price.buy or ""
                                )
                                for replased_symbol in self.replace_quote_in_symbol_name(
                                    current_symbol_last_price.symbol
                                )
                                for _ in self.fill_one_ticket_last_price(
                                    replased_symbol,
                                    last_price_decimal,
                                )
                                for _ in await self.fill_last_price()
                                for _ in await self.make_updown_margin_order(symbol)
                            )

            await asyncio.sleep(60)

    async def infinity_task(self: Self) -> Result[None, Exception]:
        """Infinity run tasks."""
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self.candle()),
                tg.create_task(self.alertest()),
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
        for _ in await kcn.send_telegram_msg("KuCoin settings are OK!")
        for _ in await kcn.infinity_task()
    ):
        case Ok(None):
            pass
        case Err(exc):
            logger.exception(exc)
            return Err(exc)
    return Ok(None)


if __name__ == "__main__":
    """Main enter."""
    asyncio.run(main())
