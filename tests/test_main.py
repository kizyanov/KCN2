#!/usr/bin/env python
"""Test for main file."""

# ruff: noqa:  S105 S101 PT006
import os
from decimal import Decimal

import pytest
from result import Ok

from main import KCN


@pytest.fixture(autouse=True)
def set_env_vars():  # noqa: ANN201
    """."""
    env_vars = {
        "KEY": "USDC",
        "SECRET": "USDC",
        "PASSPHRASE": "USDC",
        "BASE_URL": "USDC",
        "ALLCURRENCY": "USDC",
        "IGNORECURRENCY": "USDC",
        "BASE_KEEP": "1000",
        "TELEGRAM_BOT_API_KEY": "USDC",
        "TELEGRAM_BOT_CHAT_ID": "USDC",
        "PG_USER": "USDC",
        "PG_PASSWORD": "USDC",
        "PG_DATABASE": "USDC",
        "PG_HOST": "USDC",
        "PG_PORT": "USDC",
    }

    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value

    yield

    # Clean up environment variables
    for key in env_vars:
        os.environ.pop(key, None)


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        ("-USDT", ""),
        ("BTC-USDT", "BTC"),
        ("BTC", "BTC"),
    ],
)
def test_replace_usdt_symbol_name(input_value: str, expected_output: str) -> None:
    """."""
    result = KCN().replace_usdt_symbol_name(input_value)
    match result:
        case Ok(res):
            assert (
                res == expected_output
            ), f"Expected '{expected_output}', but got '{res}'"
        case _:
            pytest.fail(f"Expected Ok('{expected_output}'), but got {result}")


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        (Decimal("1"), Decimal("1.01")),
        (Decimal("0.00116"), Decimal("0.0011716")),
        (Decimal("0"), Decimal("0")),
    ],
)
def test_plus_1_percent(input_value: Decimal, expected_output: Decimal) -> None:
    """."""
    result = KCN().plus_1_percent(input_value)
    match result:
        case Ok(res):
            assert (
                res == expected_output
            ), f"Expected '{expected_output}', but got '{res}'"
        case _:
            pytest.fail(f"Expected Ok('{expected_output}'), but got {result}")


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        (Decimal("1"), Decimal(".99")),
        (Decimal("0.00116"), Decimal("0.0011484")),
        (Decimal("0"), Decimal("0")),
    ],
)
def test_minus_1_percent(input_value: Decimal, expected_output: Decimal) -> None:
    """."""
    result = KCN().minus_1_percent(input_value)
    match result:
        case Ok(res):
            assert (
                res == expected_output
            ), f"Expected '{expected_output}', but got '{res}'"
        case _:
            pytest.fail(f"Expected Ok('{expected_output}'), but got {result}")


@pytest.mark.parametrize(
    "divider_input_value, divisor_input_value, expected_output",
    [
        (Decimal("100"), Decimal("1"), Decimal("100")),
        (Decimal("1"), Decimal("100"), Decimal(".01")),
        (Decimal("2"), Decimal("3"), Decimal("0.6666666666666666666666666667")),
    ],
)
def test_divide(
    divider_input_value: Decimal,
    divisor_input_value: Decimal,
    expected_output: Decimal,
) -> None:
    """."""
    result = KCN().divide(divider_input_value, divisor_input_value)
    match result:
        case Ok(res):
            assert (
                res == expected_output
            ), f"Expected '{expected_output}', but got '{res}'"
        case _:
            pytest.fail(f"Expected Ok('{expected_output}'), but got {result}")


@pytest.mark.parametrize(
    "data, increment, expected_output",
    [
        (Decimal("0.110"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.111"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.112"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.113"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.114"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.115"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.116"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.117"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.118"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.119"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.120"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.121"), Decimal("0.01"), Decimal("0.13")),
    ],
)
def test_quantize_plus(
    data: Decimal,
    increment: Decimal,
    expected_output: Decimal,
) -> None:
    """."""
    result = KCN().quantize_plus(data, increment)
    match result:
        case Ok(res):
            assert (
                res == expected_output
            ), f"Expected '{expected_output}', but got '{res}'"
        case _:
            pytest.fail(f"Expected Ok('{expected_output}'), but got {result}")


@pytest.mark.parametrize(
    "data, increment, expected_output",
    [
        (Decimal("0.110"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.111"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.112"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.113"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.114"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.115"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.116"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.117"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.118"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.119"), Decimal("0.01"), Decimal("0.11")),
        (Decimal("0.120"), Decimal("0.01"), Decimal("0.12")),
        (Decimal("0.121"), Decimal("0.01"), Decimal("0.12")),
    ],
)
def test_quantize_minus(
    data: Decimal,
    increment: Decimal,
    expected_output: Decimal,
) -> None:
    """."""
    result = KCN().quantize_minus(data, increment)
    match result:
        case Ok(res):
            assert (
                res == expected_output
            ), f"Expected '{expected_output}', but got '{res}'"
        case _:
            pytest.fail(f"Expected Ok('{expected_output}'), but got {result}")
