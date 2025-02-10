"""Test for main file."""

import pytest
from result import Ok

from main import KCN


@pytest.mark.parametrize(("test_input", "expected"), [("", b"")])
def test_encrypt_encode(test_input: str, expected: bytes) -> None:
    """Test."""
    match KCN().encode(test_input):
        case Ok(value):
            if value != expected:
                pytest.fail(f"{value=} != {expected=}")


@pytest.mark.parametrize(("test_input", "expected"), [(b"", "")])
def test_encrypt_decode(test_input: bytes, expected: str) -> None:
    """Test."""
    match KCN().decode(test_input):
        case Ok(value):
            if value != expected:
                pytest.fail(f"{value=} != {expected=}")
