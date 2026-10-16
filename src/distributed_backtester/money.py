"""Exact USD micro-dollar boundaries and integer half-even rounding."""

import re

MICRO_DOLLARS = 1_000_000
_DECIMAL_TEXT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\Z")


def require_integer(value: int, name: str, *, minimum: int = 0) -> None:
    """Reject bool and other numeric types rather than silently coercing them."""
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")


def parse_money(text: str) -> int:
    """Parse ordinary decimal text exactly, without binary floats or a context."""
    value = text.strip()
    if not _DECIMAL_TEXT.fullmatch(value):
        raise ValueError("money must be finite ordinary decimal text")
    sign = -1 if value.startswith("-") else 1
    unsigned = value.lstrip("+-")
    whole, _, fraction = unsigned.partition(".")
    significant_fraction = fraction.rstrip("0")
    if len(significant_fraction) > 6:
        raise ValueError("money must be exactly representable in micro-dollars")
    units = int(whole or "0") * MICRO_DOLLARS
    units += int(significant_fraction.ljust(6, "0"))
    return sign * units


def round_half_even(numerator: int, denominator: int) -> int:
    """Round a signed integer ratio to its nearest integer, ties to even."""
    require_integer(denominator, "denominator", minimum=1)
    if type(numerator) is not int:
        raise ValueError("numerator must be an integer")
    quotient, remainder = divmod(abs(numerator), denominator)
    if 2 * remainder > denominator or (2 * remainder == denominator and quotient % 2):
        quotient += 1
    return -quotient if numerator < 0 else quotient


def _format_scaled(value: int, places: int) -> str:
    sign = "-" if value < 0 else ""
    whole, fraction = divmod(abs(value), 10**places)
    return f"{sign}{whole}.{fraction:0{places}d}"


def format_money(value: int) -> str:
    """Six fixed decimal places, preserving the complete monetary ledger."""
    if type(value) is not int:
        raise ValueError("money units must be an integer")
    return _format_scaled(value, 6)


def format_ratio(numerator: int, denominator: int) -> str:
    """Twelve decimal places from exact integers, including normalized zero."""
    return _format_scaled(round_half_even(numerator * 10**12, denominator), 12)
