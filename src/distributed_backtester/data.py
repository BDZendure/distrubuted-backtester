"""Strict raw USD CSV loading with provenance from the parsed byte snapshot."""

import csv
import re
from datetime import date
from hashlib import sha256
from io import StringIO
from pathlib import Path
from typing import Literal

from distributed_backtester.models import Bar, DatasetMetadata, MarketDataset
from distributed_backtester.money import parse_money

_COLUMNS = ("date", "open", "high", "low", "close", "volume")
_SESSION_TEXT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
_VOLUME_TEXT = re.compile(r"[0-9]+\Z")


class DataValidationError(ValueError):
    """The source cannot be loaded as a valid Phase 1 market dataset."""


def _invalid(path: Path, row: int, field: str, reason: str) -> DataValidationError:
    return DataValidationError(f"{path}: row {row}, field {field}: {reason}")


def _parse_bar(path: Path, row: int, fields: dict[str, str]) -> Bar:
    for field, value in fields.items():
        if not value.strip():
            raise _invalid(path, row, field, "blank fields are not allowed")

    session_text = fields["date"]
    if not _SESSION_TEXT.fullmatch(session_text):
        raise _invalid(path, row, "date", "expected an ASCII YYYY-MM-DD session date")
    try:
        session = date.fromisoformat(session_text)
    except ValueError as error:
        raise _invalid(path, row, "date", f"invalid session date: {error}") from error

    prices: dict[str, int] = {}
    for field in ("open", "high", "low", "close"):
        try:
            price = parse_money(fields[field])
        except ValueError as error:
            raise _invalid(path, row, field, str(error)) from error
        if price <= 0:
            raise _invalid(path, row, field, "price must be positive")
        prices[field] = price

    volume_text = fields["volume"]
    if not _VOLUME_TEXT.fullmatch(volume_text):
        raise _invalid(path, row, "volume", "expected a nonnegative ASCII integer")
    try:
        volume = int(volume_text)
    except ValueError as error:
        raise _invalid(path, row, "volume", f"invalid integer: {error}") from error

    try:
        return Bar(
            session=session,
            open=prices["open"],
            high=prices["high"],
            low=prices["low"],
            close=prices["close"],
            volume=volume,
        )
    except ValueError as error:
        raise _invalid(path, row, "OHLC", str(error)) from error


def load_csv(path: Path, *, symbol: str, price_basis: Literal["raw"]) -> MarketDataset:
    """Load exact, chronological bars without skipping rows or repairing inputs.

    The digest includes any UTF-8 BOM and original line endings. Row diagnostics
    use physical source lines, ending at ``csv.reader.line_num`` for a multiline
    record. Currency is fixed to USD, and the caller must declare raw prices.
    """
    if price_basis != "raw":
        raise _invalid(path, 1, "price_basis", "only raw prices are supported")

    try:
        snapshot = path.read_bytes()
    except OSError as error:
        raise DataValidationError(f"{path}: cannot read CSV source: {error}") from error

    return _load_csv_snapshot(path, snapshot, symbol=symbol, price_basis=price_basis)


def _load_csv_snapshot(
    path: Path, snapshot: bytes, *, symbol: str, price_basis: Literal["raw"]
) -> MarketDataset:
    """Hash and parse the same immutable bytes, including cache lookups."""
    if price_basis != "raw":
        raise _invalid(path, 1, "price_basis", "only raw prices are supported")

    try:
        metadata = DatasetMetadata(
            symbol=symbol,
            source_sha256=sha256(snapshot).hexdigest(),
            price_basis=price_basis,
        )
    except ValueError as error:
        raise _invalid(path, 1, "symbol", str(error)) from error

    try:
        text = snapshot.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise DataValidationError(
            f"{path}: invalid UTF-8 CSV source: {error}"
        ) from error

    reader = csv.reader(StringIO(text, newline=""), strict=True)
    try:
        header = next(reader)
    except StopIteration:
        raise _invalid(path, 1, "header", "CSV source is empty") from None
    except csv.Error as error:
        raise _invalid(path, max(1, reader.line_num), "header", str(error)) from error

    if len(header) != len(set(header)):
        raise _invalid(
            path, reader.line_num, "header", "duplicate columns are not allowed"
        )
    if set(header) != set(_COLUMNS):
        missing = sorted(set(_COLUMNS) - set(header))
        unknown = sorted(set(header) - set(_COLUMNS))
        raise _invalid(
            path,
            reader.line_num,
            "header",
            f"expected exactly {','.join(_COLUMNS)}; "
            f"missing {missing}; unknown {unknown}",
        )

    bars: list[Bar] = []
    try:
        for values in reader:
            row = reader.line_num
            if not values:
                raise _invalid(path, row, "row", "blank rows are not allowed")
            if len(values) != len(header):
                raise _invalid(
                    path,
                    row,
                    "row",
                    f"expected {len(header)} columns, found {len(values)}",
                )
            bar = _parse_bar(path, row, dict(zip(header, values, strict=True)))
            if bars and bar.session <= bars[-1].session:
                raise _invalid(
                    path, row, "date", "sessions must be unique and strictly increasing"
                )
            bars.append(bar)
    except csv.Error as error:
        raise _invalid(path, reader.line_num, "row", str(error)) from error

    if not bars:
        raise _invalid(
            path, reader.line_num, "row", "CSV contains a header but no bars"
        )
    return MarketDataset(bars=tuple(bars), metadata=metadata)
