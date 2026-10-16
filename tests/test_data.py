"""Strict CSV inputs and provenance from one immutable byte snapshot."""

import csv
from datetime import date
from hashlib import sha256
from io import StringIO
from pathlib import Path
from typing import Literal, cast

import pytest

from distributed_backtester.data import DataValidationError, load_csv
from distributed_backtester.models import Bar, DatasetMetadata, MarketDataset

HEADER = "date,open,high,low,close,volume\n"
VALID_ROW = "2026-01-02,10,12,9,11,100\n"


def write_csv(tmp_path: Path, content: str | bytes) -> Path:
    path = tmp_path / "prices.csv"
    path.write_bytes(content.encode("utf-8") if isinstance(content, str) else content)
    return path


def row_with(field: str, value: str) -> str:
    fields = VALID_ROW.rstrip("\n").split(",")
    fields[HEADER.rstrip("\n").split(",").index(field)] = value
    output = StringIO(newline="")
    csv.writer(output, lineterminator="\n").writerow(fields)
    return output.getvalue()


def assert_invalid(path: Path, *, row: int, field: str) -> DataValidationError:
    with pytest.raises(DataValidationError) as caught:
        load_csv(path, symbol="SYNTH", price_basis="raw")
    message = str(caught.value)
    assert str(path) in message
    assert f"row {row}" in message
    assert f"field {field}" in message
    return caught.value


def test_load_csv_preserves_exact_prices_and_provenance(tmp_path: Path) -> None:
    original = (
        b"date,open,high,low,close,volume\n"
        b"2026-01-02,10.000001,12,9,11.1,100\n"
        b"2026-01-05,12,13,11,12.5,0\n"
    )
    path = tmp_path / "prices.csv"
    path.write_bytes(original)

    assert load_csv(path, symbol="SYNTH", price_basis="raw") == MarketDataset(
        bars=(
            Bar(date(2026, 1, 2), 10_000_001, 12_000_000, 9_000_000, 11_100_000, 100),
            Bar(date(2026, 1, 5), 12_000_000, 13_000_000, 11_000_000, 12_500_000, 0),
        ),
        metadata=DatasetMetadata(
            symbol="SYNTH", source_sha256=sha256(original).hexdigest()
        ),
    )


@pytest.mark.parametrize("bom", [b"", b"\xef\xbb\xbf"], ids=["utf8", "utf8-bom"])
@pytest.mark.parametrize("newline", [b"\n", b"\r\n", b"\r"])
@pytest.mark.parametrize("final_newline", [False, True])
def test_reordered_headers_and_original_byte_digest(
    tmp_path: Path, bom: bytes, newline: bytes, final_newline: bool
) -> None:
    original = bom + newline.join(
        [b"volume,close,date,low,high,open", b'"00100",11,2026-01-02,9,12,10']
    )
    if final_newline:
        original += newline

    dataset = load_csv(write_csv(tmp_path, original), symbol="SYNTH", price_basis="raw")

    assert dataset.bars == (
        Bar(date(2026, 1, 2), 10_000_000, 12_000_000, 9_000_000, 11_000_000, 100),
    )
    assert dataset.metadata.source_sha256 == sha256(original).hexdigest()
    assert dataset.metadata.currency == "USD"
    assert dataset.metadata.price_basis == "raw"
    assert dataset.metadata.validation_version == 1


def test_equivalent_csvs_keep_distinct_source_byte_digests(tmp_path: Path) -> None:
    originals = [
        (HEADER + VALID_ROW).encode(),
        b"\xef\xbb\xbf" + (HEADER + VALID_ROW).encode(),
    ]
    datasets = [
        load_csv(write_csv(tmp_path, original), symbol="SYNTH", price_basis="raw")
        for original in originals
    ]

    assert datasets[0].bars == datasets[1].bars
    assert datasets[0].metadata.source_sha256 != datasets[1].metadata.source_sha256
    assert [dataset.metadata.source_sha256 for dataset in datasets] == [
        sha256(original).hexdigest() for original in originals
    ]


def test_reads_one_snapshot_even_when_source_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = (HEADER + VALID_ROW).encode()
    replacement = (HEADER + row_with("close", "10")).encode()
    path = write_csv(tmp_path, original)
    read_bytes = Path.read_bytes
    reads = 0

    def read_then_replace(source: Path) -> bytes:
        nonlocal reads
        assert source == path
        reads += 1
        assert reads == 1, "the loader must only read the source once"
        snapshot = read_bytes(source)
        source.write_bytes(replacement)
        return snapshot

    monkeypatch.setattr(Path, "read_bytes", read_then_replace)

    dataset = load_csv(path, symbol="SYNTH", price_basis="raw")

    assert reads == 1
    assert read_bytes(path) == replacement
    assert dataset.bars[0].close == 11_000_000
    assert dataset.metadata.source_sha256 == sha256(original).hexdigest()
    assert dataset.metadata.source_sha256 != sha256(replacement).hexdigest()


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (" 10.000001 ", 10_000_001),
        ("+10.5", 10_500_000),
        (".5", 500_000),
        ("1.", 1_000_000),
        ("0.000001", 1),
        ("10.1234560000", 10_123_456),
        ("12345678901234567890.123456", 12_345_678_901_234_567_890_123_456),
    ],
)
def test_prices_use_exact_existing_money_parser(
    tmp_path: Path, text: str, expected: int
) -> None:
    row = f"2026-01-02,{text},12345678901234567891,0.000001,1,0\n"
    dataset = load_csv(
        write_csv(tmp_path, HEADER + row), symbol="SYNTH", price_basis="raw"
    )
    assert dataset.bars[0].open == expected


@pytest.mark.parametrize("volume", ["0", "00100", "18446744073709551616"])
def test_volume_is_a_nonnegative_ascii_integer(tmp_path: Path, volume: str) -> None:
    dataset = load_csv(
        write_csv(tmp_path, HEADER + row_with("volume", volume)),
        symbol="SYNTH",
        price_basis="raw",
    )
    assert dataset.bars[0].volume == int(volume)


def test_supplied_sessions_need_no_calendar_inference(tmp_path: Path) -> None:
    sessions = ["2024-02-29", "2024-03-02", "2026-01-05"]
    content = HEADER + "".join(row_with("date", session) for session in sessions)
    dataset = load_csv(write_csv(tmp_path, content), symbol="SYNTH", price_basis="raw")
    assert tuple(bar.session for bar in dataset.bars) == (
        date(2024, 2, 29),
        date(2024, 3, 2),
        date(2026, 1, 5),
    )


@pytest.mark.parametrize("content", [b"", b"\xef\xbb\xbf", HEADER.encode()])
def test_empty_and_header_only_files_are_invalid(
    tmp_path: Path, content: bytes
) -> None:
    assert_invalid(
        write_csv(tmp_path, content),
        row=1,
        field="header" if not content or content == b"\xef\xbb\xbf" else "row",
    )


@pytest.mark.parametrize(
    "header",
    [
        "date,open,high,low,close",
        "date,open,high,low,close,volume,adjusted_close",
        "date,open,high,low,close,close",
        "date,open,high,low,close,volume,volume",
        "date,open,high,low,close,turnover",
        "Date,open,high,low,close,volume",
        " date,open,high,low,close,volume",
        "date,open,high,low,close,volume ",
        "date,open,high,low,close,",
        "date,open,high,low,close,volume,currency",
        "date,open,high,low,close,volume,price_basis",
    ],
)
def test_schema_requires_exact_unique_headers(tmp_path: Path, header: str) -> None:
    assert_invalid(
        write_csv(tmp_path, header + "\n" + VALID_ROW), row=1, field="header"
    )


@pytest.mark.parametrize(
    ("content", "row", "field"),
    [
        ("\n" + HEADER + VALID_ROW, 1, "header"),
        (HEADER + "\n" + VALID_ROW, 2, "row"),
        (HEADER + VALID_ROW + "\n", 3, "row"),
        (HEADER + "   \n", 2, "row"),
        (HEADER + "2026-01-02,10,12,9,11\n", 2, "row"),
        (HEADER + "2026-01-02,10,12,9,11,100,extra\n", 2, "row"),
    ],
)
def test_blank_rows_and_wrong_column_counts_are_not_skipped(
    tmp_path: Path, content: str, row: int, field: str
) -> None:
    assert_invalid(write_csv(tmp_path, content), row=row, field=field)


@pytest.mark.parametrize("field", HEADER.rstrip("\n").split(","))
@pytest.mark.parametrize("blank", ["", " \t "])
def test_blank_fields_are_invalid(tmp_path: Path, field: str, blank: str) -> None:
    assert_invalid(
        write_csv(tmp_path, HEADER + row_with(field, blank)), row=2, field=field
    )


@pytest.mark.parametrize(
    "session",
    [
        "20260102",
        "2026-W01-5",
        "2026-1-02",
        "2026-01-2",
        "2026/01/02",
        "2026-01-02T00:00:00",
        "2026-01-02Z",
        "2026-01-02\n",
        " 2026-01-02",
        "2026-01-02 ",
        "\uff12\uff10\uff12\uff16-\uff10\uff11-\uff10\uff12",
        "\u0662\u0660\u0662\u0666-\u0660\u0661-\u0660\u0662",
        "0000-01-02",
        "2026-02-29",
        "2026-04-31",
        "2026-13-01",
        "2026-01-00",
    ],
)
def test_dates_require_strict_ascii_iso_session_labels(
    tmp_path: Path, session: str
) -> None:
    row = 3 if "\n" in session else 2
    assert_invalid(
        write_csv(tmp_path, HEADER + row_with("date", session)), row=row, field="date"
    )


@pytest.mark.parametrize("field", ["open", "high", "low", "close"])
@pytest.mark.parametrize(
    "price",
    [
        "NaN",
        "nan",
        "Infinity",
        "-Infinity",
        "inf",
        "0",
        "-0.000000",
        "-1",
        "1e1",
        "1_000",
        "1,000",
        "10.0000001",
        "0.0000001",
        "\uff11\uff10",
        "\u0661\u0660",
        "10\x00",
    ],
)
def test_invalid_prices_identify_the_source_field(
    tmp_path: Path, field: str, price: str
) -> None:
    assert_invalid(
        write_csv(tmp_path, HEADER + row_with(field, price)), row=2, field=field
    )


@pytest.mark.parametrize(
    "volume",
    [
        "-1",
        "-0",
        "+1",
        "1.0",
        "0.5",
        "1e2",
        "1_000",
        "1,000",
        "NaN",
        "inf",
        "\uff11\uff12",
        "\u0661\u0662",
        " 100",
        "100 ",
        "100\x00",
    ],
)
def test_invalid_volumes_are_never_coerced(tmp_path: Path, volume: str) -> None:
    assert_invalid(
        write_csv(tmp_path, HEADER + row_with("volume", volume)), row=2, field="volume"
    )


@pytest.mark.parametrize(
    ("field", "price"),
    [
        ("open", "8"),
        ("open", "13"),
        ("close", "8"),
        ("close", "13"),
        ("low", "13"),
        ("high", "8"),
    ],
)
def test_inconsistent_prices_report_ohlc(
    tmp_path: Path, field: str, price: str
) -> None:
    error = assert_invalid(
        write_csv(tmp_path, HEADER + row_with(field, price)), row=2, field="OHLC"
    )
    assert isinstance(error.__cause__, ValueError)


@pytest.mark.parametrize("session", ["2026-01-02", "2026-01-01"])
def test_duplicate_and_decreasing_sessions_are_rejected_without_sorting(
    tmp_path: Path, session: str
) -> None:
    content = HEADER + VALID_ROW + row_with("date", session)
    assert_invalid(write_csv(tmp_path, content), row=3, field="date")


@pytest.mark.parametrize(
    ("content", "row", "field"),
    [
        ('"date,open,high,low,close,volume\n', 1, "header"),
        (HEADER + '2026-01-02,"10,12,9,11,100\n', 2, "row"),
        (HEADER + '2026-01-02,"10"junk,12,9,11,100\n', 2, "row"),
        (HEADER + '2026-01-02,"10\nmore"junk,12,9,11,100\n', 3, "row"),
        (HEADER + VALID_ROW + '2026-01-05,"10', 3, "row"),
    ],
)
def test_malformed_quoting_is_a_chained_validation_error(
    tmp_path: Path, content: str, row: int, field: str
) -> None:
    error = assert_invalid(write_csv(tmp_path, content), row=row, field=field)
    assert isinstance(error.__cause__, csv.Error)


@pytest.mark.parametrize("symbol", ["", " ", " SYNTH", "SYNTH "])
def test_invalid_symbol_is_a_validation_error(tmp_path: Path, symbol: str) -> None:
    path = write_csv(tmp_path, HEADER + VALID_ROW)
    with pytest.raises(DataValidationError, match="symbol"):
        load_csv(path, symbol=symbol, price_basis="raw")


@pytest.mark.parametrize("price_basis", ["adjusted", "RAW", "", "raw "])
def test_unsupported_price_basis_is_a_validation_error(
    tmp_path: Path, price_basis: str
) -> None:
    path = write_csv(tmp_path, HEADER + VALID_ROW)
    with pytest.raises(DataValidationError, match="price_basis"):
        load_csv(path, symbol="SYNTH", price_basis=cast(Literal["raw"], price_basis))


@pytest.mark.parametrize("currency", ["EUR", "USD"])
def test_currency_is_not_an_overridable_argument(tmp_path: Path, currency: str) -> None:
    path = write_csv(tmp_path, HEADER + VALID_ROW)
    with pytest.raises(TypeError, match="currency"):
        load_csv(path, symbol="SYNTH", price_basis="raw", currency=currency)  # type: ignore[call-arg]


def test_price_basis_declaration_is_required(tmp_path: Path) -> None:
    path = write_csv(tmp_path, HEADER + VALID_ROW)
    with pytest.raises(TypeError, match="price_basis"):
        load_csv(path, symbol="SYNTH")  # type: ignore[call-arg]


@pytest.mark.parametrize("source", ["missing", "directory"])
def test_io_failures_are_descriptive_and_chained(tmp_path: Path, source: str) -> None:
    path = tmp_path / "missing.csv" if source == "missing" else tmp_path
    with pytest.raises(DataValidationError) as caught:
        load_csv(path, symbol="SYNTH", price_basis="raw")
    assert str(path) in str(caught.value)
    assert "read" in str(caught.value).lower()
    assert isinstance(caught.value.__cause__, OSError)


@pytest.mark.parametrize("bad_bytes", [b"\xff", b"\xef\xbb", b"\xff\xfe"])
def test_invalid_utf8_is_descriptive_and_chained(
    tmp_path: Path, bad_bytes: bytes
) -> None:
    path = write_csv(tmp_path, (HEADER + VALID_ROW).encode() + bad_bytes)
    with pytest.raises(DataValidationError) as caught:
        load_csv(path, symbol="SYNTH", price_basis="raw")
    assert str(path) in str(caught.value)
    assert "UTF-8" in str(caught.value)
    assert isinstance(caught.value.__cause__, UnicodeDecodeError)


def test_validation_error_is_a_value_error() -> None:
    assert issubclass(DataValidationError, ValueError)
