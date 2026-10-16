"""Verified analytical snapshots with dependency-free, streamed source reads."""

import json
import re
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from hashlib import sha256
from pathlib import Path
from typing import cast

from distributed_backtester.execution import execute_intent
from distributed_backtester.experiment_artifacts import index_record, validate_manifest
from distributed_backtester.jobs import JobSpec, canonical_json, metadata_record
from distributed_backtester.models import Portfolio, TargetIntent
from distributed_backtester.money import format_money, format_ratio, parse_money
from distributed_backtester.scheduler.journal import read_committed_snapshot
from distributed_backtester.simulation import SMA_ID
from distributed_backtester.worker import JobFailure

Artifact = tuple[str, bytes, dict[str, object]]
_RECORD_KEYS = {"job_id", "status", "artifact", "content_sha256", "metrics", "failure"}


def _record(value: object, keys: set[str], name: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"invalid {name} fields")
    return cast(dict[str, object], value)


def _decode(content: bytes, *, canonical: bool = True) -> dict[str, object]:
    try:
        value = json.loads(content)
        if not isinstance(value, dict):
            raise ValueError("JSON must be an object")
        if canonical and (canonical_json(value) + "\n").encode() != content:
            raise ValueError("noncanonical artifact JSON")
        # Even observational JSON must contain only finite values.
        canonical_json(value)
        return cast(dict[str, object], value)
    except (UnicodeError, TypeError, ValueError) as error:
        raise ValueError("invalid or noncanonical snapshot JSON") from error


def _safe_path(path: Path) -> Path:
    if not isinstance(path, Path) or ".." in path.parts:
        raise ValueError("snapshot path must be a Path without traversal")
    absolute = path.absolute()
    if any(item.is_symlink() for item in (absolute, *absolute.parents)):
        raise ValueError("snapshot paths must not be symlinks")
    return absolute


def _read(path: Path) -> bytes:
    _safe_path(path)
    if not path.is_file():
        raise FileNotFoundError(f"snapshot file does not exist: {path}")
    return path.read_bytes()


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _money(value: object) -> int:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"(?:0|[1-9][0-9]*)\.[0-9]{6}", value) is None
    ):
        raise ValueError("money must be nonnegative canonical six-place text")
    return parse_money(value)


def _session(value: object) -> date:
    if not isinstance(value, str):
        raise ValueError("session must be date text")
    try:
        session = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("session must be valid date text") from error
    if session.isoformat() != value:
        raise ValueError("session must be canonical date text")
    return session


def _validate_ledger(
    equity: list[object], fills: list[object], job: JobSpec
) -> tuple[dict[int, dict[str, object]], tuple[date, ...]]:
    sessions = tuple(
        _session(cast(dict[str, object], point)["session"]) for point in equity
    )
    positions = {session: index for index, session in enumerate(sessions)}
    by_session: dict[date, dict[str, object]] = {}
    by_sequence: dict[int, dict[str, object]] = {}
    previous_session: date | None = None
    previous_sequence = 0
    for value in fills:
        fill = cast(dict[str, object], value)
        sequence = _integer(fill["sequence"], "fill sequence", minimum=1)
        decision = _session(fill["decision_session"])
        execution = _session(fill["execution_session"])
        index = positions.get(execution)
        if index is None or index == 0 or sessions[index - 1] != decision:
            raise ValueError("fill execution must be the next supplied session")
        if sequence <= previous_sequence or (
            previous_session is not None and execution <= previous_session
        ):
            raise ValueError("fill sequence and execution sessions must increase")
        previous_sequence, previous_session = sequence, execution
        by_session[execution] = fill
        by_sequence[sequence] = fill

    portfolio = Portfolio(job.config.initial_cash, 0)
    for point_value, session in zip(equity, sessions, strict=True):
        point = cast(dict[str, object], point_value)
        execution_fill = by_session.get(session)
        if execution_fill is not None:
            updated, outcome = execute_intent(
                TargetIntent(
                    "invested" if execution_fill["side"] == "buy" else "flat",
                    _session(execution_fill["decision_session"]),
                    _integer(execution_fill["sequence"], "fill sequence", minimum=1),
                ),
                session,
                _money(execution_fill["price"]),
                portfolio,
                job.config.fee_bps,
            )
            expected = outcome.fill
            if expected is None or (
                execution_fill["quantity"] != expected.quantity
                or _money(execution_fill["fee"]) != expected.fee
                or _money(execution_fill["cash_after"]) != updated.cash
                or execution_fill["holdings_after"] != updated.holdings
            ):
                raise ValueError(
                    "fill quantity, fee, or portfolio transition is inconsistent"
                )
            portfolio = updated
        if (
            _money(point["cash"]) != portfolio.cash
            or point["holdings"] != portfolio.holdings
        ):
            raise ValueError("equity portfolio changes do not match declared fills")
    return by_sequence, sessions


def _validate_diagnostic_timeline(
    orders: list[object], sessions: tuple[date, ...], decisions: set[date]
) -> None:
    """Prove an emitting-close assignment exists without trusting bad intents.

    Reserve all known decisions first. Greedily placing each unknown diagnostic
    in the earliest allowed unused close leaves every later option available.
    Open=2*i and close=2*i+1 distinguish ordering within a supplied session.
    This proves feasibility, not which signal the strategy actually generated.
    """
    positions = {session: index for index, session in enumerate(sessions)}
    previous_event, next_close = -1, 0
    for value in orders:
        order = cast(dict[str, object], value)
        sequence = order["sequence"]
        immediate = order["status"] == "rejected" and order["execution_session"] is None
        known_duplicate = (
            immediate
            and order["reason"] == "invalid_sequence"
            and type(sequence) is int
            and sequence > 0
        )
        if immediate and not known_duplicate:
            declared = (
                None
                if order["decision_session"] is None
                else _session(order["decision_session"])
            )
            candidate = max(next_close, (previous_event + 1) // 2)
            while candidate < len(sessions) and (
                sessions[candidate] in decisions
                or (
                    order["reason"] == "invalid_timing"
                    and sessions[candidate] == declared
                )
            ):
                candidate += 1
            if candidate == len(sessions):
                raise ValueError(
                    "rejected order has no possible emitting close session"
                )
            event, next_close = 2 * candidate + 1, candidate + 1
        elif order["status"] == "canceled":
            event = 2 * len(sessions)  # Cancellation is after the final close call.
        elif known_duplicate:
            event = 2 * positions[_session(order["decision_session"])] + 1
        else:
            event = 2 * positions[_session(order["execution_session"])]
        if event <= previous_event:
            raise ValueError(
                "order outcomes have no feasible chronological close timeline"
            )
        previous_event = event


def _validate_orders(
    orders: list[object],
    fills: dict[int, dict[str, object]],
    sessions: tuple[date, ...],
    equity: list[object],
) -> None:
    if len(orders) > len(sessions):
        raise ValueError("order outcomes exceed the supplied close calls")
    positions = {session: index for index, session in enumerate(sessions)}
    linked: set[int] = set()
    decisions: set[date] = set()
    previous_execution: date | None = None
    previous_event: date | None = None
    last_sequence = 0
    for order_index, value in enumerate(orders):
        order = _record(
            value,
            {
                "sequence",
                "target",
                "decision_session",
                "execution_session",
                "status",
                "reason",
                "fill_sequence",
            },
            "order outcome",
        )
        status, reason, target = order["status"], order["reason"], order["target"]
        sequence = order["sequence"]
        if status not in ("filled", "no_change", "rejected", "canceled"):
            raise ValueError("unsupported order status")
        if reason is not None and not isinstance(reason, str):
            raise ValueError("order reason must be text or null")
        if target is not None and not isinstance(target, str):
            raise ValueError("order target must be text or null")
        if sequence is not None and type(sequence) is not int:
            raise ValueError("order sequence must be an integer or null")
        decision = (
            None
            if order["decision_session"] is None
            else _session(order["decision_session"])
        )
        execution = (
            None
            if order["execution_session"] is None
            else _session(order["execution_session"])
        )
        if status == "rejected" and reason in (
            "invalid_target",
            "invalid_sequence",
            "invalid_timing",
        ):
            if order["fill_sequence"] is not None or execution is not None:
                raise ValueError(
                    "invalid rejected order cannot execute or reference a fill"
                )
            if reason == "invalid_target" and target in ("flat", "invested"):
                raise ValueError("invalid-target order declares a valid target")
            if reason != "invalid_target" and target not in ("flat", "invested"):
                raise ValueError("rejected order reason does not match its target")
            if (
                reason == "invalid_sequence"
                and type(sequence) is int
                and sequence > last_sequence
            ):
                raise ValueError("invalid-sequence order declares a new valid sequence")
            if reason == "invalid_sequence" and type(sequence) is int and sequence > 0:
                if decision not in positions or (
                    previous_event is not None
                    and (decision is None or decision < previous_event)
                ):
                    raise ValueError(
                        "duplicate order sequence requires a valid "
                        "current decision session"
                    )
                if decision in decisions:
                    raise ValueError(
                        "multiple order intents claim the same close session"
                    )
                decisions.add(decision)
                previous_event = decision
            if reason == "invalid_timing" and (
                type(sequence) is not int or sequence <= 0
            ):
                raise ValueError("invalid-timing order needs a valid sequence")
            continue
        sequence = _integer(sequence, "order sequence", minimum=1)
        if (
            sequence <= last_sequence
            or target not in ("flat", "invested")
            or decision not in positions
        ):
            raise ValueError("order sequence, target, or decision session is invalid")
        if decision in decisions:
            raise ValueError("multiple order intents claim the same close session")
        decisions.add(decision)
        last_sequence = sequence
        if status == "canceled":
            if (
                reason != "end_of_data"
                or execution is not None
                or decision != sessions[-1]
                or order_index != len(orders) - 1
                or order["fill_sequence"] is not None
            ):
                raise ValueError(
                    "canceled order must end at the final session without a fill"
                )
            continue
        index = positions.get(execution) if execution is not None else None
        if (
            index is None
            or index == 0
            or sessions[index - 1] != decision
            or (
                previous_execution is not None
                and cast(date, execution) <= previous_execution
            )
            or (previous_event is not None and cast(date, execution) <= previous_event)
        ):
            raise ValueError(
                "order execution must occur once at the next supplied session"
            )
        previous_execution = previous_event = cast(date, execution)
        if status == "filled":
            fill_sequence = _integer(
                order["fill_sequence"], "order fill sequence", minimum=1
            )
            fill = fills.get(fill_sequence)
            if (
                reason is not None
                or fill is None
                or fill_sequence in linked
                or sequence != fill_sequence
                or decision != _session(fill["decision_session"])
                or execution != _session(fill["execution_session"])
                or target != ("invested" if fill["side"] == "buy" else "flat")
            ):
                raise ValueError(
                    "filled order does not match exactly one declared fill"
                )
            linked.add(fill_sequence)
        else:
            if order["fill_sequence"] is not None:
                raise ValueError("unfilled order cannot reference a fill")
            holdings = cast(dict[str, object], equity[index - 1])["holdings"]
            if status == "no_change":
                expected = (
                    "already_invested" if target == "invested" else "already_flat"
                )
                if reason != expected or (target == "invested") != bool(holdings):
                    raise ValueError(
                        "no-change order contradicts its target or portfolio"
                    )
            elif reason != "insufficient_cash" or target != "invested" or holdings != 0:
                raise ValueError("rejected execution must declare insufficient cash")
    if linked != set(fills):
        raise ValueError("every declared fill needs exactly one filled order")
    _validate_diagnostic_timeline(orders, sessions, decisions)


def _metrics(result: dict[str, object], job: JobSpec) -> dict[str, object]:
    equity, fills = result["equity"], result["fills"]
    if not isinstance(equity, list) or not equity or not isinstance(fills, list):
        raise ValueError("result needs equity and fills arrays")
    peak = job.config.initial_cash
    drawdown = Fraction(0)
    previous: date | None = None
    final = 0
    last: dict[str, object] = {}
    for value in equity:
        point = _record(
            value, {"session", "cash", "holdings", "close", "equity"}, "equity"
        )
        session = _session(point["session"])
        if previous is not None and session <= previous:
            raise ValueError("equity sessions must be strictly increasing")
        previous = session
        cash, close = _money(point["cash"]), _money(point["close"])
        holdings = _integer(point["holdings"], "holdings")
        final = _money(point["equity"])
        if close == 0 or final != cash + holdings * close:
            raise ValueError("equity does not match portfolio mark")
        peak = max(peak, final)
        drawdown = max(drawdown, Fraction(peak - final, peak))
        last = point
    portfolio = _record(
        result["final_portfolio"], {"cash", "holdings"}, "final portfolio"
    )
    if canonical_json(portfolio) != canonical_json(
        {"cash": last["cash"], "holdings": last["holdings"]}
    ):
        raise ValueError("final portfolio does not match equity")
    total_fees = 0
    sequences: set[int] = set()
    for value in fills:
        fill = _record(
            value,
            {
                "sequence",
                "decision_session",
                "execution_session",
                "side",
                "quantity",
                "price",
                "fee",
                "cash_after",
                "holdings_after",
            },
            "fill",
        )
        sequence = _integer(fill["sequence"], "fill sequence", minimum=1)
        if sequence in sequences or fill["side"] not in ("buy", "sell"):
            raise ValueError("invalid fill sequence or side")
        sequences.add(sequence)
        if _session(fill["decision_session"]) >= _session(fill["execution_session"]):
            raise ValueError("fill must execute after decision")
        _integer(fill["quantity"], "quantity", minimum=1)
        _integer(fill["holdings_after"], "holdings_after")
        if _money(fill["price"]) == 0:
            raise ValueError("fill price must be positive")
        _money(fill["cash_after"])
        total_fees += _money(fill["fee"])
    orders = result["order_outcomes"]
    if not isinstance(orders, list):
        raise ValueError("order outcomes must be an array")
    checked_fills, sessions = _validate_ledger(equity, fills, job)
    _validate_orders(orders, checked_fills, sessions, equity)
    total_return = Fraction(final - job.config.initial_cash, job.config.initial_cash)
    expected: dict[str, object] = {
        "total_return": format_ratio(total_return.numerator, total_return.denominator),
        "maximum_drawdown": format_ratio(drawdown.numerator, drawdown.denominator),
        "final_equity": format_money(final),
        "fill_count": len(fills),
        "total_fees": format_money(total_fees),
    }
    if canonical_json(result["metrics"]) != canonical_json(expected):
        raise ValueError("metrics do not match equity and fills")
    return expected


def _validate_artifact(content: bytes, record: dict[str, object], job: JobSpec) -> None:
    if sha256(content).hexdigest() != record["content_sha256"]:
        raise ValueError("artifact content hash mismatch")
    artifact = _record(
        _decode(content),
        {"schema_version", "job", "status", "result", "failure"},
        "outcome",
    )
    if (
        type(artifact["schema_version"]) is not int
        or artifact["schema_version"] != 1
        or JobSpec.from_dict(artifact["job"]) != job
        or artifact["status"] != record["status"]
    ):
        raise ValueError("outcome does not match job or status")
    metrics: object = None
    failure: object = artifact["failure"]
    if artifact["status"] == "failed":
        fields = _record(failure, {"error_type", "message"}, "failure")
        if not isinstance(fields["error_type"], str) or not isinstance(
            fields["message"], str
        ):
            raise ValueError("failure fields must be text")
        JobFailure(fields["error_type"], fields["message"])
        if artifact["result"] is not None:
            raise ValueError("failed outcome cannot contain a result")
    elif artifact["status"] == "succeeded":
        result = _record(
            artifact["result"],
            {
                "schema_version",
                "engine_id",
                "config",
                "dataset",
                "fills",
                "order_outcomes",
                "equity",
                "final_portfolio",
                "metrics",
            },
            "result",
        )
        expected_config = {
            "strategy": {
                "id": SMA_ID,
                "parameters": {
                    "fast_window": job.config.strategy.fast_window,
                    "slow_window": job.config.strategy.slow_window,
                },
            },
            "initial_cash": format_money(job.config.initial_cash),
            "fee_bps": job.config.fee_bps,
            "fee_rounding": "half-even-micro-USD",
            "sizing": "maximum-affordable-whole-shares-on-target-change",
            "timing": "close-decision-next-supplied-open-fill-close-mark",
        }
        if (
            type(result["schema_version"]) is not int
            or result["schema_version"] != 1
            or result["engine_id"] != job.implementation.engine_id
            or canonical_json(result["config"]) != canonical_json(expected_config)
            or canonical_json(result["dataset"])
            != canonical_json(metadata_record(job.dataset))
            or failure is not None
        ):
            raise ValueError("result does not match job config/dataset/implementation")
        metrics = _metrics(result, job)
    else:
        raise ValueError("outcome must be terminal")
    expected = {
        "job_id": job.job_id,
        "status": artifact["status"],
        "artifact": f"results/{job.job_id}.json",
        "content_sha256": sha256(content).hexdigest(),
        "metrics": metrics,
        "failure": failure,
    }
    if canonical_json(record) != canonical_json(expected):
        raise ValueError("compact record does not match full artifact")


def _headers(
    records: object, jobs: tuple[JobSpec, ...]
) -> tuple[dict[str, object], ...]:
    if not isinstance(records, (list, tuple)) or len(records) != len(jobs):
        raise ValueError("snapshot requires all expected jobs terminal")
    checked = []
    for value, job in zip(records, jobs, strict=True):
        record = _record(value, _RECORD_KEYS, "compact outcome")
        digest = record["content_sha256"]
        if (
            record["job_id"] != job.job_id
            or record["status"] not in ("succeeded", "failed")
            or record["artifact"] != f"results/{job.job_id}.json"
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            raise ValueError("invalid terminal outcome membership/path/hash")
        checked.append(record)
    return tuple(checked)


@dataclass(slots=True)
class ExperimentSnapshot:
    manifest: dict[str, object]
    jobs: tuple[JobSpec, ...]
    records: tuple[dict[str, object], ...]
    observation: dict[str, object]
    _stream: Callable[[], Iterator[Artifact]]
    _closed: bool = False

    @property
    def experiment_id(self) -> str:
        return str(self.manifest["experiment_id"])

    @property
    def snapshot_id(self) -> str:
        return sha256(
            canonical_json(
                {
                    "experiment_id": self.experiment_id,
                    "outcomes": [
                        {
                            "job_id": record["job_id"],
                            "content_sha256": record["content_sha256"],
                        }
                        for record in self.records
                    ],
                }
            ).encode()
        ).hexdigest()

    @property
    def submitted(self) -> int:
        return len(self.records)

    @property
    def succeeded(self) -> int:
        return sum(record["status"] == "succeeded" for record in self.records)

    @property
    def failed(self) -> int:
        return self.submitted - self.succeeded

    def iter_artifacts(self) -> Generator[Artifact, None, None]:
        if self._closed:
            raise RuntimeError("experiment snapshot is closed")
        stream = self._stream()
        try:
            for job, record in zip(self.jobs, self.records, strict=True):
                if self._closed:
                    raise RuntimeError("experiment snapshot is closed")
                item = next(stream, None)
                if (
                    item is None
                    or item[0] != job.job_id
                    or canonical_json(item[2]) != canonical_json(record)
                ):
                    raise ValueError("streamed outcome differs from snapshot headers")
                _validate_artifact(item[1], record, job)
                yield item
                del item
            if next(stream, None) is not None:
                raise ValueError("unexpected extra snapshot outcome")
        finally:
            close = getattr(stream, "close", None)
            if callable(close):
                close()


@contextmanager
def read_experiment_snapshot(experiment: Path) -> Iterator[ExperimentSnapshot]:
    """Prefer authoritative journal commits, otherwise verify classic exports.

    Headers are O(jobs). Full artifact reads and validation occur as the consumer
    advances the iterator so an importing transaction can roll back on any error.
    """
    output = _safe_path(experiment)
    journal = output / "journal.sqlite3"
    if journal.exists() or journal.is_symlink():
        with read_committed_snapshot(output) as committed:
            records = _headers(committed.records, committed.jobs)
            snapshot = ExperimentSnapshot(
                committed.manifest,
                committed.jobs,
                records,
                {
                    "source_kind": "journal",
                    "source_path": str(output),
                    "journal_status": committed.status,
                },
                committed.iter_artifacts,
            )
            try:
                yield snapshot
            finally:
                snapshot._closed = True
        return
    manifest_content = _read(output / "manifest.json")
    manifest, jobs = validate_manifest(_decode(manifest_content))
    index_content = _read(output / "index.json")
    index = _decode(index_content)
    records = _headers(index.get("outcomes"), jobs)
    if canonical_json(index) != canonical_json(
        index_record(str(manifest["experiment_id"]), records)
    ):
        raise ValueError("invalid final index schema/counts/experiment")
    observation: dict[str, object] = {
        "source_kind": "classic",
        "source_path": str(output),
    }
    execution_path = output / "execution.json"
    execution_content: bytes | None = None
    if execution_path.exists() or execution_path.is_symlink():
        execution_content = _read(execution_path)
        execution = _decode(execution_content, canonical=False)
        if (
            type(execution.get("schema_version")) is not int
            or execution["schema_version"] != 1
            or execution.get("experiment_id") != manifest["experiment_id"]
        ):
            raise ValueError("execution metadata does not match experiment")
        observation["execution"] = execution

    def stream() -> Iterator[Artifact]:
        for job, record in zip(jobs, records, strict=True):
            content = _read(output / str(record["artifact"]))
            yield job.job_id, content, record
            del content
        # Recheck compact source metadata after streaming to detect changed runs.
        current_execution = (
            _read(execution_path)
            if execution_path.exists() or execution_path.is_symlink()
            else None
        )
        if (
            _read(output / "manifest.json") != manifest_content
            or _read(output / "index.json") != index_content
            or current_execution != execution_content
        ):
            raise ValueError("experiment snapshot changed during read")

    snapshot = ExperimentSnapshot(manifest, jobs, records, observation, stream)
    try:
        yield snapshot
    finally:
        snapshot._closed = True
