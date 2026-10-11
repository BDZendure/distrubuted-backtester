"""One coordinator's durable SQLite journal and validated read-only snapshots.

The coordinator supplies UTC anchored to elapsed monotonic time during a run.
Resume caps pending waits at one retry delay after a wall-clock change, and gives
interrupted attempts a full new delay. Only derived publication sets the complete
flag; resume clears it before export repair. Process recovery requires POSIX flock.
"""

import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import cast

try:
    import fcntl
except ImportError:  # Windows can still import classic execution and read status.
    fcntl = None  # type: ignore[assignment]

from distributed_backtester.experiment_artifacts import (
    outcome_artifact,
    validate_manifest,
)
from distributed_backtester.jobs import JobSpec, canonical_json, metadata_record
from distributed_backtester.models import DatasetMetadata
from distributed_backtester.money import format_money
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.scheduler.models import (
    AttemptSpec,
    RetryPolicy,
    finite_number,
)
from distributed_backtester.simulation import SMA_ID
from distributed_backtester.worker import JobFailure, JobOutcome

_APPLICATION_ID = 0x4442544A
_SCHEMA_VERSION = 1
_BUSY_TIMEOUT_MS = 5000
_STATUSES = ("pending", "running", "succeeded", "failed")
_REASONS = {"failure", "timeout", "interrupted", "worker_failure"}
_SCHEMA = {
    "metadata": """CREATE TABLE metadata (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        manifest TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
        policy TEXT NOT NULL, policy_sha256 TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK (generation >= 1),
        publication_complete INTEGER NOT NULL CHECK (publication_complete IN (0, 1)),
        created_utc REAL NOT NULL, updated_utc REAL NOT NULL
    )""",
    "jobs": """CREATE TABLE jobs (
        job_id TEXT PRIMARY KEY, spec TEXT NOT NULL, spec_sha256 TEXT NOT NULL,
        status TEXT NOT NULL CHECK (
            status IN ('pending','running','succeeded','failed')),
        attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
        next_eligible REAL
    )""",
    "attempts": """CREATE TABLE attempts (
        attempt_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(job_id),
        number INTEGER NOT NULL CHECK (number >= 1),
        generation INTEGER NOT NULL CHECK (generation >= 1),
        deadline_utc REAL NOT NULL, started_utc REAL NOT NULL, finished_utc REAL,
        status TEXT NOT NULL CHECK (
            status IN ('running','succeeded','failed','interrupted')),
        reason TEXT, failure TEXT, diagnostic TEXT,
        UNIQUE (job_id, number)
    )""",
    "commits": """CREATE TABLE commits (
        job_id TEXT PRIMARY KEY REFERENCES jobs(job_id),
        attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(attempt_id),
        content TEXT NOT NULL, content_sha256 TEXT NOT NULL,
        record TEXT NOT NULL, record_sha256 TEXT NOT NULL, committed_utc REAL NOT NULL
    )""",
    "events": """CREATE TABLE events (
        event_id INTEGER PRIMARY KEY,
        payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
    )""",
}


def _digest(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def _decode(text: object, digest: object | None = None) -> dict[str, object]:
    if not isinstance(text, str) or (digest is not None and _digest(text) != digest):
        raise RuntimeError("journal content hash mismatch")
    try:
        value = json.loads(text)
        if not isinstance(value, dict) or canonical_json(value) != text:
            raise ValueError("noncanonical JSON")
    except (TypeError, ValueError) as error:
        raise RuntimeError("invalid canonical journal JSON") from error
    return cast(dict[str, object], value)


def _failure(value: object) -> JobFailure:
    if not isinstance(value, dict) or set(value) != {"error_type", "message"}:
        raise RuntimeError("invalid journal failure")
    if not isinstance(value["error_type"], str) or not isinstance(
        value["message"], str
    ):
        raise RuntimeError("invalid journal failure fields")
    return JobFailure(value["error_type"], value["message"])


def _failure_record(failure: JobFailure) -> dict[str, object]:
    return {"error_type": failure.error_type, "message": failure.message}


@dataclass(frozen=True, slots=True)
class _Stored:
    manifest: dict[str, object]
    jobs: tuple[JobSpec, ...]
    policy: RetryPolicy
    generation: int
    publication_complete: bool


def _metadata(db: sqlite3.Connection) -> _Stored:
    rows = db.execute("SELECT * FROM metadata").fetchall()
    if len(rows) != 1 or rows[0]["singleton"] != 1:
        raise RuntimeError("journal metadata is uninitialized or corrupt")
    row = rows[0]
    manifest, jobs = validate_manifest(_decode(row["manifest"], row["manifest_sha256"]))
    policy = RetryPolicy.from_dict(_decode(row["policy"], row["policy_sha256"]))
    if type(row["generation"]) is not int or row["generation"] < 1:
        raise RuntimeError("invalid journal generation")
    if type(row["publication_complete"]) is not int or row[
        "publication_complete"
    ] not in (0, 1):
        raise RuntimeError("invalid publication flag")
    finite_number(row["created_utc"], "created_utc")
    finite_number(row["updated_utc"], "updated_utc")
    return _Stored(
        manifest, jobs, policy, row["generation"], bool(row["publication_complete"])
    )


def _verify_commit(
    row: sqlite3.Row,
    job: JobSpec,
    status: str,
    attempt: sqlite3.Row,
) -> tuple[str, dict[str, object]]:
    content = row["content"]
    if not isinstance(content, str) or not content.endswith("\n"):
        raise RuntimeError("invalid committed artifact text")
    if _digest(content) != row["content_sha256"]:
        raise RuntimeError("committed artifact hash mismatch")
    artifact = _decode(content[:-1])
    record = _decode(row["record"], row["record_sha256"])
    if (
        set(artifact) != {"schema_version", "job", "status", "result", "failure"}
        or type(artifact["schema_version"]) is not int
        or artifact["schema_version"] != 1
        or JobSpec.from_dict(artifact["job"]) != job
        or artifact["status"] != status
        or row["job_id"] != job.job_id
        or row["attempt_id"] != attempt["attempt_id"]
        or attempt["job_id"] != job.job_id
        or (
            attempt["status"] != status
            and not (status == "failed" and attempt["status"] == "interrupted")
        )
    ):
        raise RuntimeError("committed artifact does not match terminal job/attempt")
    finite_number(row["committed_utc"], "committed_utc")
    if row["committed_utc"] != attempt["finished_utc"]:
        raise RuntimeError("commit timestamp does not match attempt")
    metrics: object = None
    if status == "failed":
        failure = _failure(artifact["failure"])
        expected_content, expected_record = outcome_artifact(
            job, JobOutcome(job.job_id, None, failure)
        )
        # Interrupted terminal attempts keep their distinct history state.
        if attempt["failure"] is None or _decode(attempt["failure"]) != _failure_record(
            failure
        ):
            raise RuntimeError("terminal failure does not match attempt")
        if content != expected_content or record != expected_record:
            raise RuntimeError("terminal failure artifact/record mismatch")
        return content, record
    result = artifact["result"]
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
        status != "succeeded"
        or not isinstance(result, dict)
        or set(result)
        != {
            "schema_version",
            "engine_id",
            "config",
            "dataset",
            "fills",
            "order_outcomes",
            "equity",
            "final_portfolio",
            "metrics",
        }
        or type(result["schema_version"]) is not int
        or result["schema_version"] != 1
        or result["engine_id"] != job.implementation.engine_id
        or result["dataset"] != metadata_record(job.dataset)
        or result["config"] != expected_config
        or artifact["failure"] is not None
    ):
        raise RuntimeError("committed result does not match job inputs")
    metrics = result["metrics"]
    expected = {
        "job_id": job.job_id,
        "status": status,
        "artifact": f"results/{job.job_id}.json",
        "content_sha256": _digest(content),
        "metrics": metrics,
        "failure": None,
    }
    if record != expected:
        raise RuntimeError("committed compact record/path does not match artifact")
    return content, record


def _load(db: sqlite3.Connection) -> _Stored:
    if (
        db.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID
        or db.execute("PRAGMA user_version").fetchone()[0] != _SCHEMA_VERSION
    ):
        raise RuntimeError("unsupported or uninitialized journal schema")
    actual = {
        row["name"]: " ".join(row["sql"].split())
        for row in db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
        )
    }
    if actual != {name: " ".join(sql.split()) for name, sql in _SCHEMA.items()}:
        raise RuntimeError("journal schema is corrupt or unknown")
    if db.execute("PRAGMA quick_check").fetchall()[0][0] != "ok":
        raise RuntimeError("journal database integrity check failed")
    if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
        raise RuntimeError("journal foreign key integrity check failed")
    stored = _metadata(db)
    expected = {job.job_id: job for job in stored.jobs}
    rows = {row["job_id"]: row for row in db.execute("SELECT * FROM jobs")}
    if rows.keys() != expected.keys():
        raise RuntimeError("journal jobs do not match manifest")
    histories: dict[str, list[sqlite3.Row]] = {key: [] for key in expected}
    for attempt in db.execute("SELECT * FROM attempts ORDER BY job_id, number"):
        if attempt["job_id"] not in histories:
            raise RuntimeError("attempt references an unknown job")
        history = histories[attempt["job_id"]]
        job = expected[attempt["job_id"]]
        spec = AttemptSpec(
            job,
            attempt["number"],
            attempt["generation"],
            attempt["attempt_id"],
            attempt["deadline_utc"],
        )
        started = finite_number(attempt["started_utc"], "started_utc")
        if (
            spec.number != len(history) + 1
            or spec.generation > stored.generation
            or (history and spec.generation < history[-1]["generation"])
            or spec.deadline_utc != started + stored.policy.job_timeout_seconds
            or attempt["status"]
            not in {"running", "succeeded", "failed", "interrupted"}
        ):
            raise RuntimeError("invalid attempt history")
        if attempt["diagnostic"] is not None and not isinstance(
            attempt["diagnostic"], str
        ):
            raise RuntimeError("invalid attempt diagnostic")
        if attempt["status"] == "running":
            if any(
                attempt[key] is not None
                for key in ("finished_utc", "reason", "failure", "diagnostic")
            ):
                raise RuntimeError("running attempt has terminal data")
            if spec.generation != stored.generation:
                raise RuntimeError("running attempt has a stale generation")
        else:
            finite_number(attempt["finished_utc"], "finished_utc")
            if attempt["status"] == "succeeded":
                if any(
                    attempt[key] is not None
                    for key in ("reason", "failure", "diagnostic")
                ):
                    raise RuntimeError("successful attempt has failure data")
            elif attempt["reason"] not in _REASONS or attempt["failure"] is None:
                raise RuntimeError("failed attempt has no valid failure reason")
            else:
                _failure(_decode(attempt["failure"]))
                if (attempt["status"] == "interrupted") != (
                    attempt["reason"] == "interrupted"
                ):
                    raise RuntimeError("interruption state does not match reason")
        if history and history[-1]["status"] in {"running", "succeeded"}:
            raise RuntimeError("attempt after active/successful attempt")
        history.append(attempt)
    for key, row in rows.items():
        job = expected[key]
        if JobSpec.from_dict(_decode(row["spec"], row["spec_sha256"])) != job:
            raise RuntimeError("stored job does not match manifest")
        history = histories[key]
        count = row["attempt_count"]
        if (
            type(count) is not int
            or count != len(history)
            or not 0 <= count <= stored.policy.max_attempts
        ):
            raise RuntimeError("invalid attempt budget/count")
        if row["status"] not in _STATUSES:
            raise RuntimeError("invalid job status")
        if row["status"] == "pending":
            finite_number(row["next_eligible"], "next_eligible")
            if count >= stored.policy.max_attempts or (
                history and history[-1]["status"] not in {"failed", "interrupted"}
            ):
                raise RuntimeError("pending job has no remaining retry budget")
        elif row["next_eligible"] is not None or not history:
            raise RuntimeError("active/terminal job has inconsistent eligibility")
        elif row["status"] == "failed":
            if count != stored.policy.max_attempts or history[-1]["status"] not in {
                "failed",
                "interrupted",
            }:
                raise RuntimeError("terminal failure has inconsistent budget/state")
        elif history[-1]["status"] != row["status"]:
            raise RuntimeError("job status does not match latest attempt")
    committed: set[str] = set()
    for row in db.execute("SELECT * FROM commits ORDER BY job_id"):
        key = row["job_id"]
        if key not in expected or rows[key]["status"] not in {"succeeded", "failed"}:
            raise RuntimeError("commit without a terminal job")
        attempt = histories[key][-1]
        _verify_commit(row, expected[key], rows[key]["status"], attempt)
        committed.add(key)
    terminal = {
        key for key, row in rows.items() if row["status"] in {"succeeded", "failed"}
    }
    if terminal != committed or (
        stored.publication_complete and len(terminal) != len(expected)
    ):
        raise RuntimeError(
            "terminal state does not match committed artifacts/publication"
        )
    for row in db.execute("SELECT * FROM events ORDER BY event_id"):
        payload = _decode(row["payload"], row["payload_sha256"])
        if set(payload) != {
            "type",
            "now",
            "generation",
            "job_id",
            "attempt_id",
            "reason",
            "diagnostic",
        }:
            raise RuntimeError("invalid journal event")
        finite_number(payload["now"], "event now")
        generation = payload["generation"]
        if type(generation) is not int or not 1 <= generation <= stored.generation:
            raise RuntimeError("invalid event generation")
        if not isinstance(payload["type"], str) or not payload["type"]:
            raise RuntimeError("invalid event type")
        if payload["job_id"] is not None and payload["job_id"] not in expected:
            raise RuntimeError("event references unknown job")
    return stored


def _database_path(output: Path) -> Path:
    if not isinstance(output, Path):
        raise ValueError("output must be a Path")
    path = output / "journal.sqlite3"
    if output.is_symlink() or path.is_symlink():
        raise ValueError("journal paths must not be symlinks")
    if not path.is_file():
        raise FileNotFoundError(f"journal database does not exist: {path}")
    return path


def _connect(path: Path, *, readonly: bool, create: bool = False) -> sqlite3.Connection:
    mode = "ro" if readonly else ("rwc" if create else "rw")
    db = sqlite3.connect(
        path.resolve().as_uri() + f"?mode={mode}",
        uri=True,
        isolation_level=None,
        timeout=_BUSY_TIMEOUT_MS / 1000,
    )
    db.row_factory = sqlite3.Row
    try:
        db.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        db.execute("PRAGMA foreign_keys = ON")
        if readonly:
            db.execute("PRAGMA query_only = ON")
        else:
            if db.execute("PRAGMA journal_mode = WAL").fetchone()[0] != "wal":
                raise RuntimeError("journal requires SQLite WAL support")
            db.execute("PRAGMA synchronous = FULL")
        return db
    except BaseException:
        db.close()
        raise


@contextmanager
def _read_transaction(db: sqlite3.Connection) -> Iterator[None]:
    db.execute("BEGIN")
    try:
        yield
    finally:
        db.rollback()


@contextmanager
def _reader(output: Path) -> Iterator[sqlite3.Connection]:
    db = _connect(_database_path(output), readonly=True)
    try:
        with _read_transaction(db):
            yield db
    finally:
        db.close()


def _snapshot(db: sqlite3.Connection, stored: _Stored) -> dict[str, object]:
    jobs = [
        dict(row)
        for row in db.execute(
            "SELECT job_id, status, attempt_count, next_eligible "
            "FROM jobs ORDER BY job_id"
        )
    ]
    attempts: list[dict[str, object]] = []
    for row in db.execute("SELECT * FROM attempts ORDER BY job_id, number"):
        record = dict(row)
        record["failure"] = None if row["failure"] is None else _decode(row["failure"])
        attempts.append(record)
    events = [
        {"event_id": row["event_id"], **_decode(row["payload"], row["payload_sha256"])}
        for row in db.execute("SELECT * FROM events ORDER BY event_id")
    ]
    counts = {
        status: sum(job["status"] == status for job in jobs) for status in _STATUSES
    }
    return {
        "schema_version": _SCHEMA_VERSION,
        "experiment_id": stored.manifest["experiment_id"],
        "generation": stored.generation,
        "policy": stored.policy.to_dict(),
        "publication_complete": stored.publication_complete,
        "counts": counts,
        "jobs": jobs,
        "attempts": attempts,
        "events": events,
    }


def read_status(output: Path) -> dict[str, object]:
    """Read one validated snapshot without a coordinator lock or DB creation."""
    with _reader(output) as db:
        return _snapshot(db, _load(db))


def read_manifest(output: Path) -> dict[str, object]:
    """Return authoritative canonical inputs even when derived files are absent."""
    with _reader(output) as db:
        return _load(db).manifest


@dataclass(slots=True)
class CommittedSnapshot:
    """Headers and streamed artifacts from one read-only SQLite transaction.

    The context owns the connection. Full artifact text is never accumulated;
    use iter_artifacts only while read_committed_snapshot's context is open.
    """

    manifest: dict[str, object]
    jobs: tuple[JobSpec, ...]
    status: dict[str, object]
    records: tuple[dict[str, object], ...]
    _db: sqlite3.Connection
    _closed: bool = False

    def iter_artifacts(self) -> Iterator[tuple[str, bytes, dict[str, object]]]:
        if self._closed:
            raise RuntimeError("committed snapshot is closed")
        jobs = {job.job_id: job for job in self.jobs}
        cursor = self._db.execute("SELECT * FROM commits ORDER BY job_id")
        try:
            while True:
                if self._closed:
                    raise RuntimeError("committed snapshot is closed")
                row = cursor.fetchone()
                if row is None:
                    break
                attempt = self._db.execute(
                    "SELECT * FROM attempts WHERE attempt_id = ?", (row["attempt_id"],)
                ).fetchone()
                status = self._db.execute(
                    "SELECT status FROM jobs WHERE job_id = ?", (row["job_id"],)
                ).fetchone()[0]
                if attempt is None:
                    raise RuntimeError("committed attempt is missing")
                content, record = _verify_commit(
                    row, jobs[row["job_id"]], status, attempt
                )
                artifact = content.encode("utf-8")
                key = row["job_id"]
                del row, content
                yield key, artifact, record
                del artifact, record
        finally:
            if not self._closed:
                cursor.close()


@contextmanager
def read_committed_snapshot(output: Path) -> Iterator[CommittedSnapshot]:
    """Validate and stream committed outcomes without locking or resuming.

    Schema, hashes and scheduler invariants use the same validation as status
    and resume. Manifest, headers, history and artifacts share a single SQLite
    read transaction even if a coordinator commits concurrently. Pending jobs
    are represented in status; consumers decide whether completion is required.
    """
    if not isinstance(output, Path) or ".." in output.parts:
        raise ValueError("journal output must be a Path without traversal")
    absolute = output.absolute()
    if any(path.is_symlink() for path in (absolute, *absolute.parents)):
        raise ValueError("journal paths must not be symlinks")
    with _reader(output) as db:
        stored = _load(db)
        records = tuple(
            _decode(row["record"], row["record_sha256"])
            for row in db.execute(
                "SELECT record, record_sha256 FROM commits ORDER BY job_id"
            )
        )
        snapshot = CommittedSnapshot(
            stored.manifest, stored.jobs, _snapshot(db, stored), records, db
        )
        try:
            yield snapshot
        finally:
            snapshot._closed = True


def _lock(output: Path, *, create: bool) -> int:
    if fcntl is None:
        raise RuntimeError("durable scheduling requires POSIX advisory file locks")
    path = output / "coordinator.lock"
    if path.is_symlink():
        raise ValueError("coordinator lock must not be a symlink")
    descriptor = os.open(path, os.O_RDWR | (os.O_CREAT if create else 0), 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        os.close(descriptor)
        raise RuntimeError("another coordinator holds the journal lock") from error
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Journal:
    """A locked, single-writer session; SQLite owns all terminal result bytes."""

    def __init__(
        self, output: Path, db: sqlite3.Connection, lock: int, stored: _Stored
    ) -> None:
        self.output = output
        self.manifest = stored.manifest
        self._job_specs = stored.jobs
        self._policy = stored.policy
        self._generation = stored.generation
        self._connection = db
        self._lock_descriptor = lock
        self._closed = False
        self._jobs = {job.job_id: job for job in self.jobs}

    @property
    def jobs(self) -> tuple[JobSpec, ...]:
        return self._job_specs

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    @property
    def generation(self) -> int:
        return self._generation

    @classmethod
    def create(
        cls,
        output: Path,
        manifest: dict[str, object],
        policy: RetryPolicy,
        *,
        now: float,
    ) -> "Journal":
        now = finite_number(now, "now")
        validated, jobs = validate_manifest(manifest)
        if not isinstance(policy, RetryPolicy):
            raise ValueError("policy must be a RetryPolicy")
        if not isinstance(output, Path):
            raise ValueError("output must be a Path")
        if fcntl is None:
            raise RuntimeError("durable scheduling requires POSIX advisory file locks")
        output.mkdir(parents=True, exist_ok=False)
        descriptor = _lock(output, create=True)
        db: sqlite3.Connection | None = None
        journal: Journal | None = None
        try:
            db = _connect(output / "journal.sqlite3", readonly=False, create=True)
            stored = _Stored(validated, jobs, policy, 1, False)
            journal = cls(output, db, descriptor, stored)
            with journal._transaction():
                for sql in _SCHEMA.values():
                    db.execute(sql)
                db.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                db.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                manifest_text, policy_text = (
                    canonical_json(validated),
                    canonical_json(policy.to_dict()),
                )
                db.execute(
                    "INSERT INTO metadata VALUES (1, ?, ?, ?, ?, 1, 0, ?, ?)",
                    (
                        manifest_text,
                        _digest(manifest_text),
                        policy_text,
                        _digest(policy_text),
                        now,
                        now,
                    ),
                )
                for job in jobs:
                    text = canonical_json(job.to_dict())
                    db.execute(
                        "INSERT INTO jobs VALUES (?, ?, ?, 'pending', 0, ?)",
                        (job.job_id, text, _digest(text), now),
                    )
                journal._event("created", now)
            _sync_directory(output)
            _sync_directory(output.parent)
            return journal
        except BaseException:
            if journal is not None:
                journal.close()
            else:
                if db is not None:
                    db.close()
                os.close(descriptor)
            raise

    @classmethod
    def resume(
        cls,
        output: Path,
        *,
        dataset_metadata: DatasetMetadata,
        implementation: ImplementationIdentity,
        now: float,
    ) -> "Journal":
        now = finite_number(now, "now")
        path = _database_path(output)
        descriptor = _lock(output, create=False)
        db: sqlite3.Connection | None = None
        journal: Journal | None = None
        try:
            # Identity and all invariants precede even writer connection pragmas.
            with _reader(output) as reader:
                stored = _load(reader)
                if any(job.dataset != dataset_metadata for job in stored.jobs):
                    raise ValueError("resume dataset identity does not match journal")
                if any(job.implementation != implementation for job in stored.jobs):
                    raise ValueError(
                        "resume implementation identity does not match journal"
                    )
            db = _connect(path, readonly=False)
            journal = cls(output, db, descriptor, stored)
            with journal._transaction():
                _load(db)
                journal._generation += 1
                db.execute(
                    "UPDATE metadata SET generation = ?, publication_complete = 0, "
                    "updated_utc = ?",
                    (journal.generation, now),
                )
                # Persisted UTC may be far in the future after a clock change.
                eligible = finite_number(
                    now + stored.policy.retry_delay_seconds, "next_eligible"
                )
                db.execute(
                    "UPDATE jobs SET next_eligible = MIN(next_eligible, ?) "
                    "WHERE status = 'pending'",
                    (eligible,),
                )
                abandoned = db.execute(
                    "SELECT * FROM attempts WHERE status = 'running' ORDER BY job_id"
                ).fetchall()
                for row in abandoned:
                    attempt = AttemptSpec(
                        journal._jobs[row["job_id"]],
                        row["number"],
                        row["generation"],
                        row["attempt_id"],
                        row["deadline_utc"],
                    )
                    journal._finish_failure(
                        attempt,
                        JobFailure(
                            "Interrupted",
                            "coordinator stopped before completing the attempt",
                        ),
                        now,
                        "interrupted",
                        None,
                    )
                journal._event("resumed", now)
            return journal
        except BaseException:
            if journal is not None:
                journal.close()
            else:
                if db is not None:
                    db.close()
                os.close(descriptor)
            raise

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("journal session is closed")

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._ensure_open()
        db = self._connection
        db.execute("BEGIN IMMEDIATE")
        try:
            yield
            db.commit()
        except BaseException:
            db.rollback()
            raise

    def _event(
        self,
        kind: str,
        now: float,
        attempt: AttemptSpec | None = None,
        reason: str | None = None,
        diagnostic: str | None = None,
    ) -> None:
        # The recording coordinator's generation is used even for a stale reply.
        payload = canonical_json(
            {
                "type": kind,
                "now": now,
                "generation": self.generation,
                "job_id": None
                if attempt is None or attempt.job.job_id not in self._jobs
                else attempt.job.job_id,
                "attempt_id": None if attempt is None else attempt.attempt_id,
                "reason": reason,
                "diagnostic": diagnostic,
            }
        )
        self._connection.execute(
            "INSERT INTO events (payload, payload_sha256) VALUES (?, ?)",
            (payload, _digest(payload)),
        )

    def _lease(
        self, attempt: AttemptSpec, *, completed: bool = False
    ) -> sqlite3.Row | None:
        if not isinstance(attempt, AttemptSpec):
            raise ValueError("attempt must be an AttemptSpec")
        if self._jobs.get(attempt.job.job_id) != attempt.job:
            return None
        row = cast(
            sqlite3.Row | None,
            self._connection.execute(
                "SELECT * FROM attempts WHERE attempt_id = ?", (attempt.attempt_id,)
            ).fetchone(),
        )
        if row is None or any(
            (
                row["job_id"] != attempt.job.job_id,
                row["number"] != attempt.number,
                row["generation"] != attempt.generation,
                row["deadline_utc"] != attempt.deadline_utc,
            )
        ):
            return None
        job = self._connection.execute(
            "SELECT * FROM jobs WHERE job_id = ?", (attempt.job.job_id,)
        ).fetchone()
        if job is None or job["attempt_count"] != attempt.number:
            return None
        if completed:
            return row if row["status"] == job["status"] == "succeeded" else None
        if (
            row["status"] != "running"
            or job["status"] != "running"
            or attempt.generation != self.generation
        ):
            return None
        return row

    def find_attempt(self, attempt_id: str) -> AttemptSpec | None:
        """Read an original lease, including completed or interrupted history."""
        self._ensure_open()
        with _read_transaction(self._connection):
            row = self._connection.execute(
                "SELECT job_id, number, generation, deadline_utc FROM attempts "
                "WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if row is None:
                return None
            return AttemptSpec(
                self._jobs[row["job_id"]],
                row["number"],
                row["generation"],
                attempt_id,
                row["deadline_utc"],
            )

    def claim(self, *, now: float) -> AttemptSpec | None:
        now = finite_number(now, "now")
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM jobs WHERE status = 'pending' AND next_eligible <= ? "
                "ORDER BY job_id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            number = row["attempt_count"] + 1
            if number > self.policy.max_attempts:
                raise RuntimeError("pending job has exhausted its attempt budget")
            job = self._jobs[row["job_id"]]
            attempt = AttemptSpec(
                job,
                number,
                self.generation,
                f"{job.job_id}:{number}",
                finite_number(now + self.policy.job_timeout_seconds, "deadline_utc"),
            )
            self._connection.execute(
                "INSERT INTO attempts VALUES "
                "(?, ?, ?, ?, ?, ?, NULL, 'running', NULL, NULL, NULL)",
                (
                    attempt.attempt_id,
                    job.job_id,
                    number,
                    self.generation,
                    attempt.deadline_utc,
                    now,
                ),
            )
            self._connection.execute(
                "UPDATE jobs SET status = 'running', attempt_count = ?, "
                "next_eligible = NULL WHERE job_id = ?",
                (number, job.job_id),
            )
            self._connection.execute("UPDATE metadata SET updated_utc = ?", (now,))
            self._event("claimed", now, attempt)
            return attempt

    def _store_commit(
        self, attempt: AttemptSpec, content: str, record: dict[str, object], now: float
    ) -> None:
        text = canonical_json(record)
        self._connection.execute(
            "INSERT INTO commits VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                attempt.job.job_id,
                attempt.attempt_id,
                content,
                _digest(content),
                text,
                _digest(text),
                now,
            ),
        )

    def _finish_failure(
        self,
        attempt: AttemptSpec,
        failure: JobFailure,
        now: float,
        reason: str,
        diagnostic: str | None,
    ) -> str:
        terminal = attempt.number >= self.policy.max_attempts
        content_record = (
            outcome_artifact(attempt.job, JobOutcome(attempt.job.job_id, None, failure))
            if terminal
            else None
        )
        eligible = (
            None
            if terminal
            else finite_number(now + self.policy.retry_delay_seconds, "next_eligible")
        )
        status = "interrupted" if reason == "interrupted" else "failed"
        # Terminal interruption is a failed result, with interrupted attempt history.
        self._connection.execute(
            "UPDATE attempts SET status = ?, finished_utc = ?, reason = ?, "
            "failure = ?, diagnostic = ? WHERE attempt_id = ?",
            (
                status,
                now,
                reason,
                canonical_json(_failure_record(failure)),
                diagnostic,
                attempt.attempt_id,
            ),
        )
        if content_record is not None:
            self._store_commit(attempt, *content_record, now)
        self._connection.execute(
            "UPDATE jobs SET status = ?, next_eligible = ? WHERE job_id = ?",
            ("failed" if terminal else "pending", eligible, attempt.job.job_id),
        )
        self._connection.execute("UPDATE metadata SET updated_utc = ?", (now,))
        self._event(
            "terminal_failure" if terminal else "retry",
            now,
            attempt,
            reason,
            diagnostic,
        )
        return "terminal" if terminal else "retry"

    def fail(
        self,
        attempt: AttemptSpec,
        failure: JobFailure,
        *,
        now: float,
        reason: str = "failure",
        diagnostic: str | None = None,
    ) -> str:
        now = finite_number(now, "now")
        if not isinstance(failure, JobFailure) or reason not in _REASONS:
            raise ValueError("failure must be a JobFailure with a supported reason")
        if diagnostic is not None and not isinstance(diagnostic, str):
            raise ValueError("diagnostic must be text or None")
        with self._transaction():
            if self._lease(attempt) is None:
                self._event("stale_failure", now, attempt, reason, diagnostic)
                return "stale"
            return self._finish_failure(attempt, failure, now, reason, diagnostic)

    def commit(self, attempt: AttemptSpec, outcome: JobOutcome, *, now: float) -> str:
        now = finite_number(now, "now")
        if (
            not isinstance(outcome, JobOutcome)
            or outcome.result is None
            or outcome.failure is not None
        ):
            raise ValueError("commit requires a successful JobOutcome")
        with self._transaction():
            # Do not trust a mutable in-memory/derived manifest during commit.
            stored = _metadata(self._connection)
            if (
                stored.manifest != self.manifest
                or stored.jobs != self.jobs
                or stored.policy != self.policy
                or stored.generation != self.generation
            ):
                raise RuntimeError(
                    "journal inputs or generation changed during session"
                )
            completed = self._lease(attempt, completed=True)
            if completed is not None:
                row = self._connection.execute(
                    "SELECT * FROM commits WHERE job_id = ?", (attempt.job.job_id,)
                ).fetchone()
                if row is None:
                    raise RuntimeError("successful job has no committed artifact")
                existing, _ = _verify_commit(row, attempt.job, "succeeded", completed)
                content, record = outcome_artifact(attempt.job, outcome)
                if existing != content or row["record"] != canonical_json(record):
                    raise RuntimeError("conflicting result for completed attempt")
                self._event("duplicate", now, attempt)
                return "duplicate"
            if self._lease(attempt) is None or now >= attempt.deadline_utc:
                self._event("stale_commit", now, attempt)
                return "stale"
            content, record = outcome_artifact(attempt.job, outcome)
            self._store_commit(attempt, content, record, now)
            self._connection.execute(
                "UPDATE attempts SET status = 'succeeded', finished_utc = ? "
                "WHERE attempt_id = ?",
                (now, attempt.attempt_id),
            )
            self._connection.execute(
                "UPDATE jobs SET status = 'succeeded', next_eligible = NULL "
                "WHERE job_id = ?",
                (attempt.job.job_id,),
            )
            self._connection.execute("UPDATE metadata SET updated_utc = ?", (now,))
            self._event("committed", now, attempt)
            return "committed"

    def all_terminal(self) -> bool:
        self._ensure_open()
        return (
            self._connection.execute(
                "SELECT 1 FROM jobs WHERE status IN ('pending','running') LIMIT 1"
            ).fetchone()
            is None
        )

    def next_eligible(self) -> float | None:
        self._ensure_open()
        value = self._connection.execute(
            "SELECT MIN(next_eligible) FROM jobs WHERE status = 'pending'"
        ).fetchone()[0]
        return None if value is None else finite_number(value, "next_eligible")

    def committed(self) -> Iterator[tuple[str, str, dict[str, object]]]:
        self._ensure_open()
        with _read_transaction(self._connection):
            _load(self._connection)
            for row in self._connection.execute(
                "SELECT * FROM commits ORDER BY job_id"
            ):
                attempt = self._connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id = ?", (row["attempt_id"],)
                ).fetchone()
                if attempt is None:
                    raise RuntimeError("committed attempt is missing")
                status = self._connection.execute(
                    "SELECT status FROM jobs WHERE job_id = ?", (row["job_id"],)
                ).fetchone()[0]
                content, record = _verify_commit(
                    row, self._jobs[row["job_id"]], status, attempt
                )
                yield row["job_id"], content, record

    def snapshot(self) -> dict[str, object]:
        self._ensure_open()
        with _read_transaction(self._connection):
            return _snapshot(self._connection, _load(self._connection))

    def mark_complete(self) -> None:
        with self._transaction():
            stored = _load(self._connection)
            if not self.all_terminal():
                raise RuntimeError("publication requires all jobs to be terminal")
            now = self._connection.execute(
                "SELECT updated_utc FROM metadata"
            ).fetchone()[0]
            if not stored.publication_complete:
                self._connection.execute("UPDATE metadata SET publication_complete = 1")
                self._event("publication_complete", now)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._connection.close()
        finally:
            os.close(self._lock_descriptor)

    def __enter__(self) -> "Journal":
        self._ensure_open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
