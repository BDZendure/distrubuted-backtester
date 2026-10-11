"""Strict file defaults without starting backends or applying CLI semantics."""

from pathlib import Path

import pytest

from distributed_backtester.configuration import load_configuration


def _write_config(tmp_path: Path, fields: str = "", command: str = "backtest") -> Path:
    path = tmp_path / "run.toml"
    path.write_text(
        f'schema_version = 1\ncommand = "{command}"\n{fields}', encoding="utf-8"
    )
    return path


def test_backtest_returns_supplied_cli_defaults_and_resolves_paths(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path,
        'data = "sma.csv"\n'
        'data_format = "csv"\n'
        'dataset_version = "expected-version"\n'
        'symbol = "SYNTH"\n'
        'price_basis = "raw"\n'
        "fast = 2\nslow = 3\n"
        'initial_cash = "1000.000001"\n'
        "fee_bps = 5\n"
        'output = "results/backtest.json"\n'
        "overwrite = true\n"
        'log_level = "INFO"\n',
    )
    assert load_configuration(path, "backtest") == {
        "data": tmp_path / "sma.csv",
        "data_format": "csv",
        "dataset_version": "expected-version",
        "symbol": "SYNTH",
        "price_basis": "raw",
        "fast": 2,
        "slow": 3,
        "initial_cash": "1000.000001",
        "fee_bps": 5,
        "output": tmp_path / "results/backtest.json",
        "overwrite": True,
        "log_level": "INFO",
    }
    assert not (tmp_path / "results").exists()


def test_sweep_accepts_the_complete_cli_field_contract(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        'data = "sma.csv"\ndata_format = "auto"\ndataset_version = "digest"\n'
        'symbol = "SYNTH"\nprice_basis = "raw"\n'
        'fast = [1, 2]\nslow = [3, 4]\ninitial_cash = "1000"\nfee_bps = 0\n'
        'backend = "ray"\nworkers = 2\nmax_in_flight = 4\n'
        'ray_address = "auto"\ncpus_per_worker = 0.5\n'
        'worker_resource = ["gpu=0.25", "custom=1"]\n'
        "object_store_memory = 100000000\ndurable = true\nmax_attempts = 3\n"
        'job_timeout = 60.0\nretry_delay = 0.1\noutput_dir = "results"\n'
        'log_level = "DEBUG"\n',
        "sweep",
    )
    assert load_configuration(path, "sweep") == {
        "data": tmp_path / "sma.csv",
        "data_format": "auto",
        "dataset_version": "digest",
        "symbol": "SYNTH",
        "price_basis": "raw",
        "fast": [1, 2],
        "slow": [3, 4],
        "initial_cash": "1000",
        "fee_bps": 0,
        "backend": "ray",
        "workers": 2,
        "max_in_flight": 4,
        "ray_address": "auto",
        "cpus_per_worker": 0.5,
        "worker_resource": ["gpu=0.25", "custom=1"],
        "object_store_memory": 100000000,
        "durable": True,
        "max_attempts": 3,
        "job_timeout": 60.0,
        "retry_delay": 0.1,
        "output_dir": tmp_path / "results",
        "log_level": "DEBUG",
    }
    assert not (tmp_path / "results").exists()


def test_resume_accepts_only_its_cli_fields(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        'data = "sma.csv"\ndata_format = "parquet"\ndataset_version = "digest"\n'
        'output_dir = "journal"\nworkers = 2\nmax_in_flight = 3\n'
        'ray_address = "host:6379"\ncpus_per_worker = 1.0\n'
        'worker_resource = ["custom=1"]\nobject_store_memory = 100000000\n'
        'log_level = "WARNING"\n',
        "resume",
    )
    assert load_configuration(path, "resume") == {
        "data": tmp_path / "sma.csv",
        "data_format": "parquet",
        "dataset_version": "digest",
        "output_dir": tmp_path / "journal",
        "workers": 2,
        "max_in_flight": 3,
        "ray_address": "host:6379",
        "cpus_per_worker": 1.0,
        "worker_resource": ["custom=1"],
        "object_store_memory": 100000000,
        "log_level": "WARNING",
    }


@pytest.mark.parametrize("command", ["backtest", "sweep", "resume"])
def test_header_only_is_a_valid_partial_configuration(
    tmp_path: Path, command: str
) -> None:
    assert load_configuration(_write_config(tmp_path, command=command), command) == {}


def test_partial_configuration_does_not_insert_cli_defaults(tmp_path: Path) -> None:
    path = _write_config(tmp_path, "fee_bps = 5\n")
    assert load_configuration(path, "backtest") == {"fee_bps": 5}


@pytest.mark.parametrize(
    ("contents", "field"),
    [
        ('command = "backtest"\n', "schema_version"),
        ("schema_version = 1\n", "command"),
        ('schema_version = 2\ncommand = "backtest"\n', "schema_version"),
        ('schema_version = true\ncommand = "backtest"\n', "schema_version"),
        ('schema_version = 1.0\ncommand = "backtest"\n', "schema_version"),
        ('schema_version = "1"\ncommand = "backtest"\n', "schema_version"),
        ("schema_version = 1\ncommand = 1\n", "command"),
        ('schema_version = 1\ncommand = "Backtest"\n', "command"),
        ('schema_version = 1\ncommand = " backtest"\n', "command"),
        ('schema_version = 1\ncommand = "status"\n', "command"),
    ],
)
def test_headers_are_required_and_exact(
    tmp_path: Path, contents: str, field: str
) -> None:
    path = tmp_path / "run.toml"
    path.write_text(contents, encoding="utf-8")
    with pytest.raises(ValueError, match=field):
        load_configuration(path, "backtest")


@pytest.mark.parametrize("command", ["sweep", "resume"])
def test_file_command_must_match_requested_command(
    tmp_path: Path, command: str
) -> None:
    with pytest.raises(ValueError, match="command"):
        load_configuration(_write_config(tmp_path), command)


@pytest.mark.parametrize("command", ["status", "convert-data", "Backtest", ""])
def test_only_run_commands_can_load_configuration(tmp_path: Path, command: str) -> None:
    with pytest.raises(ValueError, match="command"):
        load_configuration(_write_config(tmp_path, command=command), command)


@pytest.mark.parametrize(
    ("command", "field"),
    [
        ("backtest", "backend"),
        ("backtest", "workers"),
        ("backtest", "durable"),
        ("backtest", "output_dir"),
        ("sweep", "output"),
        ("sweep", "overwrite"),
        ("resume", "symbol"),
        ("resume", "price_basis"),
        ("resume", "fast"),
        ("resume", "slow"),
        ("resume", "initial_cash"),
        ("resume", "fee_bps"),
        ("resume", "backend"),
        ("resume", "durable"),
        ("resume", "max_attempts"),
        ("resume", "job_timeout"),
        ("resume", "retry_delay"),
        ("backtest", "config"),
        ("backtest", "initial-cash"),
        ("backtest", "schema-version"),
        ("backtest", "fast_window"),
        ("sweep", "worker_resources"),
        ("sweep", "row_group_size"),
        ("backtest", "typo"),
    ],
)
def test_unknown_or_other_command_keys_are_named(
    tmp_path: Path, command: str, field: str
) -> None:
    path = _write_config(tmp_path, f"{field} = 1\n", command)
    with pytest.raises(ValueError, match=f"unknown.*{field}"):
        load_configuration(path, command)


def test_nested_tables_are_not_an_alternate_schema(tmp_path: Path) -> None:
    path = _write_config(tmp_path, "[backtest]\nfast = 2\n")
    with pytest.raises(ValueError, match=r"unknown.*backtest"):
        load_configuration(path, "backtest")


@pytest.mark.parametrize(
    ("command", "field"),
    [
        ("backtest", "fast"),
        ("backtest", "slow"),
        ("backtest", "fee_bps"),
        ("sweep", "workers"),
        ("sweep", "max_in_flight"),
        ("sweep", "object_store_memory"),
        ("sweep", "max_attempts"),
        ("resume", "workers"),
        ("resume", "max_in_flight"),
        ("resume", "object_store_memory"),
    ],
)
@pytest.mark.parametrize("value", ["true", "2.0", '"2"', "[2]"])
def test_integer_fields_reject_coercion_including_booleans(
    tmp_path: Path, command: str, field: str, value: str
) -> None:
    with pytest.raises(ValueError, match=field):
        load_configuration(
            _write_config(tmp_path, f"{field} = {value}\n", command), command
        )


@pytest.mark.parametrize(
    ("command", "field"), [("backtest", "overwrite"), ("sweep", "durable")]
)
@pytest.mark.parametrize("value", ["1", '"true"', "[]"])
def test_boolean_switches_require_toml_booleans(
    tmp_path: Path, command: str, field: str, value: str
) -> None:
    with pytest.raises(ValueError, match=field):
        load_configuration(
            _write_config(tmp_path, f"{field} = {value}\n", command), command
        )


@pytest.mark.parametrize(
    ("command", "field"), [("backtest", "overwrite"), ("sweep", "durable")]
)
@pytest.mark.parametrize("value", [False, True])
def test_boolean_switches_preserve_false_and_true(
    tmp_path: Path, command: str, field: str, value: bool
) -> None:
    path = _write_config(tmp_path, f"{field} = {str(value).lower()}\n", command)
    assert load_configuration(path, command) == {field: value}


@pytest.mark.parametrize(
    ("command", "field"),
    [
        ("sweep", "cpus_per_worker"),
        ("sweep", "job_timeout"),
        ("sweep", "retry_delay"),
        ("resume", "cpus_per_worker"),
    ],
)
@pytest.mark.parametrize(
    "value", ["true", '"1.0"', "nan", "inf", "-inf", "1e999", "[]"]
)
def test_float_fields_require_finite_numbers(
    tmp_path: Path, command: str, field: str, value: str
) -> None:
    with pytest.raises(ValueError, match=field):
        load_configuration(
            _write_config(tmp_path, f"{field} = {value}\n", command), command
        )


@pytest.mark.parametrize("field", ["cpus_per_worker", "job_timeout", "retry_delay"])
@pytest.mark.parametrize("value", [1, 1.25])
def test_float_options_accept_finite_integer_and_float_literals(
    tmp_path: Path, field: str, value: int | float
) -> None:
    path = _write_config(tmp_path, f"{field} = {value}\n", "sweep")
    loaded = load_configuration(path, "sweep")
    assert loaded == {field: float(value)}
    assert type(loaded[field]) is float


@pytest.mark.parametrize(
    ("command", "field"),
    [
        ("backtest", "data"),
        ("backtest", "output"),
        ("backtest", "symbol"),
        ("backtest", "dataset_version"),
        ("backtest", "initial_cash"),
        ("backtest", "data_format"),
        ("backtest", "price_basis"),
        ("backtest", "log_level"),
        ("sweep", "output_dir"),
        ("sweep", "backend"),
        ("sweep", "ray_address"),
        ("resume", "data"),
        ("resume", "output_dir"),
        ("resume", "ray_address"),
    ],
)
@pytest.mark.parametrize("value", ["1", "true", "[]", "2026-01-02", '{value = "text"}'])
def test_string_fields_are_not_coerced(
    tmp_path: Path, command: str, field: str, value: str
) -> None:
    with pytest.raises(ValueError, match=field):
        load_configuration(
            _write_config(tmp_path, f"{field} = {value}\n", command), command
        )


@pytest.mark.parametrize(
    "value", ["", " 2.00 ", "12.0000000", "0.0000001", "NaN", "1e2", "-1"]
)
def test_money_text_is_preserved_for_main_to_parse(tmp_path: Path, value: str) -> None:
    path = _write_config(tmp_path, f'initial_cash = "{value}"\n')
    assert load_configuration(path, "backtest") == {"initial_cash": value}


@pytest.mark.parametrize("command", ["backtest", "sweep", "resume"])
@pytest.mark.parametrize("level", ["OFF", "ERROR", "WARNING", "INFO", "DEBUG"])
def test_all_commands_accept_exact_log_levels(
    tmp_path: Path, command: str, level: str
) -> None:
    path = _write_config(tmp_path, f'log_level = "{level}"\n', command)
    assert load_configuration(path, command) == {"log_level": level}


@pytest.mark.parametrize(
    ("command", "field", "value"),
    [
        (command, "data_format", value)
        for command in ("backtest", "sweep", "resume")
        for value in ("auto", "csv", "parquet")
    ]
    + [("sweep", "backend", value) for value in ("sequential", "process", "ray")]
    + [(command, "price_basis", "raw") for command in ("backtest", "sweep")],
)
def test_choices_match_cli_options(
    tmp_path: Path, command: str, field: str, value: str
) -> None:
    path = _write_config(tmp_path, f'{field} = "{value}"\n', command)
    assert load_configuration(path, command) == {field: value}


@pytest.mark.parametrize(
    ("command", "field", "value"),
    [
        ("backtest", "log_level", "info"),
        ("sweep", "log_level", "WARN"),
        ("resume", "log_level", " INFO"),
        ("backtest", "data_format", "CSV"),
        ("backtest", "data_format", "json"),
        ("sweep", "data_format", " csv "),
        ("backtest", "price_basis", "adjusted"),
        ("sweep", "price_basis", "RAW"),
        ("sweep", "backend", "Ray"),
        ("sweep", "backend", "thread"),
    ],
)
def test_choices_reject_case_aliases_and_whitespace(
    tmp_path: Path, command: str, field: str, value: str
) -> None:
    path = _write_config(tmp_path, f'{field} = "{value}"\n', command)
    with pytest.raises(ValueError, match=field):
        load_configuration(path, command)


@pytest.mark.parametrize("field", ["fast", "slow"])
@pytest.mark.parametrize(
    "value",
    ["[]", "2", '"2"', "[true]", "[2.0]", '["2"]', "[0]", "[-1]", "[2, true]", "[[2]]"],
)
def test_sweep_windows_require_nonempty_positive_exact_integer_arrays(
    tmp_path: Path, field: str, value: str
) -> None:
    path = _write_config(tmp_path, f"{field} = {value}\n", "sweep")
    with pytest.raises(ValueError, match=field):
        load_configuration(path, "sweep")


def test_windows_preserve_order_duplicates_and_partial_grids(tmp_path: Path) -> None:
    path = _write_config(tmp_path, "fast = [3, 1, 3]\n", "sweep")
    assert load_configuration(path, "sweep") == {"fast": [3, 1, 3]}


@pytest.mark.parametrize("command", ["sweep", "resume"])
@pytest.mark.parametrize(
    "value",
    [
        '"gpu=1"',
        "[]",
        "[1]",
        '[""]',
        '["gpu"]',
        '["=1"]',
        '["gpu="]',
        '["  =1"]',
        '["gpu=  "]',
    ],
)
def test_worker_resources_require_nonempty_name_quantity_string_arrays(
    tmp_path: Path, command: str, value: str
) -> None:
    path = _write_config(tmp_path, f"worker_resource = {value}\n", command)
    with pytest.raises(ValueError, match="worker_resource"):
        load_configuration(path, command)


def test_resource_quantity_semantics_and_duplicates_are_left_to_main(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path, 'worker_resource = ["gpu=0", "gpu=not-a-number"]\n', "sweep"
    )
    assert load_configuration(path, "sweep") == {
        "worker_resource": ["gpu=0", "gpu=not-a-number"]
    }


def test_cross_field_and_domain_semantics_are_left_to_main(tmp_path: Path) -> None:
    path = _write_config(tmp_path, "fast = 3\nslow = 1\nfee_bps = 10001\n")
    assert load_configuration(path, "backtest") == {
        "fast": 3,
        "slow": 1,
        "fee_bps": 10001,
    }
    path = _write_config(
        tmp_path,
        'backend = "sequential"\nworkers = 0\njob_timeout = -1.0\n',
        "sweep",
    )
    assert load_configuration(path, "sweep") == {
        "backend": "sequential",
        "workers": 0,
        "job_timeout": -1.0,
    }


@pytest.mark.parametrize(
    ("command", "field"),
    [("backtest", "data"), ("backtest", "output"), ("sweep", "output_dir")],
)
def test_relative_paths_use_config_directory_regardless_of_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str, field: str
) -> None:
    directory = tmp_path / "config"
    directory.mkdir()
    path = _write_config(directory, f'{field} = "../artifacts/./value"\n', command)
    monkeypatch.chdir(tmp_path)
    loaded = load_configuration(Path("config/run.toml"), command)
    assert loaded == {field: tmp_path / "artifacts/value"}
    assert path.exists()
    assert not (tmp_path / "artifacts").exists()


def test_absolute_paths_are_not_prefixed_with_config_directory(tmp_path: Path) -> None:
    destination = tmp_path / "absolute.json"
    path = _write_config(tmp_path, f'output = "{destination}"\n')
    assert load_configuration(path, "backtest") == {"output": destination}


@pytest.mark.parametrize(
    ("command", "field"),
    [("backtest", "output"), ("sweep", "output_dir"), ("resume", "output_dir")],
)
@pytest.mark.parametrize("alias_kind", ["same", "symlink", "hardlink"])
def test_configuration_cannot_be_its_own_output_through_aliases(
    tmp_path: Path, command: str, field: str, alias_kind: str
) -> None:
    alias_name = "run.toml" if alias_kind == "same" else "alias.toml"
    path = _write_config(tmp_path, f'{field} = "{alias_name}"\n', command)
    original = path.read_bytes()
    alias = tmp_path / alias_name
    if alias_kind == "symlink":
        alias.symlink_to(path)
    elif alias_kind == "hardlink":
        alias.hardlink_to(path)
    with pytest.raises(ValueError, match=r"output.*configuration"):
        load_configuration(path, command)
    assert path.read_bytes() == original


@pytest.mark.parametrize(
    "contents", ["[", 'schema_version = 1\ncommand = "backtest"\nfast = 1\nfast = 2\n']
)
def test_malformed_toml_reports_configuration_path(
    tmp_path: Path, contents: str
) -> None:
    path = tmp_path / "malformed.toml"
    path.write_text(contents, encoding="utf-8")
    with pytest.raises(ValueError, match=r"malformed\.toml"):
        load_configuration(path, "backtest")


def test_non_utf8_toml_reports_configuration_path(tmp_path: Path) -> None:
    path = tmp_path / "invalid.toml"
    path.write_bytes(b"\xff")
    with pytest.raises(ValueError, match=r"invalid\.toml"):
        load_configuration(path, "backtest")


def test_missing_configuration_is_an_io_error_without_creation(tmp_path: Path) -> None:
    path = tmp_path / "missing.toml"
    with pytest.raises(OSError, match=r"missing\.toml"):
        load_configuration(path, "backtest")
    assert not path.exists()


@pytest.mark.parametrize("command", ["backtest", "sweep"])
def test_bundled_examples_include_required_inputs_and_user_supplies_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command: str
) -> None:
    examples = Path(__file__).resolve().parents[1] / "examples"
    monkeypatch.chdir(tmp_path)
    loaded = load_configuration(examples / f"{command}.toml", command)
    assert loaded["data"] == examples / "sma.csv"
    assert (examples / "sma.csv").is_file()
    assert loaded["symbol"] == "SYNTH"
    assert loaded["price_basis"] == "raw"
    assert type(loaded["initial_cash"]) is str
    assert "output" not in loaded
    assert "output_dir" not in loaded
    if command == "sweep":
        assert loaded["fast"] == [1, 2]
        assert loaded["slow"] == [3, 4]
    else:
        assert loaded["fast"] == 2
        assert loaded["slow"] == 3
