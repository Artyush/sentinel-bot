from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from sentinel_bot.metric_models import (
    MetricData,
    calculate_metrics,
    classify_metric,
)
from sentinel_bot.metrics import InvalidMetrics, parse_metrics

NOW = datetime(2026, 10, 21, tzinfo=UTC)
BOOT = NOW.timestamp() - 1000
MODES = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")


def exposition(*, cpu_offset: float = 0, idle_offset: float = 0) -> str:
    counters = "\n".join(
        f'node_cpu_seconds_total{{cpu="{core}",mode="{mode}"}} '
        f"{100 + (idle_offset if mode == 'idle' else cpu_offset)}"
        for core in ("0", "1")
        for mode in MODES
    )
    return f"""# HELP node_cpu_seconds_total Seconds spent in each CPU mode.
# TYPE node_cpu_seconds_total counter
{counters}
# TYPE node_memory_MemTotal_bytes gauge
node_memory_MemTotal_bytes 1000
# TYPE node_memory_MemAvailable_bytes gauge
node_memory_MemAvailable_bytes 250
# TYPE node_boot_time_seconds gauge
node_boot_time_seconds {BOOT}
# TYPE node_scrape_collector_success gauge
node_scrape_collector_success{{collector="cpu"}} 1
node_scrape_collector_success{{collector="meminfo"}} 1
node_scrape_collector_success{{collector="filesystem"}} 1
node_scrape_collector_success{{collector="stat"}} 1
node_filesystem_size_bytes{{device="/dev/sda1",fstype="ext4",mountpoint="/",device_error=""}} 1000
node_filesystem_avail_bytes{{device="/dev/sda1",fstype="ext4",mountpoint="/",device_error=""}} 100
node_filesystem_readonly{{device="/dev/sda1",fstype="ext4",mountpoint="/",device_error=""}} 0
node_filesystem_device_error{{device="/dev/sda1",fstype="ext4",mountpoint="/",device_error=""}} 0
"""


def compute(data: MetricData | None, previous=None, *, monotonic=100.0, checked_at=NOW):
    return calculate_metrics(data, previous, monotonic, 70, checked_at=checked_at)


def test_realistic_exposition_normalizes_independent_readings_and_cpu_delta():
    first = parse_metrics(exposition(), (("root", "/"),))
    readings, baseline = compute(first)
    assert readings.cpu_percent is None
    assert readings.memory_percent == 75
    assert readings.uptime_seconds == 1000
    assert readings.filesystems[0].value == 90
    current = parse_metrics(exposition(cpu_offset=1, idle_offset=3), (("root", "/"),))
    readings, _ = compute(current, baseline, monotonic=110)
    assert readings.cpu_percent == pytest.approx(70)


def test_cpu_guest_modes_are_excluded_and_iowait_is_busy():
    first = parse_metrics(exposition())
    _, baseline = compute(first)
    assert first.cpu_counters is not None
    current = replace(
        first,
        cpu_counters=tuple(
            replace(counter, value=counter.value + (10 if counter.mode == "iowait" else 0))
            for counter in first.cpu_counters
        ),
    )
    readings, _ = compute(current, baseline, monotonic=110)
    assert readings.cpu_percent == 100
    guest = '\nnode_cpu_seconds_total{cpu="0",mode="guest"} 999999\n'
    guest += 'node_cpu_seconds_total{cpu="0",mode="guest_nice"} NaN\n'
    assert parse_metrics(exposition() + guest).cpu_counters == first.cpu_counters


@pytest.mark.parametrize("change", ["reset", "set", "mode", "boot", "gap", "backward", "zero"])
def test_cpu_invalid_intervals_warm_up_instead_of_inventing_healthy(change):
    first = parse_metrics(exposition())
    _, baseline = compute(first)
    current = parse_metrics(exposition(cpu_offset=1, idle_offset=3))
    clock = 110
    assert current.cpu_counters is not None
    if change == "reset":
        current = replace(
            current,
            cpu_counters=(replace(current.cpu_counters[0], value=0), *current.cpu_counters[1:]),
        )
    elif change == "set":
        current = replace(current, cpu_counters=current.cpu_counters[:8])
    elif change == "mode":
        current = replace(current, cpu_counters=current.cpu_counters[1:])
    elif change == "boot":
        current = replace(current, boot_time=BOOT + 1)
    elif change == "gap":
        clock = 170.001
    elif change == "backward":
        clock = 99
    elif change == "zero":
        current = first
    readings, next_baseline = compute(current, baseline, monotonic=clock)
    assert readings.cpu_percent is None
    assert readings.memory_percent == 75
    assert next_baseline is not None


def test_cpu_freshness_boundary_and_wall_clock_jumps_use_monotonic_interval():
    _, baseline = compute(parse_metrics(exposition()))
    current = parse_metrics(exposition(cpu_offset=1, idle_offset=3))
    for wall in (NOW - timedelta(days=1), NOW + timedelta(days=1)):
        readings, _ = compute(current, baseline, monotonic=170, checked_at=wall)
        assert readings.cpu_percent == pytest.approx(70)


def test_failed_scrape_invalidates_cpu_baseline_and_retains_no_old_values():
    _, baseline = compute(parse_metrics(exposition()))
    empty, cleared = compute(None, baseline, monotonic=110)
    assert cleared is None
    assert empty.cpu_percent is empty.memory_percent is empty.uptime_seconds is None
    readings, _ = compute(parse_metrics(exposition(cpu_offset=1)), cleared, monotonic=120)
    assert readings.cpu_percent is None


@pytest.mark.parametrize(
    ("total", "available"),
    [
        (0, 0),
        (-1, 0),
        (100, -1),
        (100, 101),
        (float("nan"), 2),
        (100, float("inf")),
        (None, 0),
        (100, None),
    ],
)
def test_invalid_memory_is_unknown_without_hiding_filesystem(total, available):
    data = parse_metrics(exposition(), (("root", "/"),))
    data = replace(data, memory_total=total, memory_available=available)
    values, _ = compute(data)
    assert values.memory_percent is None
    assert values.filesystems[0].value == 90


@pytest.mark.parametrize("boot", [None, 0, -1, float("nan"), float("inf")])
def test_invalid_boot_time_makes_uptime_and_cpu_unknown(boot):
    data = replace(parse_metrics(exposition()), boot_time=boot)
    values, baseline = compute(data)
    assert values.uptime_seconds is values.cpu_percent is None
    assert baseline is None
    assert values.memory_percent == 75


@pytest.mark.parametrize("collector", ["cpu", "stat", "meminfo", "filesystem"])
@pytest.mark.parametrize("state", ["0", "NaN", "2", "missing", "duplicate"])
def test_collector_failure_or_ambiguous_state_affects_only_required_readings(collector, state):
    text = exposition()
    line = f'node_scrape_collector_success{{collector="{collector}"}} 1'
    if state == "missing":
        text = text.replace(line, "")
    elif state == "duplicate":
        text += line + "\n"
    else:
        text = text.replace(line, line[:-1] + state)
    data = parse_metrics(text, (("root", "/"),))
    values, _ = compute(data)
    assert (data.cpu_counters is None) == (collector == "cpu")
    assert (values.memory_percent is None) == (collector == "meminfo")
    assert (values.uptime_seconds is None) == (collector == "stat")
    assert (values.filesystems[0].value is None) == (collector == "filesystem")


@pytest.mark.parametrize(
    "mutation", ["missing", "duplicate", "wrong_label", "nan", "negative", "mode"]
)
def test_partial_or_invalid_cpu_never_uses_a_partial_denominator(mutation):
    text = exposition()
    line = 'node_cpu_seconds_total{cpu="0",mode="idle"} 100'
    changes = {
        "missing": "",
        "duplicate": line + "\n" + line,
        "wrong_label": line.replace('cpu="0"', 'cpu="0",host="unexpected"'),
        "nan": line[:-3] + "NaN",
        "negative": line[:-3] + "-1",
        "mode": line.replace('mode="idle"', 'mode="unknown"'),
    }
    assert parse_metrics(text.replace(line, changes[mutation])).cpu_counters is None


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate",
        "different_device",
        "different_labels",
        "readonly",
        "device",
        "zero",
        "oversize",
        "nan",
    ],
)
def test_selected_filesystem_requires_one_consistent_writable_device(mutation):
    text = exposition()
    line = next(
        line for line in text.splitlines() if line.startswith("node_filesystem_avail_bytes")
    )
    if mutation == "missing":
        text = text.replace(line, "")
    elif mutation == "duplicate":
        text += line + "\n"
    elif mutation == "different_device":
        text = text.replace(line, line.replace("sda1", "sdb1"))
    elif mutation == "different_labels":
        text = text.replace(line, line.replace(',device_error=""', ""))
    elif mutation == "readonly":
        text = text.replace(
            'node_filesystem_readonly{device="/dev/sda1",fstype="ext4",mountpoint="/",device_error=""} 0',
            'node_filesystem_readonly{device="/dev/sda1",fstype="ext4",mountpoint="/",device_error=""} 1',
        )
    elif mutation == "device":
        text = text.replace(
            'node_filesystem_device_error{device="/dev/sda1",fstype="ext4",mountpoint="/",device_error=""} 0',
            'node_filesystem_device_error{device="/dev/sda1",fstype="ext4",mountpoint="/",device_error=""} 1',
        )
    elif mutation == "zero":
        text = text.replace('device_error=""} 1000', 'device_error=""} 0')
    elif mutation == "oversize":
        text = text.replace(line, line[:-3] + "1001")
    else:
        text = text.replace(line, line[:-3] + "NaN")
    values, _ = compute(parse_metrics(text, (("root", "/"), ("absent", "/missing"))))
    assert values.filesystems[0].value is None
    assert values.filesystems[0].error is not None
    assert values.filesystems[1].error == "missing"
    assert values.memory_percent == 75


def test_legacy_three_filesystem_labels_and_reserved_space_are_supported():
    text = exposition().replace(',device_error=""', "")
    data = parse_metrics(text, (("root", "/"),))
    assert data.filesystems[0].value == 90
    assert "/dev/" not in repr(data)


@pytest.mark.parametrize(
    "family",
    ["node_memory_MemTotal_bytes", "node_memory_MemAvailable_bytes", "node_boot_time_seconds"],
)
def test_unexpected_labeled_scalar_variant_is_ambiguous_not_a_second_host(family):
    data = parse_metrics(exposition() + f'{family}{{unexpected="second-host"}} 50\n')
    values, _ = compute(data)
    if family == "node_boot_time_seconds":
        assert values.uptime_seconds is None
        assert values.memory_percent == 75
    else:
        assert values.memory_percent is None
        assert values.uptime_seconds == 1000


def test_unexpected_collector_labels_are_ambiguous_for_that_collector_only():
    text = exposition()
    text += 'node_scrape_collector_success{collector="meminfo",unexpected="second-host"} 1\n'
    data = parse_metrics(text, (("root", "/"),))
    values, _ = compute(data)
    assert values.memory_percent is None
    assert values.uptime_seconds == 1000
    assert values.filesystems[0].value == 90
    assert data.cpu_counters is not None


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, True),
        (75, True),
        (75.1, None),
        (84.9, None),
        (85, False),
        (100, False),
        (None, None),
        (-1, None),
        (101, None),
        (float("nan"), None),
        (float("inf"), None),
    ],
)
def test_resource_hysteresis_boundaries(value, expected):
    assert classify_metric(value, 85, 75) is expected


@pytest.mark.parametrize(
    "trigger,recover", [(85, 85), (75, 85), (101, 75), (85, -1), (float("inf"), 75)]
)
def test_hysteresis_rejects_invalid_rule_contract(trigger, recover):
    with pytest.raises(ValueError):
        classify_metric(80, trigger, recover)


@pytest.mark.parametrize(
    "text",
    [
        "broken{",
        "node_memory_MemTotal_bytes banana\n",
        'node_memory_MemTotal_bytes{a="x",a="y"} 1\n',
    ],
)
def test_malformed_exposition_is_normalized(text):
    with pytest.raises(InvalidMetrics):
        parse_metrics(text)


@pytest.mark.parametrize("monotonic,freshness", [(float("nan"), 70), (1, 0), (1, float("inf"))])
def test_invalid_interval_clocks_are_programming_errors(monotonic, freshness):
    with pytest.raises(ValueError):
        calculate_metrics(None, None, monotonic, freshness, checked_at=NOW)
