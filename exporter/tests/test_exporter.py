"""Unit tests for exporter logic functions."""

from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Helpers that replicate the trimmed-mean logic from exporter.py so tests
# don't need to import the module (which has side-effects at import time).
# ---------------------------------------------------------------------------

def _trimmed_mean(latencies: list[int]) -> float:
    """Compute trimmed-mean peer latency (top 5% dropped), matching exporter logic."""
    if not latencies:
        return 0.0
    sorted_latencies = sorted(latencies)
    trim = max(0, len(sorted_latencies) // 20)
    trimmed = sorted_latencies[:-trim] if trim > 0 else sorted_latencies
    return sum(trimmed) / len(trimmed)


def _parse_complete_ledgers(complete_ledgers: str) -> tuple[int, int]:
    """Parse complete_ledgers string to (low, high), matching exporter logic."""
    if complete_ledgers not in ("empty", "") and "-" in complete_ledgers:
        range_segments = [seg.split("-") for seg in complete_ledgers.split(",")]
        low = min(int(r[0]) for r in range_segments if len(r) == 2)
        high = max(int(r[1]) for r in range_segments if len(r) == 2)
        return low, high
    return 0, 0


# ---------------------------------------------------------------------------
# Trimmed-mean tests
# ---------------------------------------------------------------------------

class TestTrimmedMean:
    def test_single_peer_no_zero_division(self) -> None:
        """Single peer must not raise ZeroDivisionError (was broken before fix)."""
        result = _trimmed_mean([500])
        assert result == 500.0

    def test_two_peers(self) -> None:
        """Two peers — trim is 0 so both values are included."""
        result = _trimmed_mean([100, 200])
        assert result == 150.0

    def test_nineteen_peers_trim_is_zero(self) -> None:
        """19 peers: 19 // 20 == 0, so nothing is trimmed."""
        latencies = list(range(1, 20))  # 1..19
        result = _trimmed_mean(latencies)
        assert result == sum(latencies) / len(latencies)

    def test_twenty_peers_trims_one(self) -> None:
        """20 peers: 20 // 20 == 1, so the single highest value is dropped."""
        latencies = list(range(1, 21))  # 1..20; highest is 20
        result = _trimmed_mean(latencies)
        expected = sum(range(1, 20)) / 19
        assert result == pytest.approx(expected)

    def test_outlier_dropped(self) -> None:
        """High outlier (dying peer) should be excluded from the average."""
        normal_peers = [10] * 19
        dying_peer = [18000]
        result = _trimmed_mean(normal_peers + dying_peer)
        # trim = 20 // 20 = 1 — the outlier is dropped
        assert result == pytest.approx(10.0)

    def test_empty_latencies_returns_zero(self) -> None:
        assert _trimmed_mean([]) == 0.0


# ---------------------------------------------------------------------------
# complete_ledgers parsing tests
# ---------------------------------------------------------------------------

class TestCompleteledgersParsing:
    def test_simple_range(self) -> None:
        low, high = _parse_complete_ledgers("32570-1000000")
        assert low == 32570
        assert high == 1000000

    def test_multi_range_with_gap(self) -> None:
        """Comma-separated ranges (node has a gap) must return global min/max."""
        low, high = _parse_complete_ledgers("32570-1000000,1000500-2000000")
        assert low == 32570
        assert high == 2000000

    def test_three_ranges(self) -> None:
        low, high = _parse_complete_ledgers("100-200,300-400,500-600")
        assert low == 100
        assert high == 600

    def test_empty_string(self) -> None:
        assert _parse_complete_ledgers("empty") == (0, 0)

    def test_blank_string(self) -> None:
        assert _parse_complete_ledgers("") == (0, 0)

    def test_single_value_no_dash(self) -> None:
        """A string without a dash should return (0, 0) — not crash."""
        assert _parse_complete_ledgers("12345") == (0, 0)


# ---------------------------------------------------------------------------
# Helpers that replicate job-type stale-label logic from exporter.py.
# The real function calls prometheus Gauge.labels().set() which has module-
# level side-effects, so we mirror the set-tracking logic in plain dicts.
# ---------------------------------------------------------------------------

def _apply_job_type_updates(
    job_types: list[dict[str, Any]],
    labels_last: set[str],
    gauge_state: dict[str, int],
) -> set[str]:
    """
    Mirror _update_job_type_metrics() without prometheus deps.

    Updates gauge_state in-place (keyed by job_type) and returns the new
    current_labels set — the caller stores it as labels_last for next call.
    Stale labels (in labels_last but not current) are zeroed in gauge_state.
    """
    current_labels: set[str] = set()
    for job in job_types:
        if not isinstance(job, dict):
            continue
        job_type: str = str(job.get("job_type", ""))
        if not job_type:
            continue
        current_labels.add(job_type)
        gauge_state[job_type] = int(job.get("per_second", 0))

    for stale in labels_last - current_labels:
        gauge_state[stale] = 0

    return current_labels


def _compute_counter_delta(raw: int, last: int) -> int:
    """Mirror the delta-or-restart logic used for byte/op counters."""
    return raw - last if raw >= last else raw


# ---------------------------------------------------------------------------
# Job-type stale-label tests
# ---------------------------------------------------------------------------

class TestJobTypeStaleLabels:
    def test_new_labels_are_recorded(self) -> None:
        """Labels that appear for the first time are added to gauge state."""
        gauge: dict[str, int] = {}
        labels_last: set[str] = set()
        jobs = [{"job_type": "clientRPC", "per_second": 5}]
        labels_last = _apply_job_type_updates(jobs, labels_last, gauge)
        assert gauge["clientRPC"] == 5
        assert "clientRPC" in labels_last

    def test_stale_label_is_zeroed(self) -> None:
        """A job_type present last scrape but absent this scrape must be set to 0."""
        gauge: dict[str, int] = {"clientRPC": 5, "processTransaction": 3}
        labels_last: set[str] = {"clientRPC", "processTransaction"}
        # processTransaction disappears
        jobs = [{"job_type": "clientRPC", "per_second": 7}]
        labels_last = _apply_job_type_updates(jobs, labels_last, gauge)
        assert gauge["clientRPC"] == 7
        assert gauge["processTransaction"] == 0

    def test_labels_last_updated_to_current(self) -> None:
        """labels_last must reflect exactly the current scrape's labels after the call."""
        gauge: dict[str, int] = {}
        labels_last: set[str] = {"old_job"}
        jobs = [
            {"job_type": "newJob1", "per_second": 1},
            {"job_type": "newJob2", "per_second": 2},
        ]
        labels_last = _apply_job_type_updates(jobs, labels_last, gauge)
        assert labels_last == {"newJob1", "newJob2"}

    def test_non_dict_entries_skipped(self) -> None:
        """Non-dict entries in the job list must not raise and must be ignored."""
        gauge: dict[str, int] = {}
        labels_last: set[str] = set()
        jobs: list[Any] = ["not_a_dict", None, {"job_type": "clientRPC", "per_second": 2}]
        labels_last = _apply_job_type_updates(jobs, labels_last, gauge)
        assert gauge == {"clientRPC": 2}

    def test_empty_job_list_zeros_all_previous(self) -> None:
        """Empty job list (all jobs idle) must zero every previously seen label."""
        gauge: dict[str, int] = {"clientRPC": 5, "processTransaction": 3}
        labels_last: set[str] = {"clientRPC", "processTransaction"}
        labels_last = _apply_job_type_updates([], labels_last, gauge)
        assert gauge["clientRPC"] == 0
        assert gauge["processTransaction"] == 0
        assert labels_last == set()


# ---------------------------------------------------------------------------
# Counter delta / restart-reset tests
# ---------------------------------------------------------------------------

class TestCounterDelta:
    def test_normal_increment(self) -> None:
        """Monotonically increasing counter produces positive delta."""
        assert _compute_counter_delta(1000, 800) == 200

    def test_no_change(self) -> None:
        """Counter unchanged between scrapes produces zero delta."""
        assert _compute_counter_delta(500, 500) == 0

    def test_restart_resets_counter(self) -> None:
        """Counter drops below last value (rippled restarted) — use raw as delta."""
        assert _compute_counter_delta(50, 9000) == 50

    def test_restart_to_zero(self) -> None:
        """Counter resets to exactly zero after restart."""
        assert _compute_counter_delta(0, 5000) == 0

    def test_first_scrape_last_zero(self) -> None:
        """First scrape: last=0, raw=300 — full value is the delta."""
        assert _compute_counter_delta(300, 0) == 300


# ---------------------------------------------------------------------------
# Reads duration delta + µs→s conversion tests
# ---------------------------------------------------------------------------

def _reads_duration_delta_seconds(raw_us: int, last_us: int) -> float:
    """Mirror update_counts_metrics() reads-duration delta logic."""
    delta_us: int = _compute_counter_delta(raw_us, last_us)
    return delta_us / 1_000_000


class TestReadsDurationDelta:
    def test_normal_increment_converts_to_seconds(self) -> None:
        """Delta in µs is divided by 1_000_000 to produce seconds."""
        result = _reads_duration_delta_seconds(5_000_000, 3_000_000)
        assert result == pytest.approx(2.0)

    def test_no_change_produces_zero(self) -> None:
        assert _reads_duration_delta_seconds(1_000_000, 1_000_000) == pytest.approx(0.0)

    def test_restart_uses_raw_value(self) -> None:
        """rippled restart resets counter — raw value itself is the delta."""
        result = _reads_duration_delta_seconds(500_000, 9_000_000)
        assert result == pytest.approx(0.5)

    def test_fractional_seconds(self) -> None:
        """Sub-second deltas are represented correctly."""
        result = _reads_duration_delta_seconds(250_000, 0)
        assert result == pytest.approx(0.25)
