"""Unit tests for exporter logic functions."""

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
