#!/usr/bin/env python3
"""Rippled Prometheus exporter.

Scrapes rippled JSON-RPC endpoints and exposes metrics for Prometheus.

Recommended deployment:
- one exporter instance per rippled node
- exporter shares the rippled container's network namespace (network_mode: container:)
  and connects directly to 127.0.0.1:5005 — no rippled.cfg changes required

Configuration (environment variables):
    RIPPLED_URL               JSON-RPC endpoint (default: http://127.0.0.1:5005)
    NODE_TYPE                 Label applied to all metrics, e.g. "peer" or "validator"
    SCRAPE_INTERVAL           Seconds between scrapes (default: 5)
    METRICS_PORT              Port to expose Prometheus metrics on (default: 9999)
    MAX_CONSECUTIVE_FAILURES  Exit after this many consecutive core scrape failures (default: 60)
"""

import argparse
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import requests
from prometheus_client import Counter as PromCounter, Gauge, start_http_server

__version__ = "1.3.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def _parse_env_int(name: str, default: int) -> int:
    raw: str = os.environ.get(name, str(default))
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"Environment variable {name}={raw!r} must be an integer") from None


RIPPLED_URL: str = os.environ.get("RIPPLED_URL", "http://127.0.0.1:5005")
NODE_TYPE: str = os.environ.get("NODE_TYPE", "peer")
SCRAPE_INTERVAL: int = _parse_env_int("SCRAPE_INTERVAL", 5)
METRICS_PORT: int = _parse_env_int("METRICS_PORT", 9999)

SESSION = requests.Session()

SERVER_STATE_VALUES: dict[str, int] = {
    "disconnected": 0,
    "connected": 1,
    "syncing": 2,
    "tracking": 3,
    "full": 4,
    "validating": 5,
    "proposing": 6,
}

NODE_SIZE_VALUES: dict[str, int] = {
    "tiny": 0,
    "small": 1,
    "medium": 2,
    "large": 3,
    "huge": 4,
}

CONSENSUS_PHASE_VALUES: dict[str, int] = {
    "open": 0,
    "establish": 1,
    "accepted": 2,
}

COMMON_LABELS: list[str] = ["node_type"]

# --- exporter-local state ---
# One exporter process monitors one rippled node, so single-value state is fine.
_peer_disconnects_last: int = 0
_peer_disconnects_resources_last: int = 0
_db_node_writes_last: int = 0
_db_node_reads_last: int = 0
_db_node_read_bytes_last: int = 0
_db_node_written_bytes_last: int = 0

# Tracks last-seen rippled uptime; used to detect rippled restarts across scrapes
_last_rippled_uptime_seconds: int | None = None

_peer_version_labels_last: set[str] = set()
_job_queue_labels_last: set[str] = set()
_job_type_labels_last: set[str] = set()

# Tracks the last seen build_version label so the old label can be zeroed when rippled upgrades
_build_version_last: str | None = None

# Tracks which validator list site URIs were seen last scrape so stale ones can be zeroed out
_validator_site_labels_last: set[str] = set()

# --- exporter metrics ---

rippled_scrape_success = Gauge(
    "rippled_scrape_success",
    "1 if the last full scrape had all core endpoints succeed, 0 otherwise",
    COMMON_LABELS,
)

rippled_endpoint_scrape_success = Gauge(
    "rippled_endpoint_scrape_success",
    "1 if the last scrape of a given endpoint succeeded, 0 otherwise",
    COMMON_LABELS + ["endpoint"],
)

rippled_scrape_duration_seconds = Gauge(
    "rippled_scrape_duration_seconds",
    "Duration of the last full scrape in seconds",
    COMMON_LABELS,
)

rippled_last_scrape_success_timestamp_seconds = Gauge(
    "rippled_last_scrape_success_timestamp_seconds",
    "Unix timestamp of the last successful full scrape",
    COMMON_LABELS,
)

# --- server_info metrics ---

rippled_server_state = Gauge(
    "rippled_server_state",
    "Server state as numeric value (0=disconnected 1=connected 2=syncing 3=tracking 4=full 5=validating 6=proposing)",
    COMMON_LABELS,
)

rippled_server_state_info = Gauge(
    "rippled_server_state_info",
    "Server state as label, value is 1 when active",
    COMMON_LABELS + ["state"],
)

rippled_peers_total = Gauge(
    "rippled_peers_total",
    "Number of peers currently connected",
    COMMON_LABELS,
)

# prometheus_client exposes these as rippled_peer_disconnects_total and
# rippled_peer_disconnects_resources_total automatically.
rippled_peer_disconnects = PromCounter(
    "rippled_peer_disconnects",
    "Peer disconnects since rippled startup (cumulative, use rate())",
    COMMON_LABELS,
)

rippled_peer_disconnects_resources = PromCounter(
    "rippled_peer_disconnects_resources",
    "Peer disconnects due to resource limits since rippled startup (cumulative, use rate())",
    COMMON_LABELS,
)

rippled_ledger_sequence = Gauge(
    "rippled_ledger_sequence",
    "Current validated ledger sequence number",
    COMMON_LABELS,
)

rippled_complete_ledgers_low = Gauge(
    "rippled_complete_ledgers_low",
    "Lowest ledger sequence in the node's complete ledger range (0 if empty)",
    COMMON_LABELS,
)

rippled_complete_ledgers_high = Gauge(
    "rippled_complete_ledgers_high",
    "Highest ledger sequence in the node's complete ledger range (0 if empty)",
    COMMON_LABELS,
)

rippled_ledger_age_seconds = Gauge(
    "rippled_ledger_age_seconds",
    "Age of the last validated ledger in seconds",
    COMMON_LABELS,
)

rippled_load_factor = Gauge(
    "rippled_load_factor",
    "Current load factor (1 = no load)",
    COMMON_LABELS,
)

rippled_load_factor_local = Gauge(
    "rippled_load_factor_local",
    "Load factor based on load to this server only (1 = no load)",
    COMMON_LABELS,
)

rippled_load_factor_net = Gauge(
    "rippled_load_factor_net",
    "Load factor estimated by the rest of the network (1 = no load)",
    COMMON_LABELS,
)

rippled_load_factor_cluster = Gauge(
    "rippled_load_factor_cluster",
    "Load factor based on load to servers in this cluster (1 = no load; 1 if no cluster)",
    COMMON_LABELS,
)

rippled_uptime_seconds = Gauge(
    "rippled_uptime_seconds",
    "Server uptime in seconds",
    COMMON_LABELS,
)

rippled_initial_sync_duration_seconds = Gauge(
    "rippled_initial_sync_duration_seconds",
    "Time taken for the node to complete initial sync on startup (seconds)",
    COMMON_LABELS,
)

rippled_job_type_per_second = Gauge(
    "rippled_job_type_per_second",
    "Jobs processed per second for each job type from the server load report",
    COMMON_LABELS + ["job_type"],
)

rippled_job_type_peak_time_ms = Gauge(
    "rippled_job_type_peak_time_ms",
    "Peak execution time in milliseconds for each job type",
    COMMON_LABELS + ["job_type"],
)

rippled_job_type_avg_time_ms = Gauge(
    "rippled_job_type_avg_time_ms",
    "Average execution time in milliseconds for each job type",
    COMMON_LABELS + ["job_type"],
)

rippled_job_type_in_progress = Gauge(
    "rippled_job_type_in_progress",
    "Number of jobs currently in progress for each job type",
    COMMON_LABELS + ["job_type"],
)

rippled_io_latency_ms = Gauge(
    "rippled_io_latency_ms",
    "IO latency in milliseconds",
    COMMON_LABELS,
)

rippled_validation_quorum = Gauge(
    "rippled_validation_quorum",
    "Minimum number of trusted validations required to validate a ledger",
    COMMON_LABELS,
)

rippled_last_close_converge_time_seconds = Gauge(
    "rippled_last_close_converge_time_seconds",
    "Time taken for the last ledger close to converge in seconds",
    COMMON_LABELS,
)

rippled_last_close_proposers = Gauge(
    "rippled_last_close_proposers",
    "Number of trusted validators that proposed the last closed ledger",
    COMMON_LABELS,
)

rippled_transaction_overflow = Gauge(
    "rippled_transaction_overflow",
    "Transaction queue overflow count reported by rippled since startup",
    COMMON_LABELS,
)

rippled_validator_list_count = Gauge(
    "rippled_validator_list_count",
    "Number of validator lists loaded",
    COMMON_LABELS,
)

rippled_validator_list_active = Gauge(
    "rippled_validator_list_active",
    "1 if validator list status is active, 0 otherwise",
    COMMON_LABELS,
)

rippled_validator_list_expiry_timestamp = Gauge(
    "rippled_validator_list_expiry_timestamp",
    "Unix timestamp when the validator list expires",
    COMMON_LABELS,
)

rippled_load_threads = Gauge(
    "rippled_load_threads",
    "Number of threads used by the rippled job scheduler",
    COMMON_LABELS,
)

# --- build info metric ---

rippled_build_info = Gauge(
    "rippled_build_info",
    "rippled build version; value is always 1, use the version label to read it",
    COMMON_LABELS + ["version"],
)

# --- fee metrics ---

rippled_fee_base_drops = Gauge(
    "rippled_fee_base_drops",
    "Base transaction fee in drops of XRP",
    COMMON_LABELS,
)

rippled_fee_median_drops = Gauge(
    "rippled_fee_median_drops",
    "Median transaction fee in drops",
    COMMON_LABELS,
)

rippled_fee_open_ledger_drops = Gauge(
    "rippled_fee_open_ledger_drops",
    "Minimum fee to get a transaction into the current open ledger in drops",
    COMMON_LABELS,
)

rippled_fee_minimum_drops = Gauge(
    "rippled_fee_minimum_drops",
    "Minimum transaction fee accepted by this node in drops",
    COMMON_LABELS,
)

rippled_ledger_current_tx_count = Gauge(
    "rippled_ledger_current_tx_count",
    "Number of transactions in the current open ledger",
    COMMON_LABELS,
)

rippled_ledger_queue_tx_count = Gauge(
    "rippled_ledger_queue_tx_count",
    "Number of transactions currently waiting in the transaction queue",
    COMMON_LABELS,
)

rippled_ledger_queue_tx_max = Gauge(
    "rippled_ledger_queue_tx_max",
    "Maximum number of transactions the transaction queue can hold",
    COMMON_LABELS,
)

rippled_ledger_expected_tx_count = Gauge(
    "rippled_ledger_expected_tx_count",
    "Expected number of transactions per ledger based on recent history",
    COMMON_LABELS,
)

# --- peer detail metrics ---

rippled_peer_latency_avg_ms = Gauge(
    "rippled_peer_latency_avg_ms",
    "Average latency across all connected peers in milliseconds",
    COMMON_LABELS,
)

rippled_peer_latency_min_ms = Gauge(
    "rippled_peer_latency_min_ms",
    "Minimum latency across all connected peers in milliseconds",
    COMMON_LABELS,
)

rippled_peer_latency_max_ms = Gauge(
    "rippled_peer_latency_max_ms",
    "Maximum latency across all connected peers in milliseconds",
    COMMON_LABELS,
)

rippled_peer_inbound_total = Gauge(
    "rippled_peer_inbound_total",
    "Number of inbound peer connections",
    COMMON_LABELS,
)

rippled_peer_outbound_total = Gauge(
    "rippled_peer_outbound_total",
    "Number of outbound peer connections",
    COMMON_LABELS,
)

rippled_peer_version_total = Gauge(
    "rippled_peer_version_total",
    "Number of connected peers running each rippled version",
    COMMON_LABELS + ["version"],
)

# --- get_counts metrics ---

rippled_cache_ledger_hit_rate = Gauge(
    "rippled_cache_ledger_hit_rate",
    "Ledger cache hit rate as a percentage (0-100)",
    COMMON_LABELS,
)

rippled_cache_node_read_hit_rate = Gauge(
    "rippled_cache_node_read_hit_rate",
    "Node read cache hit rate as a percentage (0-100)",
    COMMON_LABELS,
)

rippled_cache_al_hit_rate = Gauge(
    "rippled_cache_al_hit_rate",
    "AccountLedger (AL) cache hit rate as a percentage (0-100)",
    COMMON_LABELS,
)

rippled_cache_sle_hit_rate = Gauge(
    "rippled_cache_sle_hit_rate",
    "State Ledger Entry (SLE) cache hit rate as a percentage (0-100)",
    COMMON_LABELS,
)

rippled_db_read_queue = Gauge(
    "rippled_db_read_queue",
    "Current number of pending database read requests",
    COMMON_LABELS,
)

rippled_db_write_load = Gauge(
    "rippled_db_write_load",
    "Current database write load",
    COMMON_LABELS,
)

# --- consensus_info metrics ---

rippled_consensus_proposing = Gauge(
    "rippled_consensus_proposing",
    "1 if the node is currently proposing in the consensus round, 0 otherwise",
    COMMON_LABELS,
)

rippled_consensus_synched = Gauge(
    "rippled_consensus_synched",
    "1 if the node is synched with the network, 0 otherwise",
    COMMON_LABELS,
)

rippled_consensus_validating = Gauge(
    "rippled_consensus_validating",
    "1 if the node is currently sending validations, 0 otherwise",
    COMMON_LABELS,
)

rippled_consensus_disputes = Gauge(
    "rippled_consensus_disputes",
    "Number of disputed transactions in the current consensus round",
    COMMON_LABELS,
)

# --- validator_info metrics ---

rippled_validator_manifest_seq = Gauge(
    "rippled_validator_manifest_seq",
    "Validator manifest sequence number; changes when the validator token is rotated",
    COMMON_LABELS,
)

# --- server_state metrics ---

rippled_amendment_blocked = Gauge(
    "rippled_amendment_blocked",
    "1 if the node is amendment blocked and cannot process newer amendment features",
    COMMON_LABELS,
)

rippled_load_factor_server = Gauge(
    "rippled_load_factor_server",
    "Server's own local load factor (not network-wide); 1 = no load",
    COMMON_LABELS,
)

rippled_closed_ledger_sequence = Gauge(
    "rippled_closed_ledger_sequence",
    "Most recently closed ledger sequence number (may not yet be validated)",
    COMMON_LABELS,
)

rippled_closed_ledger_age_seconds = Gauge(
    "rippled_closed_ledger_age_seconds",
    "Age of the most recently closed ledger in seconds",
    COMMON_LABELS,
)

rippled_reserve_base_drops = Gauge(
    "rippled_reserve_base_drops",
    "Base account reserve requirement in drops of XRP",
    COMMON_LABELS,
)

rippled_reserve_inc_drops = Gauge(
    "rippled_reserve_inc_drops",
    "Reserve increment per owned object in drops of XRP",
    COMMON_LABELS,
)

rippled_state_accounting_duration_seconds = Gauge(
    "rippled_state_accounting_duration_seconds",
    "Cumulative seconds spent in each server state since startup",
    COMMON_LABELS + ["state"],
)

rippled_state_accounting_transitions = Gauge(
    "rippled_state_accounting_transitions",
    "Number of transitions into each server state since startup",
    COMMON_LABELS + ["state"],
)

# --- validator_list_sites metrics ---

rippled_validator_list_site_status = Gauge(
    "rippled_validator_list_site_status",
    "1 if the last fetch from this validator list site was accepted, 0 otherwise",
    COMMON_LABELS + ["uri"],
)

rippled_validator_list_site_last_refresh_timestamp_seconds = Gauge(
    "rippled_validator_list_site_last_refresh_timestamp_seconds",
    "Unix timestamp of the last successful refresh from this validator list site",
    COMMON_LABELS + ["uri"],
)

# --- server_state additional metrics ---

rippled_load_base = Gauge(
    "rippled_load_base",
    "Base load value used to normalize load factors (typically 256)",
    COMMON_LABELS,
)

rippled_load_factor_fee_escalation = Gauge(
    "rippled_load_factor_fee_escalation",
    "Fee escalation component of the load factor; > 1 when fees are being escalated due to load",
    COMMON_LABELS,
)

rippled_load_factor_fee_queue = Gauge(
    "rippled_load_factor_fee_queue",
    "Fee queue component of the load factor; > 1 when the transaction queue is pressuring fees",
    COMMON_LABELS,
)

rippled_server_state_duration_seconds = Gauge(
    "rippled_server_state_duration_seconds",
    "How long the server has been in its current server state in seconds",
    COMMON_LABELS,
)

# --- server_info additional metrics ---

rippled_network_id = Gauge(
    "rippled_network_id",
    "Network ID reported by this node (0 = XRPL mainnet)",
    COMMON_LABELS,
)

rippled_node_size = Gauge(
    "rippled_node_size",
    "Configured node size as numeric (0=tiny 1=small 2=medium 3=large 4=huge)",
    COMMON_LABELS,
)

# --- get_counts additional metrics ---

rippled_db_node_writes = PromCounter(
    "rippled_db_node_writes",
    "Total node write operations to the NuDB/RocksDB store since rippled startup (use rate())",
    COMMON_LABELS,
)

rippled_db_node_reads = PromCounter(
    "rippled_db_node_reads",
    "Total node read operations from the NuDB/RocksDB store since rippled startup (use rate())",
    COMMON_LABELS,
)

rippled_db_size_kb = Gauge(
    "rippled_db_size_kb",
    "Database file size in kilobytes",
    COMMON_LABELS + ["database"],
)

rippled_cache_treenode_size = Gauge(
    "rippled_cache_treenode_size",
    "Number of objects in the SHAMap tree node cache",
    COMMON_LABELS,
)

rippled_cache_fullbelow_size = Gauge(
    "rippled_cache_fullbelow_size",
    "Number of entries in the full-below cache (tracks SHAMap nodes known to have all descendants present)",
    COMMON_LABELS,
)

rippled_validations_cached = Gauge(
    "rippled_validations_cached",
    "Number of validator signatures currently held in the validation cache",
    COMMON_LABELS,
)

rippled_db_node_read_bytes = PromCounter(
    "rippled_db_node_read_bytes",
    "Total bytes read from the NuDB/RocksDB node store since rippled startup (use rate())",
    COMMON_LABELS,
)

rippled_db_node_written_bytes = PromCounter(
    "rippled_db_node_written_bytes",
    "Total bytes written to the NuDB/RocksDB node store since rippled startup (use rate())",
    COMMON_LABELS,
)

rippled_objects_in_memory = Gauge(
    "rippled_objects_in_memory",
    "Number of objects of a given type currently held in memory",
    COMMON_LABELS + ["object_type"],
)

rippled_historical_perminute = Gauge(
    "rippled_historical_perminute",
    "Rate of historical ledger data processing per minute",
    COMMON_LABELS,
)

rippled_cache_al_size = Gauge(
    "rippled_cache_al_size",
    "Number of entries in the AccountLedger (AL) cache",
    COMMON_LABELS,
)

rippled_db_read_threads_running = Gauge(
    "rippled_db_read_threads_running",
    "Number of node store read threads currently executing",
    COMMON_LABELS,
)

rippled_db_read_threads_total = Gauge(
    "rippled_db_read_threads_total",
    "Total number of node store read threads",
    COMMON_LABELS,
)

rippled_db_node_reads_duration_seconds = Gauge(
    "rippled_db_node_reads_duration_seconds",
    "Cumulative time spent on node store read operations since rippled startup (seconds)",
    COMMON_LABELS,
)

rippled_cache_treenode_track_size = Gauge(
    "rippled_cache_treenode_track_size",
    "Number of entries being tracked in the SHAMap tree node tracker",
    COMMON_LABELS,
)

# --- peers additional metrics ---

rippled_peer_non_sane_total = Gauge(
    "rippled_peer_non_sane_total",
    "Number of connected peers with non-sane status (insane or unknown); should always be 0",
    COMMON_LABELS,
)

rippled_peer_messages = Gauge(
    "rippled_peer_messages",
    "Total protocol messages accumulated across all currently connected peers",
    COMMON_LABELS,
)

# --- consensus additional metrics ---

rippled_consensus_phase = Gauge(
    "rippled_consensus_phase",
    "Current consensus phase as numeric (0=open 1=establish 2=accepted)",
    COMMON_LABELS,
)

# --- feature / amendment metrics ---

rippled_amendments_enabled_total = Gauge(
    "rippled_amendments_enabled_total",
    "Total number of amendments currently enabled on this network",
    COMMON_LABELS,
)

rippled_amendments_pending_total = Gauge(
    "rippled_amendments_pending_total",
    "Number of amendments that have reached the voting threshold and are pending activation",
    COMMON_LABELS,
)

rippled_amendments_near_threshold_total = Gauge(
    "rippled_amendments_near_threshold_total",
    "Number of amendments with >= 75% of required validator votes but not yet at threshold",
    COMMON_LABELS,
)

# --- job_queue metrics ---

rippled_job_queue_in_progress = Gauge(
    "rippled_job_queue_in_progress",
    "Number of jobs currently executing for each job type",
    COMMON_LABELS + ["job_type"],
)

rippled_job_queue_deferred = Gauge(
    "rippled_job_queue_deferred",
    "Number of jobs queued but not yet started for each job type; sustained non-zero values indicate a processing backlog",
    COMMON_LABELS + ["job_type"],
)

# --- validators metrics ---

rippled_unl_size = Gauge(
    "rippled_unl_size",
    "Number of trusted validators in the UNL (Unique Node List)",
    COMMON_LABELS,
)

# --- fetch_info metrics ---

rippled_fetch_active_total = Gauge(
    "rippled_fetch_active_total",
    "Number of ledgers currently tracked in the fetch queue",
    COMMON_LABELS,
)

rippled_fetch_incomplete_total = Gauge(
    "rippled_fetch_incomplete_total",
    "Number of ledgers in the fetch queue not yet fully acquired; sustained > 0 may indicate sync issues",
    COMMON_LABELS,
)

rippled_fetch_timeouts_total = Gauge(
    "rippled_fetch_timeouts_total",
    "Sum of fetch timeouts across all active ledger acquisitions; spikes indicate network stress during acquisition",
    COMMON_LABELS,
)


def parse_rippled_timestamp(timestamp_str: str) -> float:
    """Parse a rippled timestamp string to a Unix timestamp."""
    truncated: str = re.sub(r"(\.\d{6})\d+", r"\1", timestamp_str.replace(" UTC", ""))
    dt: datetime = datetime.strptime(truncated, "%Y-%b-%d %H:%M:%S.%f")
    return dt.replace(tzinfo=timezone.utc).timestamp()


class RpcError(ValueError):
    """Raised when rippled returns a JSON-RPC error response."""

    def __init__(self, method: str, error: dict[str, Any]) -> None:
        self.method = method
        self.error = error
        self.error_code: int = int(error.get("error_code", -1))
        super().__init__(f"{method} returned RPC error: {error!r}")


def query_rpc(method: str) -> dict[str, Any]:
    """POST a JSON-RPC request to the configured rippled endpoint."""
    payload = {"method": method, "params": [{}]}
    response = SESSION.post(RIPPLED_URL, json=payload, timeout=10)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, dict):
        raise ValueError(f"{method} returned non-dict JSON: {data!r}")

    if "result" not in data:
        raise ValueError(f"{method} missing result field: {data!r}")

    result = data["result"]
    if not isinstance(result, dict):
        raise ValueError(f"{method} result is not an object: {result!r}")

    if "error" in result:
        raise RpcError(method, result)

    return data


def set_endpoint_success(endpoint: str, success: int) -> None:
    rippled_endpoint_scrape_success.labels(
        node_type=NODE_TYPE, endpoint=endpoint
    ).set(success)


def _update_job_type_metrics(job_types: list[dict[str, Any]]) -> None:
    """Update per-job-type rate, peak time, avg time, and in-progress gauges from server_info load."""
    global _job_type_labels_last

    current_labels: set[str] = set()
    for job in job_types:
        if not isinstance(job, dict):
            continue
        job_type: str = str(job.get("job_type", ""))
        if not job_type:
            continue
        current_labels.add(job_type)
        rippled_job_type_per_second.labels(node_type=NODE_TYPE, job_type=job_type).set(
            int(job.get("per_second", 0))
        )
        rippled_job_type_peak_time_ms.labels(node_type=NODE_TYPE, job_type=job_type).set(
            int(job.get("peak_time", 0))
        )
        rippled_job_type_avg_time_ms.labels(node_type=NODE_TYPE, job_type=job_type).set(
            int(job.get("avg_time", 0))
        )
        rippled_job_type_in_progress.labels(node_type=NODE_TYPE, job_type=job_type).set(
            int(job.get("in_progress", 0))
        )

    for stale in _job_type_labels_last - current_labels:
        rippled_job_type_per_second.labels(node_type=NODE_TYPE, job_type=stale).set(0)
        rippled_job_type_peak_time_ms.labels(node_type=NODE_TYPE, job_type=stale).set(0)
        rippled_job_type_avg_time_ms.labels(node_type=NODE_TYPE, job_type=stale).set(0)
        rippled_job_type_in_progress.labels(node_type=NODE_TYPE, job_type=stale).set(0)

    _job_type_labels_last = current_labels


def update_server_info_metrics() -> None:
    global _peer_disconnects_last
    global _peer_disconnects_resources_last
    global _last_rippled_uptime_seconds
    global _build_version_last

    data = query_rpc("server_info")
    result = data["result"]
    info = result.get("info")
    if not isinstance(info, dict):
        raise ValueError(f"server_info missing info object: {result!r}")

    state: str = str(info.get("server_state", "unknown"))
    state_numeric: int = SERVER_STATE_VALUES.get(state, -1)
    rippled_server_state.labels(node_type=NODE_TYPE).set(state_numeric)

    for state_name in SERVER_STATE_VALUES:
        rippled_server_state_info.labels(
            node_type=NODE_TYPE, state=state_name
        ).set(1 if state_name == state else 0)

    rippled_peers_total.labels(node_type=NODE_TYPE).set(int(info.get("peers", 0)))

    current_uptime: int = int(info.get("uptime", 0))
    rippled_uptime_seconds.labels(node_type=NODE_TYPE).set(current_uptime)

    raw_disconnects: int = int(info.get("peer_disconnects", 0))
    raw_disconnects_resources: int = int(info.get("peer_disconnects_resources", 0))

    # Use uptime comparison to detect rippled restarts — more reliable than
    # comparing counter deltas alone, since restarts reset counters to 0.
    restarted: bool = (
        _last_rippled_uptime_seconds is not None
        and current_uptime < _last_rippled_uptime_seconds
    )

    if restarted:
        logger.info(
            "Detected rippled restart for %s: uptime dropped from %s to %s",
            NODE_TYPE,
            _last_rippled_uptime_seconds,
            current_uptime,
        )

    disconnect_delta: int = (
        raw_disconnects
        if restarted or raw_disconnects < _peer_disconnects_last
        else raw_disconnects - _peer_disconnects_last
    )
    disconnects_resources_delta: int = (
        raw_disconnects_resources
        if restarted or raw_disconnects_resources < _peer_disconnects_resources_last
        else raw_disconnects_resources - _peer_disconnects_resources_last
    )

    if disconnect_delta > 0:
        rippled_peer_disconnects.labels(node_type=NODE_TYPE).inc(disconnect_delta)
    if disconnects_resources_delta > 0:
        rippled_peer_disconnects_resources.labels(node_type=NODE_TYPE).inc(
            disconnects_resources_delta
        )

    _peer_disconnects_last = raw_disconnects
    _peer_disconnects_resources_last = raw_disconnects_resources
    _last_rippled_uptime_seconds = current_uptime

    validated_ledger: dict[str, Any] = info.get("validated_ledger", {})
    if validated_ledger:
        rippled_ledger_sequence.labels(node_type=NODE_TYPE).set(
            int(validated_ledger.get("seq", 0))
        )
        rippled_ledger_age_seconds.labels(node_type=NODE_TYPE).set(
            int(validated_ledger.get("age", 0))
        )
    else:
        rippled_ledger_sequence.labels(node_type=NODE_TYPE).set(0)
        rippled_ledger_age_seconds.labels(node_type=NODE_TYPE).set(0)

    complete_ledgers: str = str(info.get("complete_ledgers", "empty"))
    if complete_ledgers not in ("empty", "") and "-" in complete_ledgers:
        # rippled can return a comma-separated list of ranges when the node has gaps
        # in history (e.g. "32570-1000000,1000500-2000000") — take the global min/max.
        try:
            range_segments: list[list[str]] = [seg.split("-") for seg in complete_ledgers.split(",")]
            low: int = min(int(r[0]) for r in range_segments if len(r) == 2)
            high: int = max(int(r[1]) for r in range_segments if len(r) == 2)
            rippled_complete_ledgers_low.labels(node_type=NODE_TYPE).set(low)
            rippled_complete_ledgers_high.labels(node_type=NODE_TYPE).set(high)
        except ValueError as exc:
            logger.warning(
                "Could not parse complete_ledgers %r for %s: %s — setting to 0",
                complete_ledgers,
                NODE_TYPE,
                exc,
            )
            rippled_complete_ledgers_low.labels(node_type=NODE_TYPE).set(0)
            rippled_complete_ledgers_high.labels(node_type=NODE_TYPE).set(0)
    else:
        rippled_complete_ledgers_low.labels(node_type=NODE_TYPE).set(0)
        rippled_complete_ledgers_high.labels(node_type=NODE_TYPE).set(0)

    rippled_load_factor.labels(node_type=NODE_TYPE).set(float(info.get("load_factor", 0)))
    rippled_load_factor_local.labels(node_type=NODE_TYPE).set(float(info.get("load_factor_local", 0)))
    rippled_load_factor_net.labels(node_type=NODE_TYPE).set(float(info.get("load_factor_net", 0)))
    rippled_load_factor_cluster.labels(node_type=NODE_TYPE).set(float(info.get("load_factor_cluster", 0)))
    rippled_io_latency_ms.labels(node_type=NODE_TYPE).set(float(info.get("io_latency_ms", 0)))

    rippled_initial_sync_duration_seconds.labels(node_type=NODE_TYPE).set(
        int(info.get("initial_sync_duration_us", 0)) / 1_000_000
    )

    load: dict[str, Any] = info.get("load", {})
    if load:
        rippled_load_threads.labels(node_type=NODE_TYPE).set(int(load.get("threads", 0)))
        _update_job_type_metrics(load.get("job_types", []))
    else:
        rippled_load_threads.labels(node_type=NODE_TYPE).set(0)
        _update_job_type_metrics([])

    rippled_transaction_overflow.labels(node_type=NODE_TYPE).set(
        int(info.get("jq_trans_overflow", 0))
    )

    rippled_validation_quorum.labels(node_type=NODE_TYPE).set(
        int(info.get("validation_quorum", 0))
    )

    last_close: dict[str, Any] = info.get("last_close", {})
    if last_close:
        rippled_last_close_converge_time_seconds.labels(node_type=NODE_TYPE).set(
            float(last_close.get("converge_time_s", 0))
        )
        rippled_last_close_proposers.labels(node_type=NODE_TYPE).set(
            int(last_close.get("proposers", 0))
        )
    else:
        rippled_last_close_converge_time_seconds.labels(node_type=NODE_TYPE).set(0)
        rippled_last_close_proposers.labels(node_type=NODE_TYPE).set(0)

    validator_list: dict[str, Any] = info.get("validator_list", {})
    if validator_list:
        rippled_validator_list_count.labels(node_type=NODE_TYPE).set(
            int(validator_list.get("count", 0))
        )
        rippled_validator_list_active.labels(node_type=NODE_TYPE).set(
            1 if validator_list.get("status") == "active" else 0
        )
        expiration_str: str = str(validator_list.get("expiration", ""))
        if expiration_str:
            try:
                rippled_validator_list_expiry_timestamp.labels(
                    node_type=NODE_TYPE
                ).set(parse_rippled_timestamp(expiration_str))
            except ValueError as exc:
                logger.warning(
                    "Could not parse validator_list expiration %r: %s",
                    expiration_str,
                    exc,
                )
                rippled_validator_list_expiry_timestamp.labels(node_type=NODE_TYPE).set(0)
        else:
            rippled_validator_list_expiry_timestamp.labels(node_type=NODE_TYPE).set(0)
    else:
        rippled_validator_list_count.labels(node_type=NODE_TYPE).set(0)
        rippled_validator_list_active.labels(node_type=NODE_TYPE).set(0)
        rippled_validator_list_expiry_timestamp.labels(node_type=NODE_TYPE).set(0)

    build_version: str = str(info.get("build_version", "unknown"))
    if _build_version_last and _build_version_last != build_version:
        # Zero out the old version label so Grafana doesn't show stale build versions
        rippled_build_info.labels(
            node_type=NODE_TYPE, version=_build_version_last
        ).set(0)
    rippled_build_info.labels(node_type=NODE_TYPE, version=build_version).set(1)
    _build_version_last = build_version

    rippled_amendment_blocked.labels(node_type=NODE_TYPE).set(
        1 if info.get("amendment_blocked", False) else 0
    )
    rippled_network_id.labels(node_type=NODE_TYPE).set(int(info.get("network_id", 0)))

    node_size_str: str = str(info.get("node_size", "huge"))
    rippled_node_size.labels(node_type=NODE_TYPE).set(
        NODE_SIZE_VALUES.get(node_size_str, -1)
    )

    logger.info(
        "server_info %s state=%s peers=%s ledger=%s age=%ss",
        NODE_TYPE,
        state,
        info.get("peers", 0),
        validated_ledger.get("seq", 0) if validated_ledger else "?",
        validated_ledger.get("age", 0) if validated_ledger else "?",
    )


def update_fee_metrics() -> None:
    data = query_rpc("fee")
    result: dict[str, Any] = data["result"]
    drops: dict[str, Any] = result.get("drops", {})

    rippled_fee_base_drops.labels(node_type=NODE_TYPE).set(int(drops.get("base_fee", 0)))
    rippled_fee_median_drops.labels(node_type=NODE_TYPE).set(int(drops.get("median_fee", 0)))
    rippled_fee_open_ledger_drops.labels(node_type=NODE_TYPE).set(
        int(drops.get("open_ledger_fee", 0))
    )
    rippled_fee_minimum_drops.labels(node_type=NODE_TYPE).set(
        int(drops.get("minimum_fee", 0))
    )
    rippled_ledger_current_tx_count.labels(node_type=NODE_TYPE).set(
        int(result.get("current_ledger_size", 0))
    )
    rippled_ledger_queue_tx_count.labels(node_type=NODE_TYPE).set(
        int(result.get("current_queue_size", 0))
    )
    rippled_ledger_queue_tx_max.labels(node_type=NODE_TYPE).set(
        int(result.get("max_queue_size", 0))
    )
    rippled_ledger_expected_tx_count.labels(node_type=NODE_TYPE).set(
        int(result.get("expected_ledger_size", 0))
    )

    logger.info(
        "fee %s median=%s open_ledger=%s queue=%s/%s",
        NODE_TYPE,
        drops.get("median_fee"),
        drops.get("open_ledger_fee"),
        result.get("current_queue_size"),
        result.get("max_queue_size"),
    )


def update_peer_metrics() -> None:
    global _peer_version_labels_last

    data = query_rpc("peers")
    result = data["result"]
    peers = result.get("peers", [])
    if not isinstance(peers, list):
        raise ValueError(f"peers missing peers list: {result!r}")

    if not peers:
        rippled_peer_latency_avg_ms.labels(node_type=NODE_TYPE).set(0)
        rippled_peer_latency_min_ms.labels(node_type=NODE_TYPE).set(0)
        rippled_peer_latency_max_ms.labels(node_type=NODE_TYPE).set(0)
        rippled_peer_inbound_total.labels(node_type=NODE_TYPE).set(0)
        rippled_peer_outbound_total.labels(node_type=NODE_TYPE).set(0)
        rippled_peer_non_sane_total.labels(node_type=NODE_TYPE).set(0)
        rippled_peer_messages.labels(node_type=NODE_TYPE).set(0)

        for stale_version in _peer_version_labels_last:
            rippled_peer_version_total.labels(
                node_type=NODE_TYPE, version=stale_version
            ).set(0)
        _peer_version_labels_last = set()

        logger.warning("peers %s returned empty list", NODE_TYPE)
        return

    latencies: list[int] = [int(p["latency"]) for p in peers if "latency" in p]
    avg_latency: float = 0.0
    if latencies:
        # Trim the top 5% of latency values before averaging so that a single
        # dying peer (e.g. 18,000ms) doesn't skew the reported average.
        sorted_latencies: list[int] = sorted(latencies)
        # Trim top 5% before averaging; guard ensures we never trim to an empty list.
        trim: int = max(0, len(sorted_latencies) // 20)
        trimmed: list[int] = sorted_latencies[:-trim] if trim > 0 else sorted_latencies
        avg_latency = sum(trimmed) / len(trimmed)
        rippled_peer_latency_avg_ms.labels(node_type=NODE_TYPE).set(avg_latency)
        rippled_peer_latency_min_ms.labels(node_type=NODE_TYPE).set(min(latencies))
        rippled_peer_latency_max_ms.labels(node_type=NODE_TYPE).set(max(latencies))
    else:
        rippled_peer_latency_avg_ms.labels(node_type=NODE_TYPE).set(0)
        rippled_peer_latency_min_ms.labels(node_type=NODE_TYPE).set(0)
        rippled_peer_latency_max_ms.labels(node_type=NODE_TYPE).set(0)

    inbound_count: int = sum(1 for p in peers if p.get("inbound", False))
    rippled_peer_inbound_total.labels(node_type=NODE_TYPE).set(inbound_count)
    rippled_peer_outbound_total.labels(node_type=NODE_TYPE).set(len(peers) - inbound_count)

    non_sane_count: int = sum(1 for p in peers if p.get("sanity", "sane") != "sane")
    rippled_peer_non_sane_total.labels(node_type=NODE_TYPE).set(non_sane_count)

    # Sum of per-peer message counts since each connection was established.
    # Exposed as a Gauge (not Counter) because the total can decrease when peers disconnect.
    rippled_peer_messages.labels(node_type=NODE_TYPE).set(
        sum(int(p.get("messages", 0)) for p in peers)
    )

    version_counts: Counter[str] = Counter(str(p.get("version", "unknown")) for p in peers)
    current_versions: set[str] = set()
    for version, count in version_counts.items():
        rippled_peer_version_total.labels(
            node_type=NODE_TYPE, version=version
        ).set(count)
        current_versions.add(version)

    # Zero out any versions that were present last scrape but have now disappeared,
    # avoiding .clear() which triggers a prometheus_client internal error in some versions
    for stale_version in _peer_version_labels_last - current_versions:
        rippled_peer_version_total.labels(
            node_type=NODE_TYPE, version=stale_version
        ).set(0)
    _peer_version_labels_last = current_versions

    logger.info(
        "peers %s total=%s inbound=%s outbound=%s avg_latency=%.1fms",
        NODE_TYPE,
        len(peers),
        inbound_count,
        len(peers) - inbound_count,
        avg_latency,
    )


def update_counts_metrics() -> None:
    global _db_node_writes_last
    global _db_node_reads_last
    global _db_node_read_bytes_last
    global _db_node_written_bytes_last

    data = query_rpc("get_counts")
    result: dict[str, Any] = data["result"]

    rippled_cache_ledger_hit_rate.labels(node_type=NODE_TYPE).set(
        float(result.get("ledger_hit_rate", 0))
    )
    rippled_cache_al_hit_rate.labels(node_type=NODE_TYPE).set(
        float(result.get("AL_hit_rate", 0))
    )
    rippled_cache_sle_hit_rate.labels(node_type=NODE_TYPE).set(
        float(result.get("SLE_hit_rate", 0))
    )

    node_reads_hit: int = int(result.get("node_reads_hit", 0))
    node_reads_total: int = int(result.get("node_reads_total", 0))
    node_read_hit_rate: float = (
        (node_reads_hit / node_reads_total * 100.0) if node_reads_total > 0 else 0.0
    )
    rippled_cache_node_read_hit_rate.labels(node_type=NODE_TYPE).set(node_read_hit_rate)

    rippled_db_read_queue.labels(node_type=NODE_TYPE).set(int(result.get("read_queue", 0)))
    rippled_db_write_load.labels(node_type=NODE_TYPE).set(int(result.get("write_load", 0)))

    raw_node_writes: int = int(result.get("node_writes", 0))

    # If the raw value dropped below our last reading, rippled restarted and
    # reset its counters — treat the current value itself as the delta.
    writes_delta: int = (
        raw_node_writes - _db_node_writes_last
        if raw_node_writes >= _db_node_writes_last
        else raw_node_writes
    )
    reads_delta: int = (
        node_reads_total - _db_node_reads_last
        if node_reads_total >= _db_node_reads_last
        else node_reads_total
    )

    if writes_delta > 0:
        rippled_db_node_writes.labels(node_type=NODE_TYPE).inc(writes_delta)
    if reads_delta > 0:
        rippled_db_node_reads.labels(node_type=NODE_TYPE).inc(reads_delta)

    _db_node_writes_last = raw_node_writes
    _db_node_reads_last = node_reads_total

    raw_read_bytes: int = int(result.get("node_read_bytes", 0))
    raw_written_bytes: int = int(result.get("node_written_bytes", 0))

    read_bytes_delta: int = (
        raw_read_bytes - _db_node_read_bytes_last
        if raw_read_bytes >= _db_node_read_bytes_last
        else raw_read_bytes
    )
    written_bytes_delta: int = (
        raw_written_bytes - _db_node_written_bytes_last
        if raw_written_bytes >= _db_node_written_bytes_last
        else raw_written_bytes
    )

    if read_bytes_delta > 0:
        rippled_db_node_read_bytes.labels(node_type=NODE_TYPE).inc(read_bytes_delta)
    if written_bytes_delta > 0:
        rippled_db_node_written_bytes.labels(node_type=NODE_TYPE).inc(written_bytes_delta)

    _db_node_read_bytes_last = raw_read_bytes
    _db_node_written_bytes_last = raw_written_bytes

    rippled_historical_perminute.labels(node_type=NODE_TYPE).set(
        int(result.get("historical_perminute", 0))
    )

    for object_type, key in (
        ("Ledger", "ripple::Ledger"),
        ("Transaction", "ripple::Transaction"),
        ("STTx", "ripple::STTx"),
        ("STLedgerEntry", "ripple::STLedgerEntry"),
        ("STObject", "ripple::STObject"),
        ("STValidation", "ripple::STValidation"),
        ("SHAMapInnerNode", "ripple::SHAMapInnerNode"),
        ("SHAMapAccountStateLeafNode", "ripple::SHAMapAccountStateLeafNode"),
        ("HashRouterEntry", "ripple::HashRouter::Entry"),
        ("InboundLedger", "ripple::InboundLedger"),
    ):
        rippled_objects_in_memory.labels(node_type=NODE_TYPE, object_type=object_type).set(
            int(result.get(key, 0))
        )

    for db_name, key in (
        ("ledger", "dbKBLedger"),
        ("transaction", "dbKBTransaction"),
        ("total", "dbKBTotal"),
    ):
        rippled_db_size_kb.labels(node_type=NODE_TYPE, database=db_name).set(
            int(result.get(key, 0))
        )

    rippled_cache_treenode_size.labels(node_type=NODE_TYPE).set(
        int(result.get("treenode_cache_size", 0))
    )
    rippled_cache_treenode_track_size.labels(node_type=NODE_TYPE).set(
        int(result.get("treenode_track_size", 0))
    )
    rippled_cache_fullbelow_size.labels(node_type=NODE_TYPE).set(
        int(result.get("fullbelow_size", 0))
    )
    rippled_validations_cached.labels(node_type=NODE_TYPE).set(
        int(result.get("validations_cached", 0))
    )
    rippled_cache_al_size.labels(node_type=NODE_TYPE).set(
        int(result.get("AL_size", 0))
    )
    rippled_db_read_threads_running.labels(node_type=NODE_TYPE).set(
        int(result.get("read_threads_running", 0))
    )
    rippled_db_read_threads_total.labels(node_type=NODE_TYPE).set(
        int(result.get("read_threads_total", 0))
    )
    # node_reads_duration_us is microseconds — convert to seconds for Prometheus convention
    rippled_db_node_reads_duration_seconds.labels(node_type=NODE_TYPE).set(
        int(result.get("node_reads_duration_us", 0)) / 1_000_000
    )

    logger.info(
        "get_counts %s ledger_hit=%.1f%% node_read_hit=%.1f%% read_queue=%s write_load=%s",
        NODE_TYPE,
        float(result.get("ledger_hit_rate", 0)),
        node_read_hit_rate,
        result.get("read_queue"),
        result.get("write_load"),
    )


def update_consensus_metrics() -> None:
    data = query_rpc("consensus_info")
    result = data["result"]
    info = result.get("info")
    if not isinstance(info, dict):
        raise ValueError(f"consensus_info missing info object: {result!r}")

    rippled_consensus_proposing.labels(node_type=NODE_TYPE).set(
        1 if info.get("proposing") else 0
    )
    rippled_consensus_synched.labels(node_type=NODE_TYPE).set(
        1 if info.get("synched") else 0
    )
    rippled_consensus_validating.labels(node_type=NODE_TYPE).set(
        1 if info.get("validating") else 0
    )
    rippled_consensus_disputes.labels(node_type=NODE_TYPE).set(
        len(info.get("disputes", {}))
    )

    phase: str = str(info.get("phase", "unknown"))
    rippled_consensus_phase.labels(node_type=NODE_TYPE).set(
        CONSENSUS_PHASE_VALUES.get(phase, -1)
    )

    logger.info(
        "consensus_info %s proposing=%s synched=%s validating=%s disputes=%s phase=%s",
        NODE_TYPE,
        info.get("proposing"),
        info.get("synched"),
        info.get("validating"),
        len(info.get("disputes", {})),
        phase,
    )


def update_validator_info_metrics() -> None:
    try:
        data = query_rpc("validator_info")
    except RpcError as exc:
        if exc.error_code == 31:
            # error_code 31 = "not a validator" — permanent on non-validator nodes,
            # not a scrape failure; swallow it silently so the peer container stays quiet.
            rippled_validator_manifest_seq.labels(node_type=NODE_TYPE).set(0)
            logger.debug("validator_info skipped for %s: node is not a validator", NODE_TYPE)
            return
        raise

    result: dict[str, Any] = data["result"]
    if "seq" in result:
        rippled_validator_manifest_seq.labels(node_type=NODE_TYPE).set(int(result["seq"]))
    else:
        rippled_validator_manifest_seq.labels(node_type=NODE_TYPE).set(0)

    logger.info(
        "validator_info %s manifest_seq=%s",
        NODE_TYPE,
        result.get("seq"),
    )


def update_server_state_metrics() -> None:
    data = query_rpc("server_state")
    result = data["result"]
    state = result.get("state")
    if not isinstance(state, dict):
        raise ValueError(f"server_state missing state object: {result!r}")

    load_base: int = int(state.get("load_base", 256))
    load_factor_server_raw: int = int(state.get("load_factor_server", load_base))
    rippled_load_factor_server.labels(node_type=NODE_TYPE).set(
        load_factor_server_raw / load_base if load_base > 0 else 1.0
    )

    rippled_load_base.labels(node_type=NODE_TYPE).set(load_base)

    load_factor_fee_escalation_raw: int = int(state.get("load_factor_fee_escalation", load_base))
    rippled_load_factor_fee_escalation.labels(node_type=NODE_TYPE).set(
        load_factor_fee_escalation_raw / load_base if load_base > 0 else 1.0
    )

    load_factor_fee_queue_raw: int = int(state.get("load_factor_fee_queue", load_base))
    rippled_load_factor_fee_queue.labels(node_type=NODE_TYPE).set(
        load_factor_fee_queue_raw / load_base if load_base > 0 else 1.0
    )

    state_duration_us: int = int(state.get("server_state_duration_us", 0))
    rippled_server_state_duration_seconds.labels(node_type=NODE_TYPE).set(
        state_duration_us / 1_000_000
    )

    # closed_ledger is the most recently closed ledger that has NOT yet been validated.
    # On a healthy proposing/validating node it is frequently absent because the ledger
    # is validated almost immediately after closing — in that case the effective gap is 0.
    # Fall back to validated_ledger so the sequence reads correctly (gap = 0) rather than 0.
    closed_ledger: dict[str, Any] = state.get("closed_ledger", {})
    validated_ledger_state: dict[str, Any] = state.get("validated_ledger", {})
    closed_ledger_source: dict[str, Any] = closed_ledger if closed_ledger else validated_ledger_state

    if closed_ledger_source:
        rippled_closed_ledger_sequence.labels(node_type=NODE_TYPE).set(
            int(closed_ledger_source.get("seq", 0))
        )
        rippled_closed_ledger_age_seconds.labels(node_type=NODE_TYPE).set(
            int(closed_ledger_source.get("age", 0))
        )
    else:
        rippled_closed_ledger_sequence.labels(node_type=NODE_TYPE).set(0)
        rippled_closed_ledger_age_seconds.labels(node_type=NODE_TYPE).set(0)

    # Reserves come from validated_ledger — that is the authoritative source for
    # currently enforced reserve values, regardless of whether closed_ledger is present.
    if validated_ledger_state:
        rippled_reserve_base_drops.labels(node_type=NODE_TYPE).set(
            int(validated_ledger_state.get("reserve_base", 0))
        )
        rippled_reserve_inc_drops.labels(node_type=NODE_TYPE).set(
            int(validated_ledger_state.get("reserve_inc", 0))
        )
    else:
        rippled_reserve_base_drops.labels(node_type=NODE_TYPE).set(0)
        rippled_reserve_inc_drops.labels(node_type=NODE_TYPE).set(0)

    state_accounting: dict[str, Any] = state.get("state_accounting", {})
    for state_name, accounting in state_accounting.items():
        duration_us: int = int(accounting.get("duration_us", 0))
        transitions: int = int(accounting.get("transitions", 0))
        rippled_state_accounting_duration_seconds.labels(
            node_type=NODE_TYPE, state=state_name
        ).set(duration_us / 1_000_000)
        rippled_state_accounting_transitions.labels(
            node_type=NODE_TYPE, state=state_name
        ).set(transitions)

    logger.info(
        "server_state %s load_factor_server=%.3f closed_seq=%s reserve_base=%s",
        NODE_TYPE,
        load_factor_server_raw / load_base if load_base > 0 else 1.0,
        closed_ledger_source.get("seq", "?") if closed_ledger_source else "?",
        validated_ledger_state.get("reserve_base", "?") if validated_ledger_state else "?",
    )


def update_validator_list_sites_metrics() -> None:
    global _validator_site_labels_last

    data = query_rpc("validator_list_sites")
    result = data["result"]
    sites = result.get("validator_sites", [])
    if not isinstance(sites, list):
        raise ValueError(f"validator_list_sites missing validator_sites list: {result!r}")

    current_uris: set[str] = set()

    for site in sites:
        uri: str = str(site.get("uri", "unknown"))
        current_uris.add(uri)

        status: str = str(site.get("last_refresh_status", ""))
        # "accepted" = new VL sequence fetched; "same_sequence" = fetched but no update yet — both healthy
        rippled_validator_list_site_status.labels(
            node_type=NODE_TYPE, uri=uri
        ).set(1 if status in ("accepted", "same_sequence") else 0)

        last_refresh: str = str(site.get("last_refresh_time", ""))
        if last_refresh:
            try:
                rippled_validator_list_site_last_refresh_timestamp_seconds.labels(
                    node_type=NODE_TYPE, uri=uri
                ).set(parse_rippled_timestamp(last_refresh))
            except ValueError as exc:
                logger.warning(
                    "Could not parse validator_list_sites refresh time %r: %s",
                    last_refresh,
                    exc,
                )
                rippled_validator_list_site_last_refresh_timestamp_seconds.labels(
                    node_type=NODE_TYPE, uri=uri
                ).set(0)
        else:
            rippled_validator_list_site_last_refresh_timestamp_seconds.labels(
                node_type=NODE_TYPE, uri=uri
            ).set(0)

    # Zero out any URIs that were present last scrape but have now disappeared
    for stale_uri in _validator_site_labels_last - current_uris:
        rippled_validator_list_site_status.labels(
            node_type=NODE_TYPE, uri=stale_uri
        ).set(0)
        rippled_validator_list_site_last_refresh_timestamp_seconds.labels(
            node_type=NODE_TYPE, uri=stale_uri
        ).set(0)

    _validator_site_labels_last = current_uris

    logger.info(
        "validator_list_sites %s sites=%d",
        NODE_TYPE,
        len(sites),
    )


def update_feature_metrics() -> None:
    data = query_rpc("feature")
    result = data["result"]
    features: dict[str, Any] = result.get("features", {})

    enabled_count: int = 0
    pending_count: int = 0
    near_threshold_count: int = 0

    for _amendment_hash, details in features.items():
        if details.get("enabled", False):
            enabled_count += 1
        else:
            count: int = int(details.get("count", 0))
            threshold: int = int(details.get("threshold", 0))
            if threshold > 0:
                vote_ratio: float = count / threshold
                if vote_ratio >= 1.0:
                    # Reached voting threshold — in 2-week activation window
                    pending_count += 1
                elif vote_ratio >= 0.75:
                    near_threshold_count += 1

    rippled_amendments_enabled_total.labels(node_type=NODE_TYPE).set(enabled_count)
    rippled_amendments_pending_total.labels(node_type=NODE_TYPE).set(pending_count)
    rippled_amendments_near_threshold_total.labels(node_type=NODE_TYPE).set(
        near_threshold_count
    )

    logger.info(
        "feature %s enabled=%d pending=%d near_threshold=%d",
        NODE_TYPE,
        enabled_count,
        pending_count,
        near_threshold_count,
    )


def update_job_queue_metrics() -> None:
    global _job_queue_labels_last

    data = query_rpc("job_queue")
    result = data["result"]
    job_types = result.get("job_types", [])
    if not isinstance(job_types, list):
        raise ValueError(f"job_queue missing job_types list: {result!r}")

    current_job_types: set[str] = set()

    for job in job_types:
        job_type: str = str(job.get("job_type", "unknown"))
        rippled_job_queue_in_progress.labels(
            node_type=NODE_TYPE, job_type=job_type
        ).set(int(job.get("in_progress", 0)))
        rippled_job_queue_deferred.labels(
            node_type=NODE_TYPE, job_type=job_type
        ).set(int(job.get("deferred", 0)))
        current_job_types.add(job_type)

    # Zero out job types that no longer appear in the response
    for stale_job_type in _job_queue_labels_last - current_job_types:
        rippled_job_queue_in_progress.labels(
            node_type=NODE_TYPE, job_type=stale_job_type
        ).set(0)
        rippled_job_queue_deferred.labels(
            node_type=NODE_TYPE, job_type=stale_job_type
        ).set(0)

    _job_queue_labels_last = current_job_types

    total_deferred: int = sum(int(j.get("deferred", 0)) for j in job_types)
    logger.info(
        "job_queue %s types=%d total_deferred=%d",
        NODE_TYPE,
        len(job_types),
        total_deferred,
    )


def update_validators_metrics() -> None:
    data = query_rpc("validators")
    result: dict[str, Any] = data["result"]

    trusted_keys: list[str] = result.get("trusted_validator_keys", [])
    rippled_unl_size.labels(node_type=NODE_TYPE).set(len(trusted_keys))

    logger.info(
        "validators %s unl_size=%d",
        NODE_TYPE,
        len(trusted_keys),
    )


def update_fetch_info_metrics() -> None:
    data = query_rpc("fetch_info")
    result: dict[str, Any] = data["result"]
    info: dict[str, Any] = result.get("info", {})
    if not isinstance(info, dict):
        raise ValueError(f"fetch_info missing info object: {result!r}")

    active_count: int = len(info)
    incomplete_count: int = sum(
        1 for entry in info.values() if not entry.get("complete", False)
    )
    total_timeouts: int = sum(int(entry.get("timeouts", 0)) for entry in info.values())

    rippled_fetch_active_total.labels(node_type=NODE_TYPE).set(active_count)
    rippled_fetch_incomplete_total.labels(node_type=NODE_TYPE).set(incomplete_count)
    rippled_fetch_timeouts_total.labels(node_type=NODE_TYPE).set(total_timeouts)

    logger.info(
        "fetch_info %s active=%d incomplete=%d timeouts=%d",
        NODE_TYPE,
        active_count,
        incomplete_count,
        total_timeouts,
    )


def scrape_endpoint(endpoint: str, fn, required: bool = True) -> bool:
    try:
        fn()
        set_endpoint_success(endpoint, 1)
        return True
    except (requests.exceptions.RequestException, ValueError, RpcError) as exc:
        logger.error(
            "Failed %s scrape for %s at %s: %s",
            endpoint,
            NODE_TYPE,
            RIPPLED_URL,
            exc,
        )
        set_endpoint_success(endpoint, 0)
        return not required
    except Exception:
        # Unexpected errors (bugs, prometheus_client internals, etc.) are re-raised
        # so the main loop surfaces them with a full traceback rather than silently
        # treating them as transient network failures.
        logger.exception(
            "Unexpected error in %s scrape for %s — re-raising",
            endpoint,
            NODE_TYPE,
        )
        raise


def update_all_metrics() -> bool:
    start_ts = time.time()
    core_ok = True

    core_ok &= scrape_endpoint("server_info",    update_server_info_metrics,   required=True)
    core_ok &= scrape_endpoint("server_state",   update_server_state_metrics,  required=True)
    core_ok &= scrape_endpoint("fee",            update_fee_metrics,           required=True)
    core_ok &= scrape_endpoint("peers",          update_peer_metrics,          required=True)
    core_ok &= scrape_endpoint("get_counts",     update_counts_metrics,        required=True)
    core_ok &= scrape_endpoint("consensus_info", update_consensus_metrics,     required=True)
    core_ok &= scrape_endpoint("validators",     update_validators_metrics,    required=True)

    scrape_endpoint("validator_info",       update_validator_info_metrics,       required=False)
    scrape_endpoint("validator_list_sites", update_validator_list_sites_metrics, required=False)
    scrape_endpoint("feature",              update_feature_metrics,              required=False)
    scrape_endpoint("fetch_info",           update_fetch_info_metrics,           required=False)

    end_ts = time.time()
    rippled_scrape_success.labels(node_type=NODE_TYPE).set(1 if core_ok else 0)
    rippled_scrape_duration_seconds.labels(node_type=NODE_TYPE).set(end_ts - start_ts)

    if core_ok:
        rippled_last_scrape_success_timestamp_seconds.labels(node_type=NODE_TYPE).set(
            end_ts
        )

    return core_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="rippled Prometheus exporter")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.parse_args()

    logger.info(
        "Starting rippled exporter v%s for node_type=%s on port=%d scraping=%s interval=%ds",
        __version__,
        NODE_TYPE,
        METRICS_PORT,
        RIPPLED_URL,
        SCRAPE_INTERVAL,
    )
    try:
        start_http_server(METRICS_PORT)
    except OSError as exc:
        logger.error(
            "Failed to start metrics server on port %d: %s — "
            "check that the port is not already in use and the process has permission",
            METRICS_PORT,
            exc,
        )
        raise SystemExit(1) from exc

    max_consecutive_failures: int = _parse_env_int("MAX_CONSECUTIVE_FAILURES", 60)
    consecutive_failures: int = 0

    while True:
        loop_start = time.time()
        scrape_ok = update_all_metrics()

        if scrape_ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    "%d consecutive scrape failures for %s — exiting so Docker can restart "
                    "and reattach to the rippled container network namespace",
                    consecutive_failures,
                    NODE_TYPE,
                )
                raise SystemExit(1)

        elapsed = time.time() - loop_start
        time.sleep(max(0, SCRAPE_INTERVAL - elapsed))


if __name__ == "__main__":
    main()
