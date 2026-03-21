# xrpl-observability

Prometheus exporter and Grafana dashboards for monitoring [rippled](https://github.com/XRPLF/rippled) nodes on the XRP Ledger network.

Designed to monitor one or more rippled nodes (validator and/or peer) from a single exporter deployment, with full visibility into consensus health, network connectivity, fees, and storage performance.

---

## Architecture

```
rippled (validator)  ──┐
                       ├──  rippled-exporter  ──►  Prometheus  ──►  Grafana
rippled (peer)       ──┘         (x2)
```

One exporter instance runs per rippled node. Each exporter shares its target node's Docker network namespace (`network_mode: container:`), so it connects directly to `127.0.0.1:5005` inside that namespace without any port mapping. rippled's admin port is never exposed externally.

---

## Components

| Path | Description |
|------|-------------|
| `exporter/exporter.py` | Python Prometheus exporter — scrapes rippled JSON-RPC endpoints |
| `exporter/compose.yaml` | Docker Compose for both exporter instances |
| `exporter/Dockerfile` | Container image definition |
| `dashboards/rippled-overview.json` | Grafana dashboard — health at a glance (21 panels) |
| `dashboards/rippled-deep-dive.json` | Grafana dashboard — detailed analysis (34 panels) |
| `prometheus/scrape_configs.yaml` | Prometheus scrape config snippet for both exporters |

---

## Exporter

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RIPPLED_URL` | `http://127.0.0.1:5005` | rippled JSON-RPC endpoint |
| `NODE_TYPE` | `peer` | Label applied to all metrics (`peer` or `validator`) |
| `SCRAPE_INTERVAL` | `5` | Seconds between scrapes |
| `METRICS_PORT` | `9999` | Port to expose Prometheus metrics on |
| `MAX_CONSECUTIVE_FAILURES` | `60` | Exit after this many consecutive full-scrape failures so Docker can restart and reconnect when rippled is unavailable |

### Metrics Exposed

| Metric | Type | Description |
|--------|------|-------------|
| `rippled_server_state` | Gauge | Server state as numeric (0=disconnected … 6=proposing) |
| `rippled_server_state_info` | Gauge | Server state as label, value is 1 when active |
| `rippled_peers_total` | Gauge | Connected peer count |
| `rippled_peer_disconnects_total` | Counter | Peer disconnects since startup (use `rate()`) |
| `rippled_peer_disconnects_resources_total` | Counter | Peer disconnects due to resource limits |
| `rippled_peer_latency_avg_ms` | Gauge | Trimmed mean peer latency (top 5% dropped) |
| `rippled_peer_latency_min_ms` | Gauge | Minimum peer latency |
| `rippled_peer_latency_max_ms` | Gauge | Maximum peer latency |
| `rippled_peer_inbound_total` | Gauge | Inbound peer connections |
| `rippled_peer_outbound_total` | Gauge | Outbound peer connections |
| `rippled_peer_version_total` | Gauge | Connected peers per rippled version |
| `rippled_ledger_sequence` | Gauge | Current validated ledger sequence |
| `rippled_ledger_age_seconds` | Gauge | Age of last validated ledger |
| `rippled_complete_ledgers_low` | Gauge | Lowest sequence in the node's complete ledger range (0 if empty) |
| `rippled_complete_ledgers_high` | Gauge | Highest sequence in the node's complete ledger range (0 if empty). Use `high - low` in Grafana to graph range size — climbs steadily and drops sharply at each NuDB rotation cycle. |
| `rippled_load_factor` | Gauge | Load multiplier (1 = no load) |
| `rippled_uptime_seconds` | Gauge | Node uptime |
| `rippled_io_latency_ms` | Gauge | I/O latency reported by rippled |
| `rippled_validation_quorum` | Gauge | Minimum trusted validations required |
| `rippled_last_close_converge_time_seconds` | Gauge | Last ledger close convergence time |
| `rippled_last_close_proposers` | Gauge | Proposers on last closed ledger |
| `rippled_transaction_overflow` | Gauge | Transaction queue overflow count since startup |
| `rippled_validator_list_count` | Gauge | Number of validator lists loaded |
| `rippled_validator_list_active` | Gauge | 1 if validator list status is active |
| `rippled_validator_list_expiry_timestamp` | Gauge | Unix timestamp when validator list expires |
| `rippled_load_threads` | Gauge | Job scheduler thread count |
| `rippled_build_info` | Gauge | Always 1; version label carries the build version string |
| `rippled_fee_base_drops` | Gauge | Base fee in drops |
| `rippled_fee_median_drops` | Gauge | Median fee in drops |
| `rippled_fee_open_ledger_drops` | Gauge | Open ledger fee in drops |
| `rippled_fee_minimum_drops` | Gauge | Minimum accepted fee in drops |
| `rippled_ledger_current_tx_count` | Gauge | Transactions in current open ledger |
| `rippled_ledger_queue_tx_count` | Gauge | Transactions in queue |
| `rippled_ledger_queue_tx_max` | Gauge | Max queue capacity |
| `rippled_ledger_expected_tx_count` | Gauge | Expected transactions per ledger |
| `rippled_cache_ledger_hit_rate` | Gauge | Ledger cache hit rate (%) |
| `rippled_cache_node_read_hit_rate` | Gauge | Node read cache hit rate (%) |
| `rippled_db_read_queue` | Gauge | Pending DB read requests |
| `rippled_db_write_load` | Gauge | DB write load |
| `rippled_consensus_proposing` | Gauge | 1 if node is proposing |
| `rippled_consensus_synched` | Gauge | 1 if node is synched |
| `rippled_consensus_validating` | Gauge | 1 if node is sending validations |
| `rippled_consensus_disputes` | Gauge | Disputed transactions in current round |
| `rippled_validator_manifest_seq` | Gauge | Validator manifest sequence number |
| `rippled_amendment_blocked` | Gauge | 1 if node is amendment blocked and cannot process newer features |
| `rippled_load_factor_server` | Gauge | Server's own local load factor (1 = no load); compare to `rippled_load_factor` to distinguish local vs network load |
| `rippled_closed_ledger_sequence` | Gauge | Most recently closed ledger sequence (may not yet be validated) |
| `rippled_closed_ledger_age_seconds` | Gauge | Age of the most recently closed ledger in seconds |
| `rippled_reserve_base_drops` | Gauge | Base account reserve in drops of XRP |
| `rippled_reserve_inc_drops` | Gauge | Owner reserve increment per object in drops of XRP |
| `rippled_state_accounting_duration_seconds` | Gauge | Cumulative seconds spent in each server state since startup (`state` label) |
| `rippled_state_accounting_transitions` | Gauge | Number of transitions into each server state since startup (`state` label) |
| `rippled_validator_list_site_status` | Gauge | 1 if last fetch from this VL site was accepted (`uri` label) |
| `rippled_validator_list_site_last_refresh_timestamp_seconds` | Gauge | Unix timestamp of last successful VL site refresh (`uri` label) |
| `rippled_load_base` | Gauge | Base load normalization value (typically 256) |
| `rippled_load_factor_fee_escalation` | Gauge | Fee escalation component of load factor; > 1 when fees are being inflated |
| `rippled_load_factor_fee_queue` | Gauge | Fee queue component of load factor; > 1 when queue pressure is affecting fees |
| `rippled_server_state_duration_seconds` | Gauge | How long the node has been in its current server state |
| `rippled_network_id` | Gauge | Network ID (0 = XRPL mainnet) |
| `rippled_node_size` | Gauge | Configured node size as numeric (0=tiny … 4=huge) |
| `rippled_db_node_writes_total` | Counter | Total write operations to the NuDB/RocksDB store since startup (use `rate()`) |
| `rippled_db_node_reads_total` | Counter | Total read operations from the NuDB/RocksDB store since startup (use `rate()`) |
| `rippled_db_size_kb` | Gauge | Database file size in KB (`database` label: ledger, transaction, total) |
| `rippled_cache_treenode_size` | Gauge | Objects in the SHAMap tree node cache |
| `rippled_cache_fullbelow_size` | Gauge | Entries in the full-below cache |
| `rippled_validations_cached` | Gauge | Validator signatures currently held in the validation cache |
| `rippled_peer_non_sane_total` | Gauge | Connected peers with non-sane status; should always be 0 |
| `rippled_peer_messages` | Gauge | Total protocol messages accumulated across all currently connected peers |
| `rippled_consensus_phase` | Gauge | Current consensus phase (0=open 1=establish 2=accepted) |
| `rippled_amendments_enabled_total` | Gauge | Total amendments currently enabled on this network |
| `rippled_amendments_pending_total` | Gauge | Amendments at the voting threshold pending activation |
| `rippled_amendments_near_threshold_total` | Gauge | Amendments with >= 75% of required votes |
| `rippled_unl_size` | Gauge | Number of trusted validators in the UNL |
| `rippled_fetch_active_total` | Gauge | Ledgers currently tracked in the fetch queue |
| `rippled_fetch_incomplete_total` | Gauge | Ledgers in the fetch queue not yet fully acquired |
| `rippled_fetch_timeouts_total` | Gauge | Sum of fetch timeouts across all active ledger acquisitions |
| `rippled_scrape_success` | Gauge | 1 if last full scrape succeeded |
| `rippled_endpoint_scrape_success` | Gauge | Per-endpoint scrape health |
| `rippled_scrape_duration_seconds` | Gauge | Time to complete one full scrape cycle |
| `rippled_last_scrape_success_timestamp_seconds` | Gauge | Unix timestamp of last successful scrape |

### Deployment

The exporter shares rippled's network namespace via `network_mode: container:`. This means `127.0.0.1:5005` is directly reachable with no port mapping or rippled.cfg changes required.

**After restarting or recreating a rippled container**, restart the corresponding exporter so it reattaches to the new network namespace:

```bash
docker restart rippled-exporter-peer
docker restart rippled-exporter-validator
```

**Step 1 — Add the exporter metrics port to your rippled compose services.**

The exporter shares rippled's network namespace (`network_mode: container:`). Metrics are served from within that shared namespace, so rippled's compose file must publish the port so Prometheus can scrape it:

```yaml
# rippled-peer compose service:
ports:
  - "9998:9998"   # exporter metrics

# rippled-validator compose service:
ports:
  - "9999:9999"   # exporter metrics
```

**Step 2 — Start the exporters.**

The image is available on Docker Hub for `linux/amd64` and `linux/arm64`:

```
islandsound/rippled-exporter:latest
```

```bash
cd exporter/
docker compose up -d
```

Or build locally:

```bash
docker compose up -d --build
```

---

## Grafana Dashboards

Both dashboards use a `${PROMETHEUS}` datasource placeholder and will prompt for datasource selection on import.

### Import

1. In Grafana: **Dashboards → Import**
2. Upload the JSON file from `dashboards/`
3. Select your Prometheus datasource when prompted

### rippled Overview (`rippled-overview.json`)

22 panels — health at a glance. Default time range: 6h / 30s refresh.

- Node state, peers, quorum, validator list, UNL expiry, manifest seq, build version
- Non-sane peers, Amendments Pending, UNL Size
- Consensus Phase state timeline (open / establish / accepted bands)
- Amendment Blocked alert (full-width; green OK / red AMENDMENT BLOCKED)
- Proposing / Synched / Validating status
- Server State History state timeline (colored bands by state), ledger age, peer count, convergence time, proposer count
- Uptime (validator + peer side by side)

### rippled Deep Dive (`rippled-deep-dive.json`)

34 panels — detailed analysis. Default time range: 6h / 30s refresh.

- Inbound vs outbound peers, peer latency (capped at 500ms), consensus disputes
- Network fees (base / median / open ledger), transaction queue, version spread (bar chart)
- Cache hit rates, DB queue & write load
- I/O latency, load factor (network + server + fee escalation + fee queue), load threads, transaction queue overflow
- Ledger History Range (range size — climbs steadily and drops at each NuDB rotation)
- Ledger Fetch Activity (active/incomplete fetches, cumulative timeouts)
- Closed vs Validated Ledger Gap, Reserves (base + owner increment in XRP)
- State Accounting duration and transitions (cumulative time and count per server state)
- Validator List Site Health (per-site fetch status as status-history grid)
- Server State Duration, Non-sane Peers
- Database Sizes (ledger + transaction store KB), DB Read/Write Ops rate
- Job Queue (deferred + in-progress totals), UNL Size, Validations Cached
- Amendments Enabled / Pending / Near Threshold
- Peer disconnects/min, uptime, exporter health (endpoint scrape success, scrape duration, last success age)

### Template Variable

Both dashboards include a `node_type` multi-select variable populated from `label_values(rippled_server_state, node_type)`. Use it to filter panels to `validator`, `peer`, or both.

---

## Prometheus Configuration

Add the contents of `prometheus/scrape_configs.yaml` to your Prometheus configuration under the `scrape_configs:` key.

The rippled job is configured with a 3s scrape interval to match the ~3.5s ledger close time:

```yaml
- job_name: rippled
  scrape_interval: 3s
  scrape_timeout: 2s
  static_configs:
    - targets:
        - <host>:9998   # peer exporter
        - <host>:9999   # validator exporter
      labels:
        source: rippled
```

---

## Known Issues / Design Notes

- **Peer latency trimmed mean**: The exporter drops the top 5% of peer latency values before averaging to prevent dying peers (latencies 10,000ms+) from skewing the average. The raw max is still exposed as `rippled_peer_latency_max_ms`.
- **Peer disconnects counter**: rippled reports cumulative disconnects since its own startup, not since the exporter started. The exporter tracks deltas and detects restarts via uptime comparison to maintain a monotonically increasing Prometheus Counter.
- **Version spread**: Stale version labels are zeroed out (not removed) to avoid a `prometheus_client` internal error triggered by `Gauge.clear()` after label removal in some library versions.
- **validator_info on peer nodes**: rippled returns error code 31 ("not a validator") on peer nodes for the `validator_info` RPC. The exporter silences this at DEBUG level.
- **Same-host port conflict**: Both rippled instances expose admin port 5005 internally. When running peer and validator on the same host, bind them to different host ports (`5005` and `5006`) and set `RIPPLED_URL` accordingly in the exporter compose.
