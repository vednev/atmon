# atmon

atmon is a CLI tool that scans MongoDB Atlas clusters for performance issues and prints a consolidated report to the console. It combines data from [Performance Advisor](https://www.mongodb.com/docs/atlas/performance-advisor/) and direct pymongo queries to give a single view of index health, slow queries, collection scans, schema anti-patterns, and query execution statistics.

## Usage

```
python app.py <project> [cluster] [db] [collection] [options]
```

**Positional arguments:**
- `project` — Key in [`ATLAS_CONFIGS`](https://github.com/vednev/atmon/blob/main/app.py#L20) (e.g., `PROJECT0`)
- `cluster` — (Optional) Atlas cluster name (e.g., `Cluster0`). Omit to scan all clusters in the project.
- `db` — (Optional) Database name. Omit to scan all databases.
- `collection` — (Optional) Collection name (requires `db`). Omit to scan all collections under `db`.

**Options:**
- `--slow-threshold-ms N` — Minimum execution time in milliseconds to qualify as slow (default: 1000)
- `--skip-query-stats` — Skip the [`$queryStats`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/querystats/) aggregation (useful if the connection is slow or the user lacks permissions)
- `--debug` — Enable debug logging (shows HTTP request traces, `$queryStats` filter details, and namespace resolution)

## Scoping

atmon supports four levels of scope. At every level, pymongo connects to the cluster and enumerates the in-scope databases and collections, running per-collection diagnostics for each.

| Arguments | Scope |
|-----------|-------|
| `project` | All clusters in the project, all databases, all collections |
| `project cluster` | All databases and collections in the cluster |
| `project cluster db` | All collections under `db` |
| `project cluster db collection` | Single collection |

## Data Sources

### Atlas Admin API

All Atlas API calls use [Performance Advisor v2 endpoints](https://www.mongodb.com/docs/atlas/performance-advisor/) with HTTP Digest Auth using [Atlas programmatic API keys](https://www.mongodb.com/docs/atlas/configure-api-access/).

| Report Section | What it shows |
|----------------|---------------|
| **Suggested Indexes** | Indexes Atlas recommends creating, with the distinct query shapes that drove each suggestion. Fetched per-namespace (required by the API) using the slow query log to identify namespaces with query history, then merged. |
| **Drop Index Suggestions** | Indexes that are redundant or unused and can be safely dropped. |
| **Schema Anti-Patterns** | Schema design issues (unbounded arrays, massive documents, etc.) with affected namespaces and trigger descriptions. |
| **Slow Namespaces** | Namespaces flagged by Atlas as having slow operations, filtered to those with at least one query meeting the slow threshold. |
| **COLLSCAN Queries** | Queries where MongoDB scanned every document instead of using an index. Always shown regardless of threshold. |

### pymongo

A direct connection is made to the cluster using [X.509 certificate auth](https://www.mongodb.com/docs/manual/core/security-x.509/) over an SRV connection string. The X.509 user must have the [`clusterMonitor`](https://www.mongodb.com/docs/manual/reference/built-in-roles/) role to run `$queryStats` and `$currentOp`.

pymongo is also used to enumerate databases and collections at runtime, so the in-scope namespace list always reflects what actually exists in the cluster.

| Report Section | Aggregation | What it shows |
|----------------|-------------|---------------|
| **Collection Stats** | [`$collStats`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/collstats/) | Document count, storage size, total index size, and per-index sizes for every in-scope collection. |
| **Index Access Counts** | [`$indexStats`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexstats/) | Operations per index since creation. Unused indexes (0 ops) are flagged. |
| **In-Progress Index Builds** | [`$currentOp`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/currentop/) | Any index builds currently running, with host, namespace, duration, and progress. |
| **Query Statistics** | [`$queryStats`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/querystats/) | Query shapes tracked by the cluster, filtered by the slow threshold (converted to microseconds). Shows execution count and avg/min/max latency per shape. Internal commands are excluded. |

## Threshold Filtering

The `--slow-threshold-ms` parameter (default: 1000ms) controls what counts as "slow":

- **Slow namespaces**: Only namespaces with at least one query at or above the threshold are shown.
- **$queryStats**: Filters by `totalExecMicros.max > threshold * 1000` OR computed avg (`totalExecMicros.sum / execCount`) `> threshold * 1000`. This catches both consistently slow shapes and occasional outliers.
- **COLLSCAN queries**: Always shown regardless of threshold — a collection scan is a problem at any speed.

## Configuration

`ATLAS_CONFIGS` in `app.py` is a dictionary keyed by project name. Each entry requires:

| Key | Description |
|-----|-------------|
| `project_id` | Atlas project (group) ID |
| `public_api_key` | [Atlas programmatic API](https://www.mongodb.com/docs/atlas/configure-api-access/) public key |
| `private_api_key_secret` | [Atlas programmatic API](https://www.mongodb.com/docs/atlas/configure-api-access/) private key |
| `admin_db_user_secret` | Path to the [X.509 PEM certificate](https://www.mongodb.com/docs/manual/core/security-x.509/) file |

SRV hostnames are resolved from the Atlas API at runtime via `list_clusters`, so no per-cluster configuration is needed. To add a project, add another entry to `ATLAS_CONFIGS` with the same key structure.

## Dependencies

```
httpx>=0.27.0       # HTTP client with DigestAuth for Atlas API
pymongo>=4.6.0      # Direct cluster connection for live diagnostics
certifi>=2024.0.0   # TLS CA bundle for X.509 connections
```

## Setup

- Create the Python virtual environment
- Install dependencies in requirements.txt using `pip`
- Fill in required parameters in [`ATLAS_CONFIGS`](https://github.com/vednev/atmon/blob/main/app.py#L20) / rework credentials passing using another method of choice
- `python app.py --help` for detailed usage information
