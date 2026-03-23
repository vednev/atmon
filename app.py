import argparse
import sys
import logging
import json

import certifi
import pymongo
from pymongo import MongoClient


from atlas import AtlasClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


ATLAS_CONFIGS: dict[str, dict[str, str]] = {
    "PROJECT0": {
        "project_id": "",
        "public_api_key": "",
        "private_api_key_secret": "",
        "admin_db_user_secret": "",
    },
}


def _connect(cfg: dict, srv_host: str) -> MongoClient:
    """Create a pymongo MongoClient using X.509 cert auth."""
    uri = (
        f"mongodb+srv://{srv_host}/?"
        "authSource=%24external&authMechanism=MONGODB-X509"
        "&retryWrites=true&w=majority"
    )
    return MongoClient(
        uri,
        tls=True,
        tlsCertificateKeyFile=cfg["admin_db_user_secret"],
        tlsCAFile=certifi.where(),
    )


def run_collection_diagnostics(
    client: MongoClient, db_name: str, collection_name: str
) -> dict:
    """Run pymongo-level diagnostics on a collection."""
    coll = client[db_name][collection_name]
    results: dict[str, object] = {}

    # Collection stats
    try:
        stats = list(coll.aggregate([{"$collStats": {"storageStats": {}}}]))[0][
            "storageStats"
        ]
        results["coll_stats"] = {
            "ns": stats.get("ns"),
            "count": stats.get("count", 0),
            "storageSize": stats.get("storageSize", 0),
            "totalIndexSize": stats.get("totalIndexSize", 0),
            "indexSizes": stats.get("indexSizes", {}),
        }
    except Exception as e:
        results["coll_stats_error"] = str(e)

    # Index stats
    try:
        index_stats = list(coll.aggregate([{"$indexStats": {}}]))
        results["index_stats"] = [
            {
                "name": s["name"],
                "key": s["key"],
                "ops": s["accesses"]["ops"],
                "since": str(s["accesses"]["since"]),
            }
            for s in sorted(index_stats, key=lambda x: x["accesses"]["ops"])
        ]
    except Exception as e:
        results["index_stats_error"] = str(e)

    # Current indexes
    try:
        results["indexes"] = [
            {"name": idx["name"], "key": idx["key"]} for idx in coll.list_indexes()
        ]
    except Exception as e:
        results["indexes_error"] = str(e)

    # In-progress index builds
    try:
        ops = list(
            client.get_database("admin").aggregate(
                [
                    {"$currentOp": {"allUsers": True, "idleConnections": True}},
                    {"$match": {"command.createIndexes": {"$exists": True}}},
                ]
            )
        )
        results["index_builds"] = [
            {
                "host": op.get("host"),
                "ns": op.get("ns"),
                "secs_running": op.get("secs_running"),
                "msg": op.get("msg"),
                "indexes": op.get("command", {}).get("indexes", []),
            }
            for op in ops
        ]
    except Exception as e:
        results["index_builds_error"] = str(e)

    return results


def run_query_stats(
    client: MongoClient,
    slow_threshold_ms: int,
    db_name: str | None = None,
    collection_name: str | None = None,
) -> list[dict]:
    """Run $queryStats and return slow query shapes."""
    threshold_us = slow_threshold_ms * 1000
    ns_filter: dict = {}
    if db_name:
        ns_filter["key.queryShape.cmdNs.db"] = db_name
    if collection_name:
        ns_filter["key.queryShape.cmdNs.coll"] = collection_name

    logger.debug(f"$queryStats threshold: {slow_threshold_ms}ms ({threshold_us}us)")

    # First, check if $queryStats returns anything at all (unfiltered)
    try:
        raw_count = list(
            client.get_database("admin").aggregate(
                [
                    {"$queryStats": {}},
                    {"$count": "total"},
                ]
            )
        )
        total = raw_count[0]["total"] if raw_count else 0
        logger.debug(f"$queryStats total unfiltered shapes: {total}")
    except Exception as e:
        logger.debug(f"$queryStats count check failed: {e}")

    try:
        pipeline: list = [{"$queryStats": {}}]
        if ns_filter:
            pipeline.append({"$match": ns_filter})
        pipeline += [
            {
                "$addFields": {
                    "avg_us": {
                        "$cond": [
                            {"$gt": ["$metrics.execCount", 0]},
                            {
                                "$divide": [
                                    "$metrics.totalExecMicros.sum",
                                    "$metrics.execCount",
                                ]
                            },
                            0,
                        ]
                    }
                }
            },
            {
                "$match": {
                    "$or": [
                        {"metrics.totalExecMicros.max": {"$gt": threshold_us}},
                        {"avg_us": {"$gt": threshold_us}},
                    ]
                }
            },
            {
                "$project": {
                    "hash": {"$ifNull": ["$queryShapeHash", "unknown"]},
                    "shape": "$key.queryShape",
                    "sum_us": "$metrics.totalExecMicros.sum",
                    "count": "$metrics.execCount",
                    "min_us": "$metrics.totalExecMicros.min",
                    "max_us": "$metrics.totalExecMicros.max",
                    "avg_us": "$avg_us",
                    "timestamp": "$asOf",
                }
            },
        ]
        query_stats = list(client.get_database("admin").aggregate(pipeline))
        logger.debug(f"$queryStats matched shapes after filter: {len(query_stats)}")
    except Exception as e:
        logger.error(f"$queryStats error: {e}")
        return []

    results = []
    internal_skipped = 0
    for qs in query_stats:
        count = qs.get("count", 1) or 1
        sum_ms = qs.get("sum_us", 0) / 1000
        avg_ms = qs.get("avg_us", 0) / 1000
        max_ms = qs.get("max_us", 0) / 1000
        min_ms = qs.get("min_us", 0) / 1000

        # Exclude internal commands
        shape_str = str(qs.get("shape", {}))
        if any(
            cmd in shape_str
            for cmd in ("collStats", "queryStats", "indexStats", "listSearchIndexes")
        ):
            internal_skipped += 1
            continue

        results.append(
            {
                "hash": str(qs.get("hash", "unknown"))[:8],
                "count": count,
                "avg_ms": round(avg_ms, 1),
                "max_ms": round(max_ms, 1),
                "min_ms": round(min_ms, 1),
                "total_ms": round(sum_ms, 1),
                "shape": qs.get("shape"),
                "is_slow": max_ms > slow_threshold_ms or avg_ms > slow_threshold_ms,
            }
        )

    logger.debug(
        f"$queryStats results: {len(results)} kept, {internal_skipped} internal skipped"
    )
    results.sort(key=lambda x: x["max_ms"], reverse=True)
    return results


def _print_collection_diagnostics(ns: str, diag: dict) -> None:
    """Print pymongo diagnostics for a single collection."""
    print(f"  [ {ns} ]")

    if "coll_stats" in diag:
        cs = diag["coll_stats"]
        storage_mb = cs.get("storageSize", 0) / 1024**2
        idx_mb = cs.get("totalIndexSize", 0) / 1024**2
        print(f"    Documents      : {cs.get('count', 'N/A'):,}")
        print(f"    Storage        : {storage_mb:,.1f} MB")
        print(f"    Total Idx Size : {idx_mb:,.1f} MB")
        idx_sizes = cs.get("indexSizes", {})
        if idx_sizes:
            print("    Index Sizes:")
            for name, size in sorted(idx_sizes.items(), key=lambda x: -x[1]):
                print(f"      {size / 1024**2:8.1f} MB  {name}")
    elif "coll_stats_error" in diag:
        print(f"    (coll stats error: {diag['coll_stats_error']})")

    if "index_stats" in diag:
        stats = diag["index_stats"]
        print("    Index Access Counts:")
        for s in stats:
            ops = s["ops"]
            marker = "  *** UNUSED" if ops == 0 else ""
            print(f"      {ops:>10,} ops  {s['name']}  {s['key']}{marker}")
    elif "index_stats_error" in diag:
        print(f"    (index stats error: {diag['index_stats_error']})")

    if "index_builds" in diag:
        builds = diag["index_builds"]
        if builds:
            print("    In-Progress Index Builds:")
            for b in builds:
                print(f"      Host    : {b['host']}")
                print(f"      NS      : {b['ns']}")
                print(f"      Running : {b['secs_running']}s")
                print(f"      Progress: {b['msg']}")
                print(f"      Indexes : {b['indexes']}")
    print()


def print_report(
    project: str,
    cluster: str,
    db_name: str | None,
    collection_name: str | None,
    index_suggestions: dict,
    drop_suggestions: dict,
    schema_advice: dict,
    slow_namespaces: dict,
    slow_queries: list[dict],
    collscan_queries: list[dict],
    coll_diagnostics: dict[str, dict],
    query_stats: list[dict] | None,
    slow_threshold_ms: int = 1000,
) -> None:
    """Print a structured performance report."""
    print("=" * 70)
    print("MongoDB Performance Advisor Report")
    print("=" * 70)
    print(f"Project    : {project}")
    print(f"Cluster    : {cluster}")
    if collection_name:
        print(f"Scope      : {db_name}.{collection_name}")
    elif db_name:
        print(f"Scope      : all collections under {db_name}")
    else:
        print(f"Scope      : all namespaces")
    print()

    # Collection stats, index access counts, index builds — one block per collection
    if coll_diagnostics:
        print("--- Collection Stats & Index Diagnostics ---")
        for ns in sorted(coll_diagnostics):
            _print_collection_diagnostics(ns, coll_diagnostics[ns])
    else:
        print("--- Collection Stats & Index Diagnostics ---")
        print("  (no pymongo connection — skipped)")
        print()

    # Atlas: Suggested indexes
    suggestions = index_suggestions.get("suggestedIndexes", [])
    shapes_by_id = {
        s["id"]: s for s in index_suggestions.get("shapes", []) if "id" in s
    }
    print(f"--- Atlas Suggested Indexes ({len(suggestions)}) ---")
    if suggestions:
        for s in suggestions:
            ns = s.get("namespace", "")
            idx = s.get("index", [])
            # Format index as a mongo-style key doc: {field: direction, ...}
            idx_str = (
                "{"
                + ", ".join(f"{list(k.keys())[0]}: {list(k.values())[0]}" for k in idx)
                + "}"
                if idx
                else "[]"
            )
            print(f"  {ns}  {idx_str}")
            # Print distinct query predicates from impacted shapes
            seen_preds: set[str] = set()
            for shape_id in s.get("impact", []):
                shape = shapes_by_id.get(shape_id)
                if not shape:
                    continue
                for op in shape.get("operations", []):
                    for pred in op.get("predicates", []):
                        pred_str = json.dumps(pred, default=str)
                        if pred_str not in seen_preds:
                            seen_preds.add(pred_str)
                            print(f"    query: {pred_str}")
            print()
    else:
        print("  (none)")
    print()

    # Atlas: Drop suggestions
    drops = drop_suggestions.get("suggestedIndexes", []) or drop_suggestions.get(
        "results", []
    )
    print(f"--- Atlas Drop Index Suggestions ({len(drops)}) ---")
    if drops:
        for d in drops:
            print(f"  {json.dumps(d, indent=4)}")
    else:
        print("  (none)")
    print()

    # Atlas: Schema advice
    recs = schema_advice.get("content", {}).get("recommendations", [])
    print(f"--- Schema Anti-Patterns ({len(recs)}) ---")
    if recs:
        for r in recs:
            print(f"  {r.get('recommendation')}: {r.get('description')}")
            for ns in r.get("affectedNamespaces", []):
                print(f"    - {ns.get('namespace')}")
                for t in ns.get("triggers", []):
                    print(f"      {t.get('triggerType')}: {t.get('description')}")
            print()
    else:
        print("  (none)")
    print()

    # Atlas: Slow namespaces (filtered to those with queries >= threshold)
    ns_list = slow_namespaces.get("namespaces", [])
    slow_ns_set = {
        q.get("namespace")
        for q in slow_queries
        if q.get("millis", 0) >= slow_threshold_ms
    }
    filtered_ns = [
        ns
        for ns in ns_list
        if (ns.get("namespace") if isinstance(ns, dict) else ns) in slow_ns_set
    ]
    print(f"--- Slow Namespaces ({len(filtered_ns)}, >= {slow_threshold_ms}ms) ---")
    if filtered_ns:
        for ns in filtered_ns:
            print(f"  {ns}")
    else:
        print("  (none)")
    print()

    # Atlas: Slow queries (filtered by threshold)
    # filtered_slow = [q for q in slow_queries if q.get("millis", 0) >= slow_threshold_ms]
    # print(f"--- Slow Queries ({len(filtered_slow)}, >= {slow_threshold_ms}ms) ---")
    # if filtered_slow:
    #    for q in filtered_slow[:50]:
    #        ns = q.get("namespace", "unknown")
    #        millis = q.get("millis", "?")
    #        plan = q.get("planSummary", "")
    #        docs = q.get("docsExamined", 0)
    #        returned = q.get("nreturned", 0)
    #        print(f"  [{millis}ms] {ns}  {plan}  (docs={docs}, returned={returned})")
    # else:
    #    print("  (none)")
    # print()

    # Atlas: COLLSCAN queries (always shown regardless of threshold)
    print(f"--- COLLSCAN Queries ({len(collscan_queries)}) ---")
    if collscan_queries:
        for q in collscan_queries:
            ns = q.get("namespace", "unknown")
            millis = q.get("millis", "?")
            docs = q.get("docsExamined", 0)
            returned = q.get("nreturned", 0)
            query = q.get("query", {})
            print(f"  [{millis}ms] {ns}  (docs={docs}, returned={returned})")
            print(f"    query: {json.dumps(query, default=str)[:200]}")
    else:
        print("  (none)")
    print()

    # Query stats
    if query_stats is not None:
        slow = [q for q in query_stats if q["is_slow"]]
        print(f"--- Query Statistics ({len(query_stats)} shapes, {len(slow)} slow) ---")
        if slow:
            print("  Slow queries (above threshold):")
            for q in slow[:20]:
                shape = q.get("shape", {})
                cmd_ns = shape.get("cmdNs", {})
                ns_info = f"  {cmd_ns.get('db', '?')}.{cmd_ns.get('coll', '?')}"
                print(
                    f"    #{q['hash']}{ns_info}  count={q['count']}  avg={q['avg_ms']}ms  max={q['max_ms']}ms"
                )
                print(f"      shape: {json.dumps(shape, default=str)[:200]}")
                print()
        elif query_stats:
            print("  No queries above slow threshold. Top 5 by max latency:")
            for q in query_stats[:5]:
                shape = q.get("shape", {})
                cmd_ns = shape.get("cmdNs", {})
                ns_info = f"  {cmd_ns.get('db', '?')}.{cmd_ns.get('coll', '?')}"
                print(
                    f"    #{q['hash']}{ns_info}  count={q['count']}  avg={q['avg_ms']}ms  max={q['max_ms']}ms"
                )
        else:
            print("  No query stats available.")
        print()

    print("=" * 70)
    print("End of report")


def _list_namespaces_from_mongo(
    mongo_client: MongoClient,
    db_name: str | None,
    collection_name: str | None,
) -> list[tuple[str, str]]:
    """Return (db, collection) pairs in scope, enumerated via pymongo."""
    SYSTEM_DBS = {"admin", "local", "config"}
    if db_name and collection_name:
        return [(db_name, collection_name)]
    if db_name:
        colls = mongo_client[db_name].list_collection_names()
        return [(db_name, c) for c in sorted(colls)]
    # No db specified — enumerate all user databases and their collections
    pairs = []
    for db_info in mongo_client.list_databases():
        name = db_info["name"]
        if name in SYSTEM_DBS:
            continue
        try:
            colls = mongo_client[name].list_collection_names()
            for c in sorted(colls):
                pairs.append((name, c))
        except Exception as e:
            logger.warning(f"Could not list collections for db '{name}': {e}")
    return pairs


def scan_cluster(
    atlas_client: AtlasClient,
    cfg: dict,
    project: str,
    project_id: str,
    cluster: str,
    srv_host: str,
    db_name: str | None,
    collection_name: str | None,
    slow_threshold_ms: int,
    skip_query_stats: bool,
) -> None:
    """Run all diagnostics for a single cluster and print its report."""

    process_ids = atlas_client.get_process_ids(project_id, cluster)
    process_id = process_ids[0] if process_ids else None

    # --- Atlas API: cluster-level data (fetched once) ---
    drop_suggestions = atlas_client.get_drop_index_suggestions(project_id, cluster)
    schema_advice = atlas_client.get_schema_advice(project_id, cluster)

    slow_namespaces = {"namespaces": []}
    slow_queries = []
    collscan_queries = []
    if process_id:
        slow_namespaces = atlas_client.get_slow_namespaces(project_id, process_id)
        slow_queries = atlas_client.get_slow_queries(project_id, process_id)
        collscan_queries = [
            q for q in slow_queries if "COLLSCAN" in q.get("planSummary", "")
        ]

    # --- pymongo: enumerate in-scope (db, collection) pairs ---
    mongo_client = _connect(cfg, srv_host) if srv_host else None
    ns_pairs: list[tuple[str, str]] = []
    if mongo_client:
        try:
            ns_pairs = _list_namespaces_from_mongo(
                mongo_client, db_name, collection_name
            )
        except Exception as e:
            logger.warning(f"Could not enumerate namespaces from cluster: {e}")

    # Derive the Atlas namespace strings from the enumerated pairs so we know exactly
    # which namespaces are in scope for filtering.
    scoped_namespaces = [f"{db}.{coll}" for db, coll in ns_pairs]
    scoped_ns_set = set(scoped_namespaces)
    suggestion_namespaces = scoped_namespaces

    logger.debug(f"Fetching index suggestions for namespaces: {suggestion_namespaces}")

    # --- Atlas API: index suggestions per namespace ---
    index_suggestions = atlas_client.get_index_suggestions_for_namespaces(
        project_id, cluster, suggestion_namespaces
    )

    # --- Client-side filter slow query data to in-scope namespaces ---
    if scoped_ns_set:
        slow_queries = [q for q in slow_queries if q.get("namespace") in scoped_ns_set]
        collscan_queries = [
            q for q in collscan_queries if q.get("namespace") in scoped_ns_set
        ]
        ns_list = slow_namespaces.get("namespaces", [])
        slow_namespaces["namespaces"] = [
            ns
            for ns in ns_list
            if (ns.get("namespace") if isinstance(ns, dict) else ns) in scoped_ns_set
        ]

    # --- pymongo: per-collection diagnostics and query stats ---
    coll_diagnostics: dict[str, dict] = {}
    query_stats: list[dict] | None = None

    if mongo_client:
        for db, coll in ns_pairs:
            try:
                coll_diagnostics[f"{db}.{coll}"] = run_collection_diagnostics(
                    mongo_client, db, coll
                )
            except Exception as e:
                logger.warning(f"Collection diagnostics failed for {db}.{coll}: {e}")

        if not skip_query_stats:
            try:
                query_stats = run_query_stats(
                    mongo_client,
                    slow_threshold_ms,
                    db_name=db_name,
                    collection_name=collection_name,
                )
            except Exception as e:
                logger.warning(f"Query stats failed: {e}")

    print_report(
        project=project,
        cluster=cluster,
        db_name=db_name,
        collection_name=collection_name,
        index_suggestions=index_suggestions,
        drop_suggestions=drop_suggestions,
        schema_advice=schema_advice,
        slow_namespaces=slow_namespaces,
        slow_queries=slow_queries,
        collscan_queries=collscan_queries,
        coll_diagnostics=coll_diagnostics,
        query_stats=query_stats,
        slow_threshold_ms=slow_threshold_ms,
    )


def main():
    parser = argparse.ArgumentParser(
        prog="atmon",
        description="Scan MongoDB Atlas for performance issues.",
    )

    parser.add_argument("project", help="Atlas target project name")
    parser.add_argument(
        "cluster",
        nargs="?",
        help="Atlas target cluster name (optional, omit for all clusters)",
    )
    parser.add_argument(
        "db", nargs="?", help="Database name (optional, omit for all namespaces)"
    )
    parser.add_argument(
        "collection", nargs="?", help="Collection name (optional, requires db)"
    )
    parser.add_argument(
        "--skip-query-stats", action="store_true", help="Skip query stats"
    )
    parser.add_argument(
        "--slow-threshold-ms", type=int, default=1000, help="Slow threshold (ms)"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    project = args.project
    cluster = args.cluster
    db_name = args.db
    collection_name = args.collection

    if collection_name and not db_name:
        parser.error("collection requires db to be specified")

    if project not in ATLAS_CONFIGS:
        logger.error(
            f"Unknown project '{project}'. Available: {list(ATLAS_CONFIGS.keys())}"
        )
        sys.exit(1)

    cfg = ATLAS_CONFIGS[project]
    project_id = cfg["project_id"]

    try:
        atlas_client = AtlasClient(
            public_key=cfg["public_api_key"],
            private_key=cfg["private_api_key_secret"],
        )

        if cluster:
            clusters = atlas_client.list_clusters(project_id)
            match = next((c for c in clusters if c["name"] == cluster), None)
            if not match:
                logger.error(f"Cluster '{cluster}' not found in project '{project}'")
                sys.exit(1)
            srv_host = match["srv_host"]

            logger.info(f"Running diagnostics for {project}/{cluster}...")
            scan_cluster(
                atlas_client,
                cfg,
                project,
                project_id,
                cluster,
                srv_host,
                db_name,
                collection_name,
                args.slow_threshold_ms,
                args.skip_query_stats,
            )
        else:
            # All clusters in project
            clusters = atlas_client.list_clusters(project_id)
            if not clusters:
                logger.error(f"No clusters found in project '{project}'")
                sys.exit(1)

            logger.info(f"Scanning {len(clusters)} cluster(s) in {project}...")
            for i, cl in enumerate(clusters):
                if i > 0:
                    print("\n")
                logger.info(f"Scanning cluster {cl['name']}...")
                scan_cluster(
                    atlas_client,
                    cfg,
                    project,
                    project_id,
                    cl["name"],
                    cl["srv_host"],
                    db_name,
                    collection_name,
                    args.slow_threshold_ms,
                    args.skip_query_stats,
                )

    except Exception as e:
        logger.error(f"Failed to run diagnostics: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
