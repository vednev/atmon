import gzip
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class AtlasClient:
    BASE_URL = "https://cloud.mongodb.com/api/atlas/v2"

    def __init__(
        self, public_key: Optional[str] = None, private_key: Optional[str] = None
    ):
        self.public_key = public_key or os.environ.get("ATLAS_PUBLIC_KEY", "")
        self.private_key = private_key or os.environ.get("ATLAS_PRIVATE_KEY", "")
        self.auth = httpx.DigestAuth(self.public_key, self.private_key)

    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        headers = {"Accept": "application/vnd.atlas.2024-08-05+json"}
        with httpx.Client(timeout=120) as client:
            response = client.request(
                method, url, auth=self.auth, headers=headers, **kwargs
            )
            response.raise_for_status()
            return response.json()

    def _request_raw(self, method: str, endpoint: str, **kwargs) -> httpx.Response:
        url = f"{self.BASE_URL}{endpoint}"
        headers = {"Accept": "application/vnd.atlas.2024-08-05+gzip"}
        # For raw downloads, we'll return the response so the caller can handle iteration.
        # Note: the caller needs to ensure the client Context manager remains open, or we
        # simply use a single one-off request here for simplicity, considering the original
        # code used single requests.request
        client = httpx.Client(timeout=120)
        response = client.request(
            method, url, auth=self.auth, headers=headers, **kwargs
        )
        response.raise_for_status()
        return response

    def list_processes(self, project_id: str) -> List[Dict[str, Any]]:
        endpoint = f"/groups/{project_id}/processes"
        result = self._request("GET", endpoint)
        return result.get("results", [])

    def _normalize_slow_query(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        metrics = entry.get("metrics") or {}

        line_data: Dict[str, Any] = {}
        line = entry.get("line", "")
        if line:
            try:
                line_data = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                pass

        attr = line_data.get("attr") or line_data
        command = attr.get("command") or {}

        normalized: Dict[str, Any] = {
            "namespace": entry.get("namespace", "unknown"),
            "millis": metrics.get("operationExecutionTime", 0),
            "docsExamined": metrics.get("docsExamined", 0),
            "nreturned": metrics.get("docsReturned", 0),
            "hasSortStage": metrics.get("hasSort", False),
            "keysExamined": metrics.get("keysExamined", 0),
            "planSummary": attr.get("planSummary", ""),
            "query": command.get("filter") or command.get("q") or command,
            "line": line,
        }

        query_shape = attr.get("queryShape")
        if query_shape:
            normalized["queryShape"] = query_shape

        return normalized

    def get_slow_queries(
        self,
        project_id: str,
        process_id: str,
        since_hours: int = 24,
        namespace: Optional[str] = None,
        n_logs: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        since_ms = int(since.timestamp() * 1000)

        endpoint = f"/groups/{project_id}/processes/{process_id}/performanceAdvisor/slowQueryLogs"
        params: Dict[str, Any] = {"since": since_ms, "includeMetrics": True}
        if namespace:
            params["namespaces"] = namespace
        if n_logs is not None:
            params["nLogs"] = n_logs

        result = self._request("GET", endpoint, params=params)
        return [
            self._normalize_slow_query(raw) for raw in result.get("slowQueries", [])
        ]

    def get_collscan_queries(
        self, project_id: str, process_id: str, since_hours: int = 24
    ) -> List[Dict[str, Any]]:
        queries = self.get_slow_queries(project_id, process_id, since_hours)
        return [q for q in queries if "COLLSCAN" in q.get("planSummary", "")]

    def download_host_logs(
        self,
        project_id: str,
        host: str,
        output_path: Path,
        since_hours: int = 24,
    ) -> Path:
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        since_ms = int(since.timestamp() * 1000)

        endpoint = f"/groups/{project_id}/clusters/{host}/logs/mongodb.gz"
        params = {"startDate": since_ms}

        response = self._request_raw("GET", endpoint, params=params)

        output_path.mkdir(parents=True, exist_ok=True)
        log_file = output_path / f"{host}_mongodb.log"

        with open(log_file, "wb") as f:
            for chunk in response.iter_bytes(chunk_size=8192):
                f.write(gzip.decompress(chunk))

        return log_file

    def list_clusters(self, project_id: str) -> List[Dict[str, Any]]:
        """Return cluster names and SRV hostnames for a project."""
        data = self._request("GET", f"/groups/{project_id}/clusters")
        clusters = []
        for c in data.get("results", []):
            srv = c.get("connectionStrings", {}).get("standardSrv", "")
            # Extract host from "mongodb+srv://host.example.net"
            srv_host = srv.replace("mongodb+srv://", "") if srv else ""
            clusters.append({"name": c["name"], "srv_host": srv_host})
        return sorted(clusters, key=lambda c: c["name"])

    def get_process_ids(self, project_id: str, cluster: str) -> List[str]:
        data = self._request("GET", f"/groups/{project_id}/processes")
        cluster_lower = cluster.lower()
        return [
            f"{p['hostname']}:{p['port']}"
            for p in data.get("results", [])
            if p.get("userAlias", "").lower().startswith(cluster_lower + "-")
            and p.get("typeName")
            in ("REPLICA_PRIMARY", "SHARD_PRIMARY", "SHARD_MONGOS")
        ]

    def get_index_suggestions(
        self, project_id: str, cluster: str, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        params = {}
        if namespace:
            params["namespaces"] = namespace
        result = self._request(
            "GET",
            f"/groups/{project_id}/clusters/{cluster}/performanceAdvisor/suggestedIndexes",
            params=params,
        )
        # API wraps data under a "content" key; normalise to a flat dict.
        return result.get("content", result)

    def get_index_suggestions_for_namespaces(
        self, project_id: str, cluster: str, namespaces: List[str]
    ) -> Dict[str, Any]:
        """Fetch and merge index suggestions for a list of namespaces."""
        merged: Dict[str, Any] = {"suggestedIndexes": [], "shapes": []}
        seen_index_ids: set = set()
        seen_shape_ids: set = set()
        for ns in namespaces:
            try:
                result = self.get_index_suggestions(project_id, cluster, namespace=ns)
            except Exception as e:
                logger.warning(f"Could not get index suggestions for {ns}: {e}")
                continue
            for idx in result.get("suggestedIndexes", []):
                idx_id = idx.get("id") or str(idx)
                if idx_id not in seen_index_ids:
                    seen_index_ids.add(idx_id)
                    merged["suggestedIndexes"].append(idx)
            for shape in result.get("shapes", []):
                shape_id = shape.get("id") or str(shape)
                if shape_id not in seen_shape_ids:
                    seen_shape_ids.add(shape_id)
                    merged["shapes"].append(shape)
        return merged

    def get_drop_index_suggestions(
        self, project_id: str, cluster: str
    ) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/groups/{project_id}/clusters/{cluster}/performanceAdvisor/dropIndexSuggestions",
        )

    def get_schema_advice(self, project_id: str, cluster: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/groups/{project_id}/clusters/{cluster}/performanceAdvisor/schemaAdvice",
        )

    def get_slow_namespaces(self, project_id: str, process_id: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/groups/{project_id}/processes/{process_id}/performanceAdvisor/namespaces",
        )

    def collect_logs(
        self,
        project_id: str,
        output_path: Path,
        since_hours: int = 24,
    ) -> Path:
        """Download mongod logs for all processes in a project."""
        output_path.mkdir(parents=True, exist_ok=True)
        processes = self.list_processes(project_id)
        for process in processes:
            hostname = process.get("hostname", "")
            if hostname:
                self.download_host_logs(
                    project_id=project_id,
                    host=hostname,
                    output_path=output_path,
                    since_hours=since_hours,
                )
        return output_path
