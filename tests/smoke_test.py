"""
Smoke test for every tab and API endpoint.

Runs against a real local instance (start with: uvicorn app:app --port 8000).
Uses X-Forwarded-User header to bypass Allowlist middleware.

Usage:
    python tests/smoke_test.py [--base-url http://localhost:8000]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import urllib.request
import urllib.error

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_BASE = "http://localhost:8000"
AUTH_HEADER = {"X-Forwarded-User": "smoketest@example.com"}

# All API endpoints to test, grouped by tab
ENDPOINTS: dict[str, list[dict]] = {
    "Health": [
        {"method": "GET", "path": "/api/v1/health/", "expect_keys": ["status"]},
        {"method": "GET", "path": "/api/v1/health/whoami", "expect_keys": ["user"]},
    ],
    "Cost Dashboard": [
        {"method": "GET", "path": "/api/v1/cost/filters", "expect_keys": ["workspaces"]},
        {"method": "GET", "path": "/api/v1/cost/summary", "expect_keys": None},
        {"method": "GET", "path": "/api/v1/cost/daily-trend", "expect_keys": None},
        {"method": "GET", "path": "/api/v1/cost/by-product", "expect_keys": None},
        {"method": "GET", "path": "/api/v1/cost/by-workspace", "expect_keys": None},
        {"method": "GET", "path": "/api/v1/cost/anomalies", "expect_keys": None},
    ],
    "Anomalies (ML)": [
        {"method": "GET", "path": "/api/v1/cost/ml-anomalies", "expect_keys": ["anomalies"]},
    ],
    "User Adoption": [
        {"method": "GET", "path": "/api/v1/platform/user-adoption", "expect_keys": None},
    ],
    "Compute Sizing": [
        {"method": "GET", "path": "/api/v1/platform/compute-insights", "expect_keys": None},
    ],
    "Query Attribution": [
        {"method": "GET", "path": "/api/v1/platform/query-insights", "expect_keys": None},
    ],
    "AI / LLM": [
        {"method": "GET", "path": "/api/v1/platform/ai-insights", "expect_keys": None},
    ],
    "UC Graph / Access": [
        {"method": "GET", "path": "/api/v1/platform/access-governance", "expect_keys": None},
    ],
    "Job SLA": [
        {"method": "GET", "path": "/api/v1/platform/job-insights", "expect_keys": None},
    ],
    "Storage": [
        {"method": "GET", "path": "/api/v1/platform/storage-insights", "expect_keys": None},
    ],
    "Lineage & ML": [
        {"method": "GET", "path": "/api/v1/platform/lineage-insights", "expect_keys": None},
    ],
}

# HTML element IDs that must exist per tab
TAB_ELEMENTS: dict[str, list[str]] = {
    "tab-cost": [
        "kpi-dbus", "kpi-cost", "kpi-ws", "kpi-prod",
        "trend-chart", "cost-trend-chart", "donut-chart",
        "filter-bar", "start-date", "end-date",
    ],
    "tab-ml": [
        "ml-kpi-running", "ml-kpi-anomalies", "ml-kpi-idle",
        "ml-kpi-drift", "ml-kpi-critical",
        "ml-running-wrap", "ml-anomaly-wrap", "ml-idle-wrap", "ml-drift-wrap",
    ],
    "tab-adopt": [
        "adopt-kpi-total", "adopt-kpi-7d", "adopt-kpi-30d", "adopt-kpi-inactive",
        "adopt-dau-chart", "adopt-svc-chart",
        "adopt-users-wrap", "adopt-inactive-wrap",
    ],
    "tab-compute": [
        "cmp-kpi-clusters", "cmp-kpi-warehouses", "cmp-kpi-noterm",
        "cmp-kpi-nostop", "cmp-kpi-noscale", "cmp-kpi-underutil",
        "cmp-dbr-chart", "cmp-size-chart",
        "cmp-clusters-wrap", "cmp-warehouses-wrap",
    ],
    "tab-queries": [
        "qry-kpi-total", "qry-kpi-users", "qry-kpi-gb",
        "qry-kpi-failed", "qry-kpi-slow",
        "qry-trend-chart", "qry-wh-chart",
        "qry-users-wrap", "qry-expensive-wrap", "qry-slow-wrap",
    ],
    "tab-aiusage": [
        "ai-kpi-requests", "ai-kpi-tokens", "ai-kpi-cost",
        "ai-kpi-models", "ai-kpi-users",
        "ai-trend-chart", "ai-model-chart",
        "ai-models-wrap", "ai-users-wrap",
    ],
    "tab-ucgraph": [
        "ucg-kpi-users", "ucg-kpi-groups", "ucg-kpi-principals",
        "ucg-graph-wrap", "ucg-svg", "ucg-cards-wrap",
    ],
    "tab-jobsla": [
        "job-kpi-jobs", "job-kpi-runs", "job-kpi-sla",
        "job-kpi-failed", "job-kpi-pipelines",
        "job-trend-chart", "job-sla-chart",
        "job-list-wrap", "job-fail-wrap", "job-pipe-wrap",
    ],
    "tab-datastorage": [
        "stor-kpi-catalogs", "stor-kpi-schemas", "stor-kpi-tables",
        "stor-kpi-ops", "stor-kpi-workspaces",
        "stor-schema-wrap", "stor-ophist-wrap",
    ],
    "tab-datalineage": [
        "lin-kpi-tables", "lin-kpi-edges", "lin-kpi-endpoints", "lin-kpi-models",
        "lin-lineage-wrap", "lin-serving-wrap", "lin-models-wrap",
    ],
}

# JavaScript functions that must exist
JS_FUNCTIONS = [
    "loadDashboard", "loadFilters", "loadSummary", "loadTrendChart",
    "loadDonutChart", "loadSpikeDrilldown",
    "loadMLAnomalies", "loadUserAdoption", "loadComputeInsights",
    "loadQueryInsights", "loadAIInsights", "loadAccessGovernance",
    "loadUCGraph", "loadJobSLA", "loadStorageInsights", "loadLineageInsights",
    "switchTab", "apiFetch", "destroyChart",
    "_lazyLoad",  # New lazy-load tracker
]


# ── Result tracking ───────────────────────────────────────────────────────────

@dataclass
class TestResult:
    name: str
    status: str  # PASS, FAIL, WARN
    detail: str = ""
    duration_ms: float = 0


@dataclass
class SmokeReport:
    results: list[TestResult] = field(default_factory=list)
    start_time: float = 0

    def add(self, name: str, status: str, detail: str = "", duration_ms: float = 0):
        self.results.append(TestResult(name, status, detail, duration_ms))

    def summary(self) -> str:
        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = sum(1 for r in self.results if r.status == "FAIL")
        warned = sum(1 for r in self.results if r.status == "WARN")
        total = len(self.results)
        elapsed = time.time() - self.start_time

        lines = [
            "",
            "=" * 72,
            f"  SMOKE TEST REPORT  —  {total} tests  |  "
            f"✅ {passed} passed  |  ❌ {failed} failed  |  ⚠️  {warned} warnings",
            f"  Total time: {elapsed:.1f}s",
            "=" * 72,
            "",
        ]

        # Group by section
        current_section = ""
        for r in self.results:
            section = r.name.split(" :: ")[0] if " :: " in r.name else ""
            if section != current_section:
                current_section = section
                lines.append(f"\n  ── {section} ──")

            icon = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️ "}.get(r.status, "?")
            short_name = r.name.split(" :: ")[-1] if " :: " in r.name else r.name
            time_str = f" ({r.duration_ms:.0f}ms)" if r.duration_ms > 0 else ""
            detail_str = f"  → {r.detail}" if r.detail else ""
            lines.append(f"    {icon} {short_name}{time_str}{detail_str}")

        lines.append("")
        if failed > 0:
            lines.append(f"  ❌ {failed} FAILURES — review above")
        else:
            lines.append("  ✅ ALL TESTS PASSED")
        lines.append("")
        return "\n".join(lines)


# ── HTTP helper ───────────────────────────────────────────────────────────────

def http_get(base_url: str, path: str, headers: dict | None = None, timeout: int = 60) -> tuple[int, str, float]:
    """Return (status_code, body, duration_ms)."""
    url = f"{base_url}{path}"
    req = urllib.request.Request(url, headers=headers or {})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body, (time.time() - t0) * 1000
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        return e.code, body, (time.time() - t0) * 1000
    except Exception as e:
        return 0, str(e), (time.time() - t0) * 1000


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_connectivity(report: SmokeReport, base_url: str):
    """Can we reach the server at all?"""
    code, body, ms = http_get(base_url, "/api/v1/health/")
    if code == 200:
        report.add("Connectivity :: Server reachable", "PASS", f"HTTP {code}", ms)
    else:
        report.add("Connectivity :: Server reachable", "FAIL", f"HTTP {code}: {body[:200]}", ms)
        print(f"\n❌ Cannot reach {base_url} — is the app running?")
        print(f"   Start it with: uvicorn app:app --port 8000\n")
        sys.exit(1)


def test_html_served(report: SmokeReport, base_url: str):
    """Static HTML page loads and contains all tabs."""
    code, body, ms = http_get(base_url, "/", {**AUTH_HEADER, "Accept": "text/html"})
    if code == 200 and "<html" in body.lower():
        report.add("HTML :: Page loads", "PASS", f"{len(body)} bytes", ms)
    else:
        report.add("HTML :: Page loads", "FAIL", f"HTTP {code}, body={body[:100]}", ms)
        return

    # Check all 10 tabs exist
    for tab_id in TAB_ELEMENTS:
        if f'id="{tab_id}"' in body:
            report.add(f"HTML :: Tab #{tab_id}", "PASS")
        else:
            report.add(f"HTML :: Tab #{tab_id}", "FAIL", "Element not found in HTML")

    # Check all section/KPI element IDs per tab
    for tab_id, elements in TAB_ELEMENTS.items():
        for elem_id in elements:
            if f'id="{elem_id}"' in body:
                report.add(f"HTML :: {tab_id} :: #{elem_id}", "PASS")
            else:
                report.add(f"HTML :: {tab_id} :: #{elem_id}", "FAIL", "Missing element ID")

    # Check JavaScript functions exist
    for fn_name in JS_FUNCTIONS:
        # Match function declarations: function name( or async function name( or const name = 
        if re.search(rf'\b(async\s+)?function\s+{re.escape(fn_name)}\b', body) or \
           re.search(rf'\bconst\s+{re.escape(fn_name)}\b', body):
            report.add(f"HTML :: JS :: {fn_name}()", "PASS")
        else:
            report.add(f"HTML :: JS :: {fn_name}()", "FAIL", "Function not found")

    # Check lazy-load tracker (Fix #6)
    if "_tabLoaded" in body:
        report.add("HTML :: JS :: Lazy-load tracker", "PASS")
    else:
        report.add("HTML :: JS :: Lazy-load tracker", "FAIL", "_tabLoaded not found")

    # Verify no prefetch storm
    if "Promise.allSettled" not in body:
        report.add("HTML :: JS :: No prefetch storm", "PASS")
    else:
        report.add("HTML :: JS :: No prefetch storm", "WARN", "Promise.allSettled still present")

    # Check Chart.js loaded
    if "chart.js" in body.lower() or "Chart(" in body:
        report.add("HTML :: JS :: Chart.js", "PASS")
    else:
        report.add("HTML :: JS :: Chart.js", "WARN", "Chart.js reference not found")


def test_api_endpoints(report: SmokeReport, base_url: str):
    """Test each API endpoint returns 200 and valid JSON."""
    for tab_name, endpoints in ENDPOINTS.items():
        for ep in endpoints:
            path = ep["path"]
            code, body, ms = http_get(base_url, path, AUTH_HEADER, timeout=120)

            test_name = f"{tab_name} :: GET {path}"

            if code == 200:
                # Try to parse JSON
                try:
                    data = json.loads(body)
                except json.JSONDecodeError:
                    report.add(test_name, "FAIL", f"HTTP 200 but invalid JSON: {body[:100]}", ms)
                    continue

                # Check expected keys if specified
                if ep["expect_keys"] and isinstance(data, dict):
                    missing = [k for k in ep["expect_keys"] if k not in data]
                    if missing:
                        report.add(test_name, "WARN", f"Missing keys: {missing}", ms)
                    else:
                        report.add(test_name, "PASS", _response_summary(data), ms)
                else:
                    report.add(test_name, "PASS", _response_summary(data), ms)

            elif code in (400, 422):
                # Expected for endpoints needing params we didn't send
                report.add(test_name, "WARN", f"HTTP {code} (may need params)", ms)
            else:
                report.add(test_name, "FAIL", f"HTTP {code}: {body[:200]}", ms)


def test_security_headers(report: SmokeReport, base_url: str):
    """Check security headers are present."""
    url = f"{base_url}/api/v1/health/"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            headers = dict(resp.headers)
            expected = [
                "X-Content-Type-Options",
                "X-Frame-Options",
                "Referrer-Policy",
            ]
            for h in expected:
                if h.lower() in {k.lower() for k in headers}:
                    report.add(f"Security :: {h}", "PASS", headers.get(h, ""))
                else:
                    report.add(f"Security :: {h}", "WARN", "Header missing")
    except Exception as e:
        report.add("Security :: Headers check", "FAIL", str(e))


def test_auth_enforcement(report: SmokeReport, base_url: str):
    """Test that protected endpoints reject unauthenticated requests."""
    # Request WITHOUT auth header
    code, body, ms = http_get(base_url, "/api/v1/cost/filters", {})
    if code == 403:
        report.add("Auth :: Protected endpoint rejects anon", "PASS", f"HTTP {code}", ms)
    elif code == 200:
        report.add("Auth :: Protected endpoint rejects anon", "WARN",
                    "Endpoint returned 200 without auth (allowlist may be empty)", ms)
    else:
        report.add("Auth :: Protected endpoint rejects anon", "WARN", f"HTTP {code}", ms)

    # Health should be public
    code, body, ms = http_get(base_url, "/api/v1/health/", {})
    if code == 200:
        report.add("Auth :: Health is public", "PASS", f"HTTP {code}", ms)
    else:
        report.add("Auth :: Health is public", "FAIL", f"HTTP {code}", ms)


def test_rate_limiting(report: SmokeReport, base_url: str):
    """Verify rate limiter allows normal traffic."""
    # Just confirm we don't get 429 with a few requests
    for i in range(5):
        code, _, _ = http_get(base_url, "/api/v1/health/", AUTH_HEADER)
        if code == 429:
            report.add("RateLimit :: Normal traffic", "FAIL",
                        f"Got 429 on request #{i+1}")
            return
    report.add("RateLimit :: Normal traffic", "PASS", "5 requests without 429")


def _response_summary(data) -> str:
    """Create a brief summary of the JSON response."""
    if isinstance(data, dict):
        keys = list(data.keys())[:6]
        extra = f" +{len(data) - 6} more" if len(data) > 6 else ""
        return f"dict({len(data)} keys: {', '.join(keys)}{extra})"
    if isinstance(data, list):
        return f"list({len(data)} items)"
    return str(type(data).__name__)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Smoke test for Cost Observability")
    parser.add_argument("--base-url", default=DEFAULT_BASE,
                       help=f"Base URL (default: {DEFAULT_BASE})")
    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")

    print(f"\n🔥 Smoke Testing: {base_url}")
    print("=" * 72)

    report = SmokeReport()
    report.start_time = time.time()

    # Run test suites in order
    test_connectivity(report, base_url)
    test_html_served(report, base_url)
    test_security_headers(report, base_url)
    test_auth_enforcement(report, base_url)
    test_rate_limiting(report, base_url)
    test_api_endpoints(report, base_url)

    # Print report
    print(report.summary())

    # Exit with failure code if any test failed
    failures = sum(1 for r in report.results if r.status == "FAIL")
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
