"""
Standalone daily cost alert emailer — runs as a scheduled Databricks Job.

Detects spend spikes vs a 30-day rolling baseline and emails a summary.
Configure via environment variables (set as Databricks Job env vars or secrets).

Required env vars:
  DATABRICKS_HOST         workspace URL
  DATABRICKS_TOKEN        personal access token
  DATABRICKS_WAREHOUSE_ID SQL warehouse ID
  SMTP_USER               sender email (e.g. yourname@gmail.com)
  SMTP_PASSWORD           app password (Gmail: Settings → App passwords)
  ALERT_EMAIL_TO          recipient(s), comma-separated

Optional env vars:
  SMTP_HOST               default: smtp.gmail.com
  SMTP_PORT               default: 587
  ALERT_THRESHOLD_PCT     spike threshold %, default: 20
  APP_URL                 app URL for "Open Dashboard" link in email
  MOCK_MODE               true/false (inherits from app env)
"""

from __future__ import annotations

import html
import os
import smtplib
import sys
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ── Config ────────────────────────────────────────────────────────────────────
HOST         = os.environ["DATABRICKS_HOST"]
TOKEN        = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "21f5bd20b7f44a51")
SMTP_HOST    = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT    = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER    = os.environ["SMTP_USER"]
SMTP_PASS    = os.environ["SMTP_PASSWORD"]
ALERT_TO     = os.environ["ALERT_EMAIL_TO"]
THRESHOLD    = float(os.environ.get("ALERT_THRESHOLD_PCT", "20"))
APP_URL      = os.environ.get("APP_URL", "")
MOCK_MODE    = os.environ.get("MOCK_MODE", "false").lower() == "true"

client = WorkspaceClient(host=HOST, token=TOKEN)

# ── SQL rewriter (mirrors core/sql_executor.py) ───────────────────────────────
import re

_MOCK_TABLE_MAP = {
    r"\bsystem\.billing\b":             "workspace.mock_system_billing",
    r"\bsystem\.access\b":              "workspace.mock_system_access",
    r"\bsystem\.compute\b":             "workspace.mock_system_compute",
    r"\bsystem\.lakeflow\b":            "workspace.mock_system_lakeflow",
    r"\bsystem\.query\b":               "workspace.mock_system_query",
    r"\bsystem\.ai_gateway\b":          "workspace.mock_system_ai_gateway",
    r"\bsystem\.serving\b":             "workspace.mock_system_serving",
    r"\bsystem\.mlflow\b":              "workspace.mock_system_mlflow",
    r"\bsystem\.storage\b":             "workspace.mock_system_storage",
    r"\bsystem\.information_schema\b":  "workspace.mock_system_information_schema",
    r"\bsystem\.networking\b":          "workspace.mock_system_networking",
    r"\bsystem\.lakeview\b":            "workspace.mock_system_lakeview",
    r"\bsystem\.dashboards\b":          "workspace.mock_system_dashboards",
    r"\bsystem\.marketplace\b":         "workspace.mock_system_marketplace",
}

def _rewrite(sql: str) -> str:
    if not MOCK_MODE:
        return sql
    for pattern, replacement in _MOCK_TABLE_MAP.items():
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    return sql


def run_sql(sql: str) -> list[dict]:
    resp = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=_rewrite(sql),
        wait_timeout="50s",
    )
    deadline = time.time() + 300
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            return []
        time.sleep(3)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        print(f"  SQL failed: {getattr(resp.status.error, 'message', resp.status.state)}")
        return []

    if not resp.manifest or not resp.manifest.schema or not resp.manifest.schema.columns:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    return [
        {k: (str(v) if v is not None else None) for k, v in zip(cols, row)}
        for row in (resp.result.data_array or [])
    ]


# ── Spike detection ───────────────────────────────────────────────────────────

def fetch_daily_spend() -> list[tuple[str, float]]:
    rows = run_sql("""
        SELECT
            CAST(DATE(u.usage_start_time) AS STRING)                          AS date,
            ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS daily_cost
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
            ON  u.sku_name          = p.sku_name
            AND u.cloud             = p.cloud
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
        WHERE u.usage_start_time >= timestamp(date_add(current_date(), -37))
        GROUP BY DATE(u.usage_start_time)
        ORDER BY date
    """)
    return [(r["date"], float(r["daily_cost"] or 0)) for r in rows]


def fetch_sku_wow() -> list[dict]:
    return run_sql("""
        SELECT
            u.sku_name,
            ROUND(SUM(CASE WHEN u.usage_start_time >= timestamp(date_add(current_date(), -7))
                THEN u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_7d,
            ROUND(SUM(CASE
                WHEN u.usage_start_time >= timestamp(date_add(current_date(), -14))
                 AND u.usage_start_time <  timestamp(date_add(current_date(), -7))
                THEN u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_prior_7d
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
            ON  u.sku_name          = p.sku_name
            AND u.cloud             = p.cloud
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
        WHERE u.usage_start_time >= timestamp(date_add(current_date(), -14))
        GROUP BY u.sku_name
        ORDER BY cost_7d DESC
    """)


def detect_alerts(costs: list[tuple], sku_rows: list[dict]) -> list[dict]:
    alerts = []
    baseline = [c for _, c in costs[:-7]] if len(costs) > 7 else [c for _, c in costs]
    baseline_avg = sum(baseline) / len(baseline) if baseline else 0
    recent = costs[-7:] if len(costs) >= 7 else costs
    recent_avg = sum(c for _, c in recent) / len(recent) if recent else 0

    if baseline_avg > 0:
        pct = (recent_avg - baseline_avg) / baseline_avg * 100
        if pct > THRESHOLD:
            alerts.append({
                "severity": "HIGH" if pct > THRESHOLD * 2 else "MEDIUM",
                "title":    f"Spend up {pct:.1f}% above baseline",
                "detail":   f"7-day avg ${recent_avg:,.2f}/day vs ${baseline_avg:,.2f}/day baseline",
            })

    for date, cost in recent:
        if baseline_avg > 0 and cost > baseline_avg * 2:
            alerts.append({
                "severity": "HIGH",
                "title":    f"Extreme spike on {date}: ${cost:,.2f}",
                "detail":   f"{cost / baseline_avg:.1f}× the daily baseline",
            })

    for r in sku_rows:
        prior  = float(r.get("cost_prior_7d") or 0)
        recent_cost = float(r.get("cost_7d") or 0)
        sku    = r.get("sku_name", "")
        if prior == 0 and recent_cost > 10:
            alerts.append({
                "severity": "MEDIUM",
                "title":    f"New SKU: {sku}",
                "detail":   f"${recent_cost:,.2f} this week — not seen the prior week",
            })
        elif prior > 0:
            sku_pct = (recent_cost - prior) / prior * 100
            if sku_pct > THRESHOLD * 1.5:
                alerts.append({
                    "severity": "MEDIUM",
                    "title":    f"{sku} +{sku_pct:.0f}% week-over-week",
                    "detail":   f"${recent_cost:,.2f} this week vs ${prior:,.2f} last week",
                })

    return alerts


# ── Email builder ─────────────────────────────────────────────────────────────

def build_email(costs, alerts, baseline_avg, recent_avg) -> str:
    total_30d = sum(c for _, c in costs[-30:])
    pct = (recent_avg - baseline_avg) / baseline_avg * 100 if baseline_avg > 0 else 0
    pct_color = "#ff4444" if pct > 0 else "#00cc88"
    arrow     = "↑" if pct > 0 else "↓"

    rows_html = "".join(f"""
    <tr>
      <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;
                 color:{'#ff4444' if a['severity']=='HIGH' else '#ffaa00'};font-weight:bold">
        {a['severity']}</td>
      <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-weight:500">
        {html.escape(a['title'])}</td>
      <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;color:#aaa">
        {html.escape(a['detail'])}</td>
    </tr>""" for a in alerts)

    link = (f'<a href="{html.escape(APP_URL)}" style="color:#FF3621;text-decoration:none">'
            f'Open Dashboard →</a>' if APP_URL else "")

    return f"""
    <html><body style="background:#13131f;color:#e0e0e0;font-family:Arial,sans-serif;
                       padding:32px;max-width:700px;margin:0 auto">
      <div style="margin-bottom:24px">
        <span style="color:#FF3621;font-size:22px;font-weight:bold">⚠ Databricks Cost Alert</span>
      </div>
      <table style="width:100%;border-spacing:12px;border-collapse:separate;margin-bottom:24px">
        <tr>
          <td style="background:#1e1e30;border-radius:8px;padding:16px;text-align:center">
            <div style="color:#888;font-size:11px">30-DAY SPEND</div>
            <div style="font-size:26px;font-weight:bold">${total_30d:,.2f}</div>
          </td>
          <td style="background:#1e1e30;border-radius:8px;padding:16px;text-align:center">
            <div style="color:#888;font-size:11px">7-DAY AVG / DAY</div>
            <div style="font-size:26px;font-weight:bold">${recent_avg:,.2f}</div>
            <div style="color:{pct_color}">{arrow}{abs(pct):.1f}% vs baseline</div>
          </td>
          <td style="background:#1e1e30;border-radius:8px;padding:16px;text-align:center">
            <div style="color:#888;font-size:11px">ALERTS</div>
            <div style="font-size:26px;font-weight:bold;color:#ff4444">{len(alerts)}</div>
          </td>
        </tr>
      </table>
      <table style="width:100%;border-collapse:collapse;background:#1e1e30;border-radius:8px">
        <thead><tr style="background:#252540">
          <th style="padding:10px 8px;text-align:left;color:#888;font-size:12px">SEVERITY</th>
          <th style="padding:10px 8px;text-align:left;color:#888;font-size:12px">ALERT</th>
          <th style="padding:10px 8px;text-align:left;color:#888;font-size:12px">DETAIL</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
      <p style="margin-top:24px;color:#555;font-size:12px">
        {link} &nbsp;·&nbsp; Threshold: {THRESHOLD:.0f}% &nbsp;·&nbsp;
        Databricks Cost Observability
      </p>
    </body></html>"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Fetching spend data...")
    costs    = fetch_daily_spend()
    sku_rows = fetch_sku_wow()

    if not costs:
        print("No billing data found. Exiting.")
        sys.exit(0)

    baseline = [c for _, c in costs[:-7]] if len(costs) > 7 else [c for _, c in costs]
    baseline_avg = sum(baseline) / len(baseline) if baseline else 0
    recent   = costs[-7:] if len(costs) >= 7 else costs
    recent_avg = sum(c for _, c in recent) / len(recent) if recent else 0

    alerts = detect_alerts(costs, sku_rows)
    print(f"Detected {len(alerts)} alert(s).")

    if not alerts:
        print("Spend is within threshold. No email sent.")
        sys.exit(0)

    body = build_email(costs, alerts, baseline_avg, recent_avg)

    recipients = [e.strip() for e in ALERT_TO.split(",") if e.strip()]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"⚠️ Databricks Cost Alert — {len(alerts)} issue{'s' if len(alerts) != 1 else ''} detected"
    msg["From"]    = SMTP_USER
    msg["To"]      = ALERT_TO
    msg.attach(MIMEText(body, "html"))

    print(f"Sending email to {recipients}...")
    import ssl
    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())

    print("Done.")


if __name__ == "__main__":
    main()
