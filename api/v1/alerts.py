"""Cost spike alert endpoints — summary, test email, config check."""

from __future__ import annotations

import html
import logging
import os
import re
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from core.dependencies import get_workspace_client, require_admin
from core.validators import validate_threshold_pct, validate_email_address

router = APIRouter(prefix="/alerts", tags=["Alerts"])
_log   = logging.getLogger(__name__)

# Allowed SMTP ports (reject arbitrary ports to prevent SSRF)
_ALLOWED_SMTP_PORTS = {25, 465, 587, 2525}
# SMTP hosts must be a valid domain (prevent SSRF to internal IPs)
_SMTP_HOST_RE = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$")


def _validate_smtp_config() -> tuple[str, int, str, str, list[str]]:
    """Validate and return (host, port, user, password, recipients). Raises HTTPException on invalid config."""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    try:
        smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    except ValueError:
        raise HTTPException(status_code=400, detail="SMTP_PORT must be a valid integer")
    if smtp_port not in _ALLOWED_SMTP_PORTS:
        raise HTTPException(status_code=400, detail=f"SMTP_PORT must be one of {sorted(_ALLOWED_SMTP_PORTS)}")
    if not _SMTP_HOST_RE.match(smtp_host):
        raise HTTPException(status_code=400, detail="SMTP_HOST must be a valid domain name")
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASSWORD", "")
    alert_to_raw = os.environ.get("ALERT_EMAIL_TO", "")

    missing = [k for k, v in {"SMTP_USER": smtp_user, "SMTP_PASSWORD": smtp_pass, "ALERT_EMAIL_TO": alert_to_raw}.items() if not v]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing env vars: {', '.join(missing)}")

    # Validate each recipient email
    recipients = []
    for addr in alert_to_raw.split(","):
        addr = addr.strip()
        if addr:
            try:
                recipients.append(validate_email_address(addr))
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid recipient email format")
    if not recipients:
        raise HTTPException(status_code=400, detail="ALERT_EMAIL_TO has no valid addresses")

    return smtp_host, smtp_port, smtp_user, smtp_pass, recipients


def _send_email(smtp_host: str, smtp_port: int, smtp_user: str, smtp_pass: str,
                recipients: list[str], subject: str, html_body: str) -> None:
    """Send email with enforced TLS. Raises HTTPException on failure."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject[:200]  # Truncate to prevent header injection
    msg["From"] = smtp_user
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls(context=ctx)
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, msg.as_string())
    except smtplib.SMTPAuthenticationError:
        _log.error("SMTP_AUTH_FAILED host=%s user=%s", smtp_host, smtp_user)
        raise HTTPException(status_code=500, detail="SMTP authentication failed")
    except smtplib.SMTPException as exc:
        _log.error("SMTP_ERROR host=%s error=%s", smtp_host, exc)
        raise HTTPException(status_code=500, detail="Email delivery failed")
    except Exception as exc:
        _log.error("EMAIL_SEND_FAILED host=%s error=%s", smtp_host, exc)
        raise HTTPException(status_code=500, detail="Email delivery failed")


def _svc(request: Request):
    from services.alert_service import AlertService
    return AlertService(get_workspace_client())


# ── Read endpoints (any authenticated user) ──────────────────────────────────

@router.get("/summary")
def get_alert_summary(
    threshold_pct: float = Query(default=20.0, ge=1.0, le=100.0),
    svc=Depends(_svc),
):
    """Return current spike detection results."""
    try:
        pct = validate_threshold_pct(threshold_pct)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return svc.detect_spikes(threshold_pct=pct)


# ── Admin-only endpoints ──────────────────────────────────────────────────────

@router.get("/config")
def get_alert_config(request: Request):
    """Return which alert env vars are configured (admin only, values masked)."""
    require_admin(request)
    keys = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD",
            "ALERT_EMAIL_TO", "ALERT_THRESHOLD_PCT", "APP_URL"]
    return {
        k: ("✓ set" if os.environ.get(k) else "✗ not set")
        for k in keys
    }


@router.post("/test-email")
def send_test_email(request: Request):
    """Send a test alert email to verify SMTP config (admin only)."""
    require_admin(request)
    smtp_host, smtp_port, smtp_user, smtp_pass, recipients = _validate_smtp_config()

    body = """
    <html><body style="font-family:Arial,sans-serif;padding:24px;background:#1a1a2e;color:#e0e0e0">
      <h2 style="color:#FF3621">&#x2705; Test Alert &#x2014; Databricks Cost Observability</h2>
      <p>SMTP is configured correctly. You will receive real alerts when cost spikes are detected.</p>
      <p style="color:#888;font-size:12px">Databricks Cost Observability &#x2014; Alert System</p>
    </body></html>
    """
    _send_email(smtp_host, smtp_port, smtp_user, smtp_pass, recipients,
                "Test Alert — Databricks Cost Observability", body)
    return {"status": "sent", "to": recipients[0]}


def _build_email_html(result: dict, threshold_pct: float, app_url: str) -> str:
    """Render alert summary as an HTML email body."""
    s   = result.get("summary", {})
    pct = s.get("pct_vs_baseline", 0)
    pct_color = "#ff4444" if pct > 0 else "#00cc88"
    arrow     = "&#x2191;" if pct > 0 else "&#x2193;"

    rows_html = ""
    for a in result.get("alerts", []):
        color = "#ff4444" if a["severity"] == "HIGH" else "#ffaa00"
        rows_html += f"""
        <tr>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;
                     color:{color};font-weight:bold;white-space:nowrap">{a['severity']}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-weight:500">
            {html.escape(a['title'])}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;color:#aaa">
            {html.escape(a['detail'])}</td>
        </tr>"""

    link = (f'<a href="{html.escape(app_url)}" '
            f'style="color:#FF3621;text-decoration:none">Open Dashboard &#x2192;</a>'
            if app_url else "")

    return f"""
    <html><body style="background:#13131f;color:#e0e0e0;font-family:Arial,sans-serif;
                       padding:32px;max-width:700px;margin:0 auto">
      <div style="margin-bottom:24px">
        <span style="color:#FF3621;font-size:22px;font-weight:bold">&#x26A0; Databricks Cost Alert</span>
        <span style="color:#555;font-size:13px;margin-left:12px">{result.get('generated_at','')}</span>
      </div>

      <table style="width:100%;border-spacing:12px;border-collapse:separate;margin-bottom:24px">
        <tr>
          <td style="background:#1e1e30;border-radius:8px;padding:16px;text-align:center">
            <div style="color:#888;font-size:11px;letter-spacing:1px">30-DAY SPEND</div>
            <div style="font-size:26px;font-weight:bold;margin-top:4px">
              ${s.get('total_30d', 0):,.2f}</div>
          </td>
          <td style="background:#1e1e30;border-radius:8px;padding:16px;text-align:center">
            <div style="color:#888;font-size:11px;letter-spacing:1px">7-DAY AVG/DAY</div>
            <div style="font-size:26px;font-weight:bold;margin-top:4px">
              ${s.get('recent_avg_daily', 0):,.2f}</div>
            <div style="color:{pct_color};font-size:13px">
              {arrow}{abs(pct):.1f}% vs baseline</div>
          </td>
          <td style="background:#1e1e30;border-radius:8px;padding:16px;text-align:center">
            <div style="color:#888;font-size:11px;letter-spacing:1px">ALERTS</div>
            <div style="font-size:26px;font-weight:bold;color:#ff4444;margin-top:4px">
              {result.get('alert_count', 0)}</div>
            <div style="color:#888;font-size:13px">threshold: {threshold_pct:.0f}%</div>
          </td>
        </tr>
      </table>

      <table style="width:100%;border-collapse:collapse;background:#1e1e30;border-radius:8px;
                    overflow:hidden">
        <thead>
          <tr style="background:#252540">
            <th style="padding:10px 8px;text-align:left;color:#888;font-size:12px">SEVERITY</th>
            <th style="padding:10px 8px;text-align:left;color:#888;font-size:12px">ALERT</th>
            <th style="padding:10px 8px;text-align:left;color:#888;font-size:12px">DETAIL</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>

      <p style="margin-top:24px;color:#555;font-size:12px">
        {link} &nbsp;&#xB7;&nbsp; Threshold: {threshold_pct:.0f}% above baseline
        &nbsp;&#xB7;&nbsp; Databricks Cost Observability
      </p>
    </body></html>"""


@router.post("/send-now")
def send_alerts_now(
    request: Request,
    threshold_pct: float = Query(default=20.0, ge=1.0, le=100.0),
    svc=Depends(_svc),
):
    """Trigger an immediate alert email (admin only — only sends if alerts exist)."""
    require_admin(request)
    try:
        pct = validate_threshold_pct(threshold_pct)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    smtp_host, smtp_port, smtp_user, smtp_pass, recipients = _validate_smtp_config()
    app_url = os.environ.get("APP_URL", "")

    result = svc.detect_spikes(threshold_pct=pct)

    if not result["has_alerts"]:
        return {"status": "skipped", "reason": "No alerts detected — spend is within threshold"}

    body  = _build_email_html(result, pct, app_url)
    count = result["alert_count"]
    subject = f"Databricks Cost Alert — {count} issue{'s' if count != 1 else ''} detected"

    _send_email(smtp_host, smtp_port, smtp_user, smtp_pass, recipients, subject, body)

    return {
        "status":      "sent",
        "to":          recipients[0],
        "alert_count": count,
        "alerts":      [a["title"] for a in result["alerts"]],
    }
