"""Centralised input validation — strict type/format enforcement for all user input.

Every function raises ValueError with a generic message on invalid input.
"""

import re
from datetime import date, timedelta
from typing import Optional

# ── Compiled patterns ─────────────────────────────────────────────────────────

# YYYY-MM-DD, digits only
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Databricks workspace IDs: numeric or alphanumeric with hyphens/underscores
_WORKSPACE_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")

# SKU names: uppercase alphanumeric with underscores
_SKU_RE = re.compile(r"^[A-Z0-9_]{1,128}$")

# Azure tag keys: letters, digits, spaces, dots, colons, hyphens, underscores
_TAG_KEY_RE = re.compile(r"^[A-Za-z0-9 _\-\.\:]{1,128}$")

# Tag values: printable ASCII, no control chars, reasonable length
_TAG_VALUE_RE = re.compile(r"^[\x20-\x7E]{0,256}$")

# SQL identifiers (catalog, schema, table names): letters, digits, underscores, hyphens
# NO backticks, quotes, semicolons, spaces, or special chars
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_\-]{1,255}$")

# User identity: Databricks user emails or service principal IDs
# Allows: email addresses, numeric IDs with @ for SPs (e.g. "12345@67890")
_USER_IDENTITY_RE = re.compile(
    r"^[a-z0-9][a-z0-9._\-+]{0,127}@[a-z0-9][a-z0-9.\-]{0,127}$"
)

# Strict email: must have a real TLD (≥2 chars), no consecutive dots
_EMAIL_RE = re.compile(
    r"^[a-z0-9][a-z0-9._+\-]{0,127}@[a-z0-9][a-z0-9.\-]{0,126}\.[a-z]{2,}$"
)

# Valid dashboard tab names (must stay in sync with frontend tab IDs)
_VALID_TABS = frozenset({
    "executive", "cost", "ml-cost", "audit", "compute", "query",
    "ai", "access", "lakeflow", "storage", "lineage", "governance",
    "cluster-health", "marketplace", "platform-ops",
})

# Subscription IDs: UUID format or alphanumeric
_SUBSCRIPTION_RE = re.compile(r"^[A-Za-z0-9\-]{1,64}$")

# Resource group names: letters, digits, hyphens, underscores, periods, parens
_RESOURCE_GROUP_RE = re.compile(r"^[A-Za-z0-9_\-\.()]{1,90}$")


# ── Max date range ────────────────────────────────────────────────────────────
_MAX_DATE_RANGE_DAYS = 365


# ── Validators ────────────────────────────────────────────────────────────────

def validate_date(value: str) -> str:
    """Validate YYYY-MM-DD date string. Rejects implausible dates."""
    if not isinstance(value, str) or not _DATE_RE.match(value):
        raise ValueError("Invalid date format")
    # Verify it's a real calendar date
    try:
        year, month, day = int(value[:4]), int(value[5:7]), int(value[8:10])
        d = date(year, month, day)
    except (ValueError, OverflowError):
        raise ValueError("Invalid date format")
    # Reject dates too far in the past or future
    today = date.today()
    if d > today + timedelta(days=1):
        raise ValueError("Invalid date format")
    if d < today - timedelta(days=_MAX_DATE_RANGE_DAYS * 2):
        raise ValueError("Invalid date format")
    return value


def validate_workspace_id(value: str) -> str:
    """Validate a Databricks workspace ID."""
    if not isinstance(value, str) or not _WORKSPACE_RE.match(value):
        raise ValueError("Invalid workspace ID")
    return value


def validate_sku(value: str) -> str:
    """Validate a Databricks SKU name."""
    if not isinstance(value, str) or not _SKU_RE.match(value):
        raise ValueError("Invalid SKU name")
    return value


def validate_tag_key(value: str) -> str:
    """Validate an Azure tag key."""
    if not isinstance(value, str) or not _TAG_KEY_RE.match(value):
        raise ValueError("Invalid tag key")
    return value


def escape_tag_value(value: str) -> str:
    """Validate and SQL-escape a tag value. Rejects control characters."""
    if not isinstance(value, str) or len(value) > 256:
        raise ValueError("Invalid tag value")
    if not _TAG_VALUE_RE.match(value):
        raise ValueError("Invalid tag value")
    return value.replace("'", "''")


def validate_sql_identifier(value: str) -> str:
    """Validate a SQL identifier (catalog, schema, table name).

    Strictly rejects anything that could break backtick-quoting:
    backticks, quotes, semicolons, spaces, and non-alphanumeric chars
    (except underscores and hyphens).
    """
    if not isinstance(value, str) or not _SQL_IDENTIFIER_RE.match(value):
        raise ValueError("Invalid SQL identifier")
    return value


def validate_user_identity(value: str) -> str:
    """Validate a user identity string (email or SP ID).

    Returns the lowercase, stripped value. Raises on non-email-like strings
    to prevent header injection or XSS via identity fields.
    """
    cleaned = value.strip().lower() if isinstance(value, str) else ""
    if not cleaned or not _USER_IDENTITY_RE.match(cleaned):
        raise ValueError("Invalid user identity")
    return cleaned


def validate_subscription_id(value: str) -> str:
    """Validate an Azure subscription ID (UUID)."""
    if not isinstance(value, str) or not _SUBSCRIPTION_RE.match(value):
        raise ValueError("Invalid subscription ID")
    return value


def validate_resource_group(value: str) -> str:
    """Validate an Azure resource group name."""
    if not isinstance(value, str) or not _RESOURCE_GROUP_RE.match(value):
        raise ValueError("Invalid resource group name")
    return value


def validate_email_address(value: str) -> str:
    """Validate a real email address (admin operations, not SP IDs).

    Stricter than validate_user_identity: requires a proper TLD and rejects
    numeric-ID-style strings and any input containing control characters.
    We reject rather than strip control chars so caller intent is unambiguous.
    """
    if not isinstance(value, str):
        raise ValueError("Invalid email address")
    if re.search(r"[\x00-\x1f\x7f]", value):
        raise ValueError("Invalid email address")
    cleaned = value.strip().lower()
    if not cleaned or not _EMAIL_RE.match(cleaned):
        raise ValueError("Invalid email address")
    return cleaned


def validate_threshold_pct(value: float) -> float:
    """Validate an alert threshold percentage.  Must be in [1.0, 100.0]."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        raise ValueError("Threshold must be a number")
    if not (1.0 <= v <= 100.0):
        raise ValueError("Threshold must be between 1 and 100")
    return v


def validate_tab_list(tabs: Optional[list]) -> list[str]:
    """Validate a list of dashboard tab names.

    Returns a de-duplicated, lower-cased list of known tab IDs.
    Raises ValueError on unknown names or oversized lists.
    """
    if tabs is None:
        return []
    if not isinstance(tabs, list) or len(tabs) > 50:
        raise ValueError("Invalid tab list")
    result = []
    for t in tabs:
        if not isinstance(t, str) or t.strip().lower() not in _VALID_TABS:
            raise ValueError(f"Unknown tab: {t!r}")
        result.append(t.strip().lower())
    return result


def validate_workspace_list(workspaces: Optional[list]) -> list[str]:
    """Validate a list of workspace IDs for permission config.

    Raises ValueError on invalid IDs or oversized lists.
    """
    if workspaces is None:
        return []
    if not isinstance(workspaces, list) or len(workspaces) > 200:
        raise ValueError("Invalid workspace list")
    result = []
    for w in workspaces:
        if not isinstance(w, str):
            raise ValueError("Invalid workspace ID")
        result.append(validate_workspace_id(w))
    return result
