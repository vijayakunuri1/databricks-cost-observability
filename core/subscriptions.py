"""
Static mapping of cloud subscription/account IDs to display names.

Update this mapping when subscriptions or accounts are added or renamed.
Works with Azure subscription IDs, AWS account IDs, or any string identifier.
"""

SUBSCRIPTION_MAP: dict[str, str] = {
    # Add your cloud subscription/account ID → display name mappings here.
    # Examples:
    #   "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx": "Production"
    #   "123456789012": "Dev Account"   # AWS account ID
}


def resolve_subscription(sub_id: str) -> str:
    """Return display name for a subscription ID, or the ID itself if unknown."""
    if not sub_id:
        return ""
    return SUBSCRIPTION_MAP.get(sub_id.strip().lower(),
           SUBSCRIPTION_MAP.get(sub_id.strip(), sub_id))


# Also build a reverse map for filter lookups (name → id)
_REVERSE_MAP: dict[str, str] = {
    name.lower(): sid for sid, name in SUBSCRIPTION_MAP.items()
}


def resolve_subscription_id(name_or_id: str) -> str:
    """Given a display name or ID, return the subscription ID."""
    if not name_or_id:
        return ""
    val = name_or_id.strip()
    # Already an ID?
    if val.lower() in {s.lower() for s in SUBSCRIPTION_MAP}:
        return val
    # Resolve from name
    return _REVERSE_MAP.get(val.lower(), val)
