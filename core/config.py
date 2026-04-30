from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Automatically injected by Databricks Apps runtime
    databricks_host: str = Field(default="")
    databricks_client_id: str = Field(default="")
    databricks_client_secret: str = Field(default="")
    databricks_token: str = Field(default="")
    databricks_app_port: int = Field(default=8000)
    databricks_app_name: str = Field(default="local")
    databricks_workspace_id: str = Field(default="")
    databricks_account_id: str = Field(default="")

    # Bound via app.yaml
    databricks_warehouse_id: str = Field(default="")
    uc_catalog_name: str = Field(default="workspace")

    # Application config
    environment: str = Field(default="development")
    log_level: str = Field(default="info")
    app_version: str = Field(default="1.1.0")

    # Access control — comma-separated list of allowed email addresses.
    # Enforced via the X-Forwarded-User header injected by Databricks Apps.
    # Leave empty to allow all authenticated workspace users.
    allowed_users: str = Field(default="")

    # Workspace scoping — comma-separated list of workspace IDs users may query.
    # When empty, billing queries are scoped to the current workspace only.
    # Set to "*" to allow querying across all workspaces (admin mode).
    allowed_workspace_ids: str = Field(default="")

    # ── Cloud Platform Cost credentials ───────────────────────────────────────
    # Azure Cost Management (Service Principal with Cost Management Reader role)
    azure_tenant_id: str = Field(default="")
    azure_client_id: str = Field(default="")
    azure_client_secret: str = Field(default="")
    azure_subscription_ids: str = Field(default="")   # comma-separated GUIDs

    # AWS Cost Explorer (IAM user/role with ce:GetCostAndUsage permission)
    aws_access_key_id: str = Field(default="")
    aws_secret_access_key: str = Field(default="")
    aws_account_ids: str = Field(default="")           # comma-separated account IDs

    # GCP Cloud Billing (Service Account JSON with billing.viewer role)
    gcp_service_account_json: str = Field(default="")  # full JSON as a single-line string
    gcp_project_ids: str = Field(default="")           # comma-separated project IDs
    gcp_billing_account_id: str = Field(default="")    # e.g. "012345-ABCDEF-012345"

    # Per-user tab and workspace restrictions — JSON string mapping email → config.
    # Example: {"alice@example.com": {"tabs": ["executive", "cost"], "workspaces": ["ws-123"]}}
    # Leave empty to give all users access to all tabs and workspaces.
    user_configs: str = Field(default="")

    # Comma-separated emails of users who can access the Admin panel and manage permissions.
    # Admins always see all tabs and all data regardless of user_configs entries.
    admin_users: str = Field(default="")

    # JSON mapping of Databricks numeric user IDs to email addresses.
    # Use this when Databricks Apps injects a numeric ID instead of an email.
    # Example: {"123456789@987654321": "alice@example.com"}
    user_id_map: str = Field(default="")

    @property
    def allowed_users_set(self) -> set[str]:
        return {e.strip().lower() for e in self.allowed_users.split(",") if e.strip()}

    @property
    def allowed_workspace_ids_set(self) -> set[str]:
        """Return the set of workspace IDs the app may query.
        Empty set means 'current workspace only'. {"*"} means unrestricted.
        """
        return {w.strip() for w in self.allowed_workspace_ids.split(",") if w.strip()}

    @property
    def admin_users_set(self) -> set[str]:
        return {e.strip().lower() for e in self.admin_users.split(",") if e.strip()}

    @property
    def user_configs_dict(self) -> dict:
        """Parse USER_CONFIGS JSON → dict. Returns {} on empty or invalid JSON."""
        import json
        if not self.user_configs.strip():
            return {}
        try:
            return json.loads(self.user_configs)
        except (ValueError, TypeError):
            return {}

    @property
    def user_id_map_dict(self) -> dict:
        """Parse USER_ID_MAP JSON → dict mapping numeric IDs to emails."""
        import json
        if not self.user_id_map.strip():
            return {}
        try:
            return {k.lower().strip(): v.lower().strip() for k, v in json.loads(self.user_id_map).items()}
        except (ValueError, TypeError):
            return {}

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
