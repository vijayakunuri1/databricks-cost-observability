"""
Setup mock system tables for free-tier Databricks workspaces.

Runs automatically during deployment when MOCK_MODE=true.
Uses the Databricks SQL statement execution API via the existing warehouse.
"""

import os
import sys
import time
import base64

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

HOST         = os.environ["DATABRICKS_HOST"]
TOKEN        = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "21f5bd20b7f44a51")

client = WorkspaceClient(host=HOST, token=TOKEN)


def run_sql(label: str, sql: str) -> None:
    resp = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )
    deadline = time.time() + 300
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            print(f"  TIMEOUT: {label}")
            return
        time.sleep(3)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state == StatementState.SUCCEEDED:
        print(f"  OK: {label}")
    else:
        err = getattr(resp.status.error, "message", str(resp.status.state))
        print(f"  SKIP ({err[:120]}): {label}")


STATEMENTS = [
    # ── Schemas ────────────────────────────────────────────────────────────────
    ("schema billing",   "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_billing"),
    ("schema access",    "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_access"),
    ("schema compute",   "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_compute"),
    ("schema lakeflow",  "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_lakeflow"),

    # ── system.billing.usage ───────────────────────────────────────────────────
    ("drop billing.usage", "DROP TABLE IF EXISTS workspace.mock_system_billing.usage"),
    ("create billing.usage", """
CREATE TABLE workspace.mock_system_billing.usage (
  record_id              STRING,
  account_id             STRING,
  workspace_id           STRING,
  sku_name               STRING,
  cloud                  STRING,
  usage_start_time       TIMESTAMP,
  usage_end_time         TIMESTAMP,
  usage_date             DATE,
  custom_tags            MAP<STRING, STRING>,
  usage_unit             STRING,
  usage_quantity         DOUBLE,
  usage_type             STRING,
  billing_origin_product STRING,
  record_type            STRING,
  ingestion_date         DATE,
  identity_metadata      STRUCT<run_as: STRING, created_by: STRING>,
  usage_metadata         STRUCT<cluster_id: STRING, warehouse_id: STRING, job_id: STRING,
                                job_run_id: STRING, dlt_pipeline_id: STRING, notebook_id: STRING,
                                endpoint_name: STRING, endpoint_id: STRING, run_id: STRING>
)"""),
    ("insert billing.usage", """
INSERT INTO workspace.mock_system_billing.usage VALUES
  ('r1','acc1','7474651834822727','STANDARD_ALL_PURPOSE_COMPUTE','AWS',
   current_timestamp()-INTERVAL 1 DAY, current_timestamp()-INTERVAL 1 DAY+INTERVAL 1 HOUR,
   current_date()-INTERVAL 1 DAY, map('team','engineering'),'DBU',3.5,'COMPUTE',
   'INTERACTIVE','ORIGINAL',current_date()-INTERVAL 1 DAY,
   named_struct('run_as','vijaymohan.akunuri@gmail.com','created_by','vijaymohan.akunuri@gmail.com'),
   named_struct('cluster_id','cls-001','warehouse_id','','job_id','','job_run_id','',
                'dlt_pipeline_id','','notebook_id','','endpoint_name','','endpoint_id','','run_id','')),
  ('r2','acc1','7474651834822727','SERVERLESS_SQL','AWS',
   current_timestamp()-INTERVAL 2 DAY, current_timestamp()-INTERVAL 2 DAY+INTERVAL 2 HOUR,
   current_date()-INTERVAL 2 DAY, map('team','analytics'),'DBU',1.2,'SQL',
   'SQL','ORIGINAL',current_date()-INTERVAL 2 DAY,
   named_struct('run_as','vijaymohan.akunuri@gmail.com','created_by','vijaymohan.akunuri@gmail.com'),
   named_struct('cluster_id','','warehouse_id','21f5bd20b7f44a51','job_id','','job_run_id','',
                'dlt_pipeline_id','','notebook_id','','endpoint_name','','endpoint_id','','run_id','')),
  ('r3','acc1','7474651834822727','JOBS_COMPUTE','AWS',
   current_timestamp()-INTERVAL 3 DAY, current_timestamp()-INTERVAL 3 DAY+INTERVAL 3 HOUR,
   current_date()-INTERVAL 3 DAY, map('team','data'),'DBU',0.9,'JOBS',
   'JOBS','ORIGINAL',current_date()-INTERVAL 3 DAY,
   named_struct('run_as','vijaymohan.akunuri@gmail.com','created_by','vijaymohan.akunuri@gmail.com'),
   named_struct('cluster_id','','warehouse_id','','job_id','job-001','job_run_id','run-001',
                'dlt_pipeline_id','','notebook_id','','endpoint_name','','endpoint_id','','run_id',''))
"""),

    # ── system.billing.list_prices ─────────────────────────────────────────────
    ("drop billing.list_prices", "DROP TABLE IF EXISTS workspace.mock_system_billing.list_prices"),
    ("create billing.list_prices", """
CREATE TABLE workspace.mock_system_billing.list_prices (
  price_start_time TIMESTAMP,
  price_end_time   TIMESTAMP,
  account_id       STRING,
  sku_name         STRING,
  cloud            STRING,
  currency_code    STRING,
  usage_unit       STRING,
  pricing          STRUCT<default: DOUBLE, promotional: DOUBLE, effective_list: DOUBLE>
)"""),
    ("insert billing.list_prices", """
INSERT INTO workspace.mock_system_billing.list_prices VALUES
  (current_timestamp()-INTERVAL 365 DAY,null,'acc1','STANDARD_ALL_PURPOSE_COMPUTE','AWS','USD','DBU',
   named_struct('default',0.55,'promotional',cast(null as double),'effective_list',0.55)),
  (current_timestamp()-INTERVAL 365 DAY,null,'acc1','SERVERLESS_SQL','AWS','USD','DBU',
   named_struct('default',0.70,'promotional',cast(null as double),'effective_list',0.70)),
  (current_timestamp()-INTERVAL 365 DAY,null,'acc1','JOBS_COMPUTE','AWS','USD','DBU',
   named_struct('default',0.15,'promotional',cast(null as double),'effective_list',0.15))
"""),

    # ── system.access.workspaces_latest ────────────────────────────────────────
    ("drop access.workspaces_latest", "DROP TABLE IF EXISTS workspace.mock_system_access.workspaces_latest"),
    ("create access.workspaces_latest", """
CREATE TABLE workspace.mock_system_access.workspaces_latest (
  account_id        STRING,
  workspace_id      STRING,
  workspace_name    STRING,
  workspace_url     STRING,
  workspace_status  STRING,
  cloud             STRING,
  region            STRING,
  pricing_tier      STRING,
  creation_time     TIMESTAMP
)"""),
    ("insert access.workspaces_latest", """
INSERT INTO workspace.mock_system_access.workspaces_latest VALUES
  ('acc1','7474651834822727','prod-workspace',
   'https://databricks-cost-observability-7474651834822727.aws.databricksapps.com',
   'RUNNING','AWS','us-east-1','PREMIUM',
   current_timestamp()-INTERVAL 90 DAY)
"""),

    # ── system.access.audit ────────────────────────────────────────────────────
    ("drop access.audit", "DROP TABLE IF EXISTS workspace.mock_system_access.audit"),
    ("create access.audit", """
CREATE TABLE workspace.mock_system_access.audit (
  account_id        STRING,
  workspace_id      STRING,
  version           STRING,
  event_time        TIMESTAMP,
  event_date        DATE,
  source_ip_address STRING,
  user_agent        STRING,
  session_id        STRING,
  user_identity     STRUCT<email: STRING, subjectName: STRING>,
  service_name      STRING,
  action_name       STRING,
  request_id        STRING,
  request_params    MAP<STRING, STRING>,
  response          STRUCT<statusCode: INT, errorMessage: STRING, result: STRING>,
  audit_level       STRING,
  event_id          STRING,
  identity_metadata STRUCT<run_by: STRING, run_as: STRING>
)"""),
    ("insert access.audit", """
INSERT INTO workspace.mock_system_access.audit VALUES
  ('acc1','7474651834822727','2.0',current_timestamp()-INTERVAL 1 DAY,current_date()-INTERVAL 1 DAY,
   '1.2.3.4','Databricks','session-1',
   named_struct('email','vijaymohan.akunuri@gmail.com','subjectName','vijaymohan.akunuri@gmail.com'),
   'accounts','login','req-1',map('user','vijaymohan.akunuri@gmail.com'),
   named_struct('statusCode',200,'errorMessage',null,'result','success'),
   'ACCOUNT_LEVEL','evt-1',
   named_struct('run_by','vijaymohan.akunuri@gmail.com','run_as','vijaymohan.akunuri@gmail.com'))
"""),

    # ── system.compute.clusters ────────────────────────────────────────────────
    ("drop compute.clusters", "DROP TABLE IF EXISTS workspace.mock_system_compute.clusters"),
    ("create compute.clusters", """
CREATE TABLE workspace.mock_system_compute.clusters (
  account_id               STRING,
  workspace_id             STRING,
  cluster_id               STRING,
  cluster_name             STRING,
  owned_by                 STRING,
  create_time              TIMESTAMP,
  delete_time              TIMESTAMP,
  driver_node_type         STRING,
  worker_node_type         STRING,
  worker_count             BIGINT,
  min_autoscale_workers    BIGINT,
  max_autoscale_workers    BIGINT,
  auto_termination_minutes BIGINT,
  enable_elastic_disk      BOOLEAN,
  tags                     MAP<STRING, STRING>,
  cluster_source           STRING,
  dbr_version              STRING,
  change_time              TIMESTAMP,
  change_date              DATE,
  data_security_mode       STRING
)"""),
    ("insert compute.clusters", """
INSERT INTO workspace.mock_system_compute.clusters VALUES
  ('acc1','7474651834822727','cls-001','my-cluster','vijaymohan.akunuri@gmail.com',
   current_timestamp()-INTERVAL 7 DAY,null,
   'i3.xlarge','i3.xlarge',2,1,4,120,true,
   map('team','engineering'),'UI','14.3.x-scala2.12',
   current_timestamp()-INTERVAL 1 DAY,current_date()-INTERVAL 1 DAY,'SINGLE_USER')
"""),

    # ── system.compute.warehouses ──────────────────────────────────────────────
    ("drop compute.warehouses", "DROP TABLE IF EXISTS workspace.mock_system_compute.warehouses"),
    ("create compute.warehouses", """
CREATE TABLE workspace.mock_system_compute.warehouses (
  account_id        STRING,
  workspace_id      STRING,
  warehouse_id      STRING,
  warehouse_name    STRING,
  warehouse_type    STRING,
  warehouse_size    STRING,
  min_clusters      INT,
  max_clusters      INT,
  auto_stop_minutes INT,
  change_time       TIMESTAMP,
  delete_time       TIMESTAMP
)"""),
    ("insert compute.warehouses", """
INSERT INTO workspace.mock_system_compute.warehouses VALUES
  ('acc1','7474651834822727','21f5bd20b7f44a51','Starter Warehouse',
   'PRO','Small',1,1,10,current_timestamp()-INTERVAL 1 DAY,null)
"""),

    # ── system.compute.node_timeline ──────────────────────────────────────────
    ("drop compute.node_timeline", "DROP TABLE IF EXISTS workspace.mock_system_compute.node_timeline"),
    ("create compute.node_timeline", """
CREATE TABLE workspace.mock_system_compute.node_timeline (
  account_id             STRING,
  workspace_id           STRING,
  cluster_id             STRING,
  node_id                STRING,
  instance_id            STRING,
  start_time             TIMESTAMP,
  end_time               TIMESTAMP,
  driver                 BOOLEAN,
  is_driver              BOOLEAN,
  cpu_user_percent       DOUBLE,
  cpu_system_percent     DOUBLE,
  cpu_wait_percent       DOUBLE,
  mem_used_percent       DOUBLE,
  mem_swap_percent       DOUBLE,
  network_sent_bytes     BIGINT,
  network_received_bytes BIGINT,
  node_type              STRING,
  private_ip             STRING,
  uptime_seconds         DOUBLE,
  num_task_slots         INT,
  avg_num_running_tasks  DOUBLE,
  avg_num_queued_tasks   DOUBLE
)"""),
    ("insert compute.node_timeline", """
INSERT INTO workspace.mock_system_compute.node_timeline VALUES
  ('acc1','7474651834822727','cls-001','node-001','i-abc123',
   current_timestamp()-INTERVAL 1 DAY,current_timestamp()-INTERVAL 1 DAY+INTERVAL 1 HOUR,
   false,false,42.5,8.2,1.1,61.3,0.0,1024000,2048000,'i3.xlarge','10.0.0.1',
   3600.0,8,3.2,0.1)
"""),

    # ── system.compute.cluster_events ─────────────────────────────────────────
    ("drop compute.cluster_events", "DROP TABLE IF EXISTS workspace.mock_system_compute.cluster_events"),
    ("create compute.cluster_events", """
CREATE TABLE workspace.mock_system_compute.cluster_events (
  account_id   STRING,
  workspace_id STRING,
  cluster_id   STRING,
  timestamp    TIMESTAMP,
  type         STRING,
  details      MAP<STRING, STRING>
)"""),

    # ── system.compute.warehouse_events ───────────────────────────────────────
    ("drop compute.warehouse_events", "DROP TABLE IF EXISTS workspace.mock_system_compute.warehouse_events"),
    ("create compute.warehouse_events", """
CREATE TABLE workspace.mock_system_compute.warehouse_events (
  account_id    STRING,
  workspace_id  STRING,
  warehouse_id  STRING,
  event_type    STRING,
  cluster_count INT,
  event_time    TIMESTAMP
)"""),

    # ── system.lakeflow.jobs ───────────────────────────────────────────────────
    ("drop lakeflow.jobs", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.jobs"),
    ("create lakeflow.jobs", """
CREATE TABLE workspace.mock_system_lakeflow.jobs (
  account_id        STRING,
  workspace_id      STRING,
  job_id            STRING,
  name              STRING,
  creator_user_name STRING,
  run_as_user_name  STRING,
  tags              MAP<STRING, STRING>,
  schedule          STRUCT<quartz_cron_expression: STRING, pause_status: STRING>,
  created_time      TIMESTAMP,
  change_time       TIMESTAMP,
  delete_time       TIMESTAMP
)"""),
    ("insert lakeflow.jobs", """
INSERT INTO workspace.mock_system_lakeflow.jobs VALUES
  ('acc1','7474651834822727','job-001','Daily ETL',
   'vijaymohan.akunuri@gmail.com','vijaymohan.akunuri@gmail.com',
   map('Env','prod'),
   named_struct('quartz_cron_expression','0 0 8 * * ?','pause_status','UNPAUSED'),
   current_timestamp()-INTERVAL 30 DAY,current_timestamp()-INTERVAL 1 DAY,null)
"""),

    # ── system.lakeflow.job_run_timeline ───────────────────────────────────────
    ("drop lakeflow.job_run_timeline", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.job_run_timeline"),
    ("create lakeflow.job_run_timeline", """
CREATE TABLE workspace.mock_system_lakeflow.job_run_timeline (
  account_id        STRING,
  workspace_id      STRING,
  job_id            STRING,
  run_id            STRING,
  period_start_time TIMESTAMP,
  period_end_time   TIMESTAMP,
  trigger_type      STRING,
  run_type          STRING,
  result_state      STRING,
  termination_code  STRING
)"""),
    ("insert lakeflow.job_run_timeline", """
INSERT INTO workspace.mock_system_lakeflow.job_run_timeline VALUES
  ('acc1','7474651834822727','job-001','run-001',
   current_timestamp()-INTERVAL 1 DAY,current_timestamp()-INTERVAL 1 DAY+INTERVAL 30 MINUTE,
   'SCHEDULED','JOB_RUN','SUCCEEDED','SUCCESS'),
  ('acc1','7474651834822727','job-001','run-002',
   current_timestamp()-INTERVAL 2 DAY,current_timestamp()-INTERVAL 2 DAY+INTERVAL 25 MINUTE,
   'SCHEDULED','JOB_RUN','SUCCEEDED','SUCCESS')
"""),

    # ── system.lakeflow.job_tasks ──────────────────────────────────────────────
    ("drop lakeflow.job_tasks", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.job_tasks"),
    ("create lakeflow.job_tasks", """
CREATE TABLE workspace.mock_system_lakeflow.job_tasks (
  account_id   STRING,
  workspace_id STRING,
  job_id       STRING,
  task_key     STRING,
  change_time  TIMESTAMP,
  delete_time  TIMESTAMP
)"""),
    ("insert lakeflow.job_tasks", """
INSERT INTO workspace.mock_system_lakeflow.job_tasks VALUES
  ('acc1','7474651834822727','job-001','task-1',current_timestamp()-INTERVAL 30 DAY,null)
"""),

    # ── system.lakeflow.job_task_run_timeline ──────────────────────────────────
    ("drop lakeflow.job_task_run_timeline", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.job_task_run_timeline"),
    ("create lakeflow.job_task_run_timeline", """
CREATE TABLE workspace.mock_system_lakeflow.job_task_run_timeline (
  account_id        STRING,
  workspace_id      STRING,
  job_id            STRING,
  run_id            STRING,
  task_key          STRING,
  period_start_time TIMESTAMP,
  period_end_time   TIMESTAMP,
  result_state      STRING,
  termination_code  STRING
)"""),
    ("insert lakeflow.job_task_run_timeline", """
INSERT INTO workspace.mock_system_lakeflow.job_task_run_timeline VALUES
  ('acc1','7474651834822727','job-001','run-001','task-1',
   current_timestamp()-INTERVAL 1 DAY,
   current_timestamp()-INTERVAL 1 DAY+INTERVAL 30 MINUTE,
   'SUCCEEDED','SUCCESS')
"""),

    # ── system.lakeflow.pipelines ──────────────────────────────────────────────
    ("drop lakeflow.pipelines", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.pipelines"),
    ("create lakeflow.pipelines", """
CREATE TABLE workspace.mock_system_lakeflow.pipelines (
  account_id        STRING,
  workspace_id      STRING,
  pipeline_id       STRING,
  name              STRING,
  creator_user_name STRING,
  run_as_user_name  STRING,
  channel           STRING,
  edition           STRING,
  created_time      TIMESTAMP,
  change_time       TIMESTAMP,
  delete_time       TIMESTAMP
)"""),

    # ── system.lakeflow.pipeline_update_timeline ───────────────────────────────
    ("drop lakeflow.pipeline_update_timeline", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.pipeline_update_timeline"),
    ("create lakeflow.pipeline_update_timeline", """
CREATE TABLE workspace.mock_system_lakeflow.pipeline_update_timeline (
  account_id        STRING,
  workspace_id      STRING,
  pipeline_id       STRING,
  update_id         STRING,
  result_state      STRING,
  period_start_time TIMESTAMP,
  period_end_time   TIMESTAMP
)"""),

    # ── New schemas ────────────────────────────────────────────────────────────
    ("schema query",              "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_query"),
    ("schema ai_gateway",         "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_ai_gateway"),
    ("schema serving",            "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_serving"),
    ("schema mlflow",             "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_mlflow"),
    ("schema storage",            "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_storage"),
    ("schema information_schema", "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_information_schema"),
    ("schema networking",         "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_networking"),
    ("schema lakeview",           "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_lakeview"),
    ("schema dashboards",         "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_dashboards"),
    ("schema marketplace",        "CREATE SCHEMA IF NOT EXISTS workspace.mock_system_marketplace"),

    # ── system.query.history ───────────────────────────────────────────────────
    ("drop query.history", "DROP TABLE IF EXISTS workspace.mock_system_query.history"),
    ("create query.history", """
CREATE TABLE workspace.mock_system_query.history (
  statement_id    STRING,
  executed_by     STRING,
  executed_as     STRING,
  compute         STRUCT<warehouse_id: STRING>,
  statement_type  STRING,
  statement_text  STRING,
  start_time      TIMESTAMP,
  total_duration_ms BIGINT,
  read_bytes      BIGINT,
  read_rows       BIGINT,
  error_message   STRING
)"""),
    ("insert query.history", """
INSERT INTO workspace.mock_system_query.history VALUES
  ('stmt-001','vijaymohan.akunuri@gmail.com','vijaymohan.akunuri@gmail.com',
   named_struct('warehouse_id','21f5bd20b7f44a51'),'SELECT',
   'SELECT * FROM workspace.mock_system_billing.usage LIMIT 100',
   current_timestamp()-INTERVAL 1 DAY,1850,104857600,50000,null),
  ('stmt-002','vijaymohan.akunuri@gmail.com','vijaymohan.akunuri@gmail.com',
   named_struct('warehouse_id','21f5bd20b7f44a51'),'SELECT',
   'SELECT sku_name, SUM(usage_quantity) FROM workspace.mock_system_billing.usage GROUP BY 1',
   current_timestamp()-INTERVAL 2 DAY,4200,524288000,200000,null),
  ('stmt-003','vijaymohan.akunuri@gmail.com','vijaymohan.akunuri@gmail.com',
   named_struct('warehouse_id','21f5bd20b7f44a51'),'SELECT',
   'SELECT COUNT(*) FROM workspace.mock_system_access.audit',
   current_timestamp()-INTERVAL 3 DAY,320,1048576,100,null)
"""),

    # ── system.ai_gateway.usage ────────────────────────────────────────────────
    ("drop ai_gateway.usage", "DROP TABLE IF EXISTS workspace.mock_system_ai_gateway.usage"),
    ("create ai_gateway.usage", """
CREATE TABLE workspace.mock_system_ai_gateway.usage (
  route_name            STRING,
  event_time            TIMESTAMP,
  total_token_count     BIGINT,
  input_token_count     BIGINT,
  output_token_count    BIGINT,
  execution_duration_ms DOUBLE,
  requester             STRING
)"""),
    ("insert ai_gateway.usage", """
INSERT INTO workspace.mock_system_ai_gateway.usage VALUES
  ('databricks-meta-llama',current_timestamp()-INTERVAL 1 DAY,1500,1000,500,320.5,
   'vijaymohan.akunuri@gmail.com'),
  ('databricks-meta-llama',current_timestamp()-INTERVAL 2 DAY,2200,1400,800,410.0,
   'vijaymohan.akunuri@gmail.com')
"""),

    # ── system.serving.served_entities ────────────────────────────────────────
    ("drop serving.served_entities", "DROP TABLE IF EXISTS workspace.mock_system_serving.served_entities"),
    ("create serving.served_entities", """
CREATE TABLE workspace.mock_system_serving.served_entities (
  endpoint_name     STRING,
  served_entity_name STRING,
  entity_type       STRING,
  workspace_id      STRING,
  change_time       TIMESTAMP
)"""),
    ("insert serving.served_entities", """
INSERT INTO workspace.mock_system_serving.served_entities VALUES
  ('llm-endpoint','databricks-meta-llama','FOUNDATION_MODEL','7474651834822727',
   current_timestamp()-INTERVAL 10 DAY)
"""),

    # ── system.serving.endpoint_usage ─────────────────────────────────────────
    ("drop serving.endpoint_usage", "DROP TABLE IF EXISTS workspace.mock_system_serving.endpoint_usage"),
    ("create serving.endpoint_usage", """
CREATE TABLE workspace.mock_system_serving.endpoint_usage (
  served_entity_name STRING,
  request_time       TIMESTAMP,
  total_token_count  BIGINT,
  status_code        INT
)"""),
    ("insert serving.endpoint_usage", """
INSERT INTO workspace.mock_system_serving.endpoint_usage VALUES
  ('databricks-meta-llama',current_timestamp()-INTERVAL 1 DAY,1500,200),
  ('databricks-meta-llama',current_timestamp()-INTERVAL 2 DAY,2200,200)
"""),

    # ── system.mlflow.experiments_latest ──────────────────────────────────────
    ("drop mlflow.experiments_latest", "DROP TABLE IF EXISTS workspace.mock_system_mlflow.experiments_latest"),
    ("create mlflow.experiments_latest", """
CREATE TABLE workspace.mock_system_mlflow.experiments_latest (
  experiment_id    STRING,
  name             STRING,
  lifecycle_stage  STRING,
  creation_time    BIGINT,
  last_update_time BIGINT
)"""),
    ("insert mlflow.experiments_latest", """
INSERT INTO workspace.mock_system_mlflow.experiments_latest VALUES
  ('exp-001','Cost Forecasting Model','active',
   UNIX_TIMESTAMP(current_timestamp()-INTERVAL 30 DAY)*1000,
   UNIX_TIMESTAMP(current_timestamp()-INTERVAL 1 DAY)*1000),
  ('exp-002','Anomaly Detection','active',
   UNIX_TIMESTAMP(current_timestamp()-INTERVAL 60 DAY)*1000,
   UNIX_TIMESTAMP(current_timestamp()-INTERVAL 5 DAY)*1000)
"""),

    # ── system.mlflow.runs_latest ─────────────────────────────────────────────
    ("drop mlflow.runs_latest", "DROP TABLE IF EXISTS workspace.mock_system_mlflow.runs_latest"),
    ("create mlflow.runs_latest", """
CREATE TABLE workspace.mock_system_mlflow.runs_latest (
  experiment_id STRING,
  run_id        STRING,
  status        STRING,
  start_time    TIMESTAMP,
  end_time      TIMESTAMP,
  user_id       STRING
)"""),
    ("insert mlflow.runs_latest", """
INSERT INTO workspace.mock_system_mlflow.runs_latest VALUES
  ('exp-001','run-001','FINISHED',
   current_timestamp()-INTERVAL 2 DAY,
   current_timestamp()-INTERVAL 2 DAY+INTERVAL 20 MINUTE,
   'vijaymohan.akunuri@gmail.com'),
  ('exp-001','run-002','FINISHED',
   current_timestamp()-INTERVAL 1 DAY,
   current_timestamp()-INTERVAL 1 DAY+INTERVAL 15 MINUTE,
   'vijaymohan.akunuri@gmail.com'),
  ('exp-002','run-003','FAILED',
   current_timestamp()-INTERVAL 3 DAY,
   current_timestamp()-INTERVAL 3 DAY+INTERVAL 5 MINUTE,
   'vijaymohan.akunuri@gmail.com')
"""),

    # ── system.mlflow.registered_models_latest ────────────────────────────────
    ("drop mlflow.registered_models_latest", "DROP TABLE IF EXISTS workspace.mock_system_mlflow.registered_models_latest"),
    ("create mlflow.registered_models_latest", """
CREATE TABLE workspace.mock_system_mlflow.registered_models_latest (
  name                   STRING,
  creation_timestamp     BIGINT,
  last_updated_timestamp BIGINT,
  user_id                STRING
)"""),
    ("insert mlflow.registered_models_latest", """
INSERT INTO workspace.mock_system_mlflow.registered_models_latest VALUES
  ('cost-forecast-model',
   UNIX_TIMESTAMP(current_timestamp()-INTERVAL 25 DAY)*1000,
   UNIX_TIMESTAMP(current_timestamp()-INTERVAL 2 DAY)*1000,
   'vijaymohan.akunuri@gmail.com')
"""),

    # ── system.access.table_lineage ───────────────────────────────────────────
    ("drop access.table_lineage", "DROP TABLE IF EXISTS workspace.mock_system_access.table_lineage"),
    ("create access.table_lineage", """
CREATE TABLE workspace.mock_system_access.table_lineage (
  source_table_catalog STRING,
  source_table_schema  STRING,
  source_table_name    STRING,
  target_table_catalog STRING,
  target_table_schema  STRING,
  target_table_name    STRING,
  event_time           TIMESTAMP
)"""),
    ("insert access.table_lineage", """
INSERT INTO workspace.mock_system_access.table_lineage VALUES
  ('workspace','mock_system_billing','usage',
   'workspace','default','billing_summary',
   current_timestamp()-INTERVAL 1 DAY),
  ('workspace','mock_system_billing','list_prices',
   'workspace','default','billing_summary',
   current_timestamp()-INTERVAL 1 DAY)
"""),

    # ── system.access.column_lineage ──────────────────────────────────────────
    ("drop access.column_lineage", "DROP TABLE IF EXISTS workspace.mock_system_access.column_lineage"),
    ("create access.column_lineage", """
CREATE TABLE workspace.mock_system_access.column_lineage (
  source_table_catalog STRING,
  source_table_schema  STRING,
  source_table_name    STRING,
  source_column_name   STRING,
  target_table_catalog STRING,
  target_table_schema  STRING,
  target_table_name    STRING,
  target_column_name   STRING,
  event_time           TIMESTAMP
)"""),
    ("insert access.column_lineage", """
INSERT INTO workspace.mock_system_access.column_lineage VALUES
  ('workspace','mock_system_billing','usage','usage_quantity',
   'workspace','default','billing_summary','total_dbus',
   current_timestamp()-INTERVAL 1 DAY)
"""),

    # ── system.storage.predictive_optimization_operations_history ─────────────
    ("drop storage.predictive_opt", "DROP TABLE IF EXISTS workspace.mock_system_storage.predictive_optimization_operations_history"),
    ("create storage.predictive_opt", """
CREATE TABLE workspace.mock_system_storage.predictive_optimization_operations_history (
  catalog_name      STRING,
  schema_name       STRING,
  table_name        STRING,
  operation_type    STRING,
  operation_status  STRING,
  start_time        TIMESTAMP,
  end_time          TIMESTAMP,
  operation_metrics MAP<STRING, STRING>
)"""),
    ("insert storage.predictive_opt", """
INSERT INTO workspace.mock_system_storage.predictive_optimization_operations_history VALUES
  ('workspace','mock_system_billing','usage','OPTIMIZE','SUCCEEDED',
   current_timestamp()-INTERVAL 1 DAY,
   current_timestamp()-INTERVAL 1 DAY+INTERVAL 5 MINUTE,
   map('files_removed','12','bytes_removed','524288'))
"""),

    # ── system.information_schema.tables ──────────────────────────────────────
    ("drop information_schema.tables", "DROP TABLE IF EXISTS workspace.mock_system_information_schema.tables"),
    ("create information_schema.tables", """
CREATE TABLE workspace.mock_system_information_schema.tables (
  table_catalog STRING,
  table_schema  STRING,
  table_name    STRING,
  table_type    STRING,
  created       TIMESTAMP
)"""),
    ("insert information_schema.tables", """
INSERT INTO workspace.mock_system_information_schema.tables VALUES
  ('workspace','mock_system_billing','usage','MANAGED',current_timestamp()-INTERVAL 90 DAY),
  ('workspace','mock_system_billing','list_prices','MANAGED',current_timestamp()-INTERVAL 90 DAY),
  ('workspace','mock_system_access','audit','MANAGED',current_timestamp()-INTERVAL 90 DAY),
  ('workspace','mock_system_compute','clusters','MANAGED',current_timestamp()-INTERVAL 90 DAY),
  ('workspace','default','billing_summary','MANAGED',current_timestamp()-INTERVAL 30 DAY)
"""),
]


def main():
    print(f"Setting up mock tables on {HOST} using warehouse {WAREHOUSE_ID}\n")
    errors = 0
    for label, sql in STATEMENTS:
        try:
            run_sql(label, sql.strip())
        except Exception as exc:
            print(f"  ERROR: {label} — {exc}")
            errors += 1

    print(f"\nDone. {len(STATEMENTS)} statements, {errors} errors.")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
