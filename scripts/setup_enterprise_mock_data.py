"""
Enterprise-scale mock data generator for Databricks Cost Observability.

Generates 3 years of realistic data modeled after large financial institutions
(Chase, BOFA scale):
  - ~$5M annual Databricks spend (forecast)
  - 1,000+ active users across multiple teams/departments
  - Multiple workspaces, clusters, warehouses, jobs, ML experiments
  - Realistic growth patterns, seasonality, and cost distribution

Runs via Databricks SQL statement execution API.
"""

import os
import sys
import time
import random
import hashlib
from datetime import datetime, timedelta
from typing import List, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "21f5bd20b7f44a51")

client = WorkspaceClient(host=HOST, token=TOKEN)

# ─── Constants ────────────────────────────────────────────────────────────────

ACCOUNT_ID = "acc-chase-001"
# 5 workspaces representing different LOBs
WORKSPACES = [
    ("ws-100001", "prod-consumer-banking", "us-east-1"),
    ("ws-100002", "prod-investment-banking", "us-east-1"),
    ("ws-100003", "prod-risk-analytics", "us-west-2"),
    ("ws-100004", "prod-data-platform", "us-east-1"),
    ("ws-100005", "prod-fraud-detection", "us-west-2"),
]

DEPARTMENTS = [
    "Consumer Banking", "Investment Banking", "Risk Analytics",
    "Fraud Detection", "Data Engineering", "Data Science",
    "Compliance", "Treasury", "Marketing Analytics",
    "Credit Risk", "Market Risk", "Operations",
    "Wealth Management", "Card Services", "Mortgage",
]

TEAMS = [
    "platform-core", "etl-pipeline", "ml-ops", "analytics-eng",
    "feature-store", "data-quality", "streaming-ingest",
    "reporting", "bi-team", "quant-research", "fraud-ml",
    "aml-detection", "credit-scoring", "nrt-analytics",
    "data-governance", "lakehouse-admin", "cost-optimization",
]

SKU_MAP = {
    "STANDARD_ALL_PURPOSE_COMPUTE":  0.55,
    "PREMIUM_ALL_PURPOSE_COMPUTE":   0.70,
    "JOBS_COMPUTE":                  0.15,
    "JOBS_LIGHT_COMPUTE":            0.10,
    "SERVERLESS_SQL":                0.70,
    "PRO_SQL":                       0.55,
    "SERVERLESS_REAL_TIME_INFERENCE": 0.07,
    "GPU_ALL_PURPOSE_COMPUTE":       1.50,
    "DLT_CORE_COMPUTE":             0.20,
    "DLT_PRO_COMPUTE":              0.25,
    "DLT_ADVANCED_COMPUTE":         0.36,
}

# Weight distribution mimicking enterprise spend (heavily jobs + SQL)
SKU_WEIGHTS = {
    "JOBS_COMPUTE":                  0.35,
    "SERVERLESS_SQL":                0.20,
    "PRO_SQL":                       0.10,
    "STANDARD_ALL_PURPOSE_COMPUTE":  0.08,
    "PREMIUM_ALL_PURPOSE_COMPUTE":   0.05,
    "DLT_PRO_COMPUTE":              0.07,
    "DLT_ADVANCED_COMPUTE":         0.04,
    "DLT_CORE_COMPUTE":             0.03,
    "GPU_ALL_PURPOSE_COMPUTE":       0.04,
    "SERVERLESS_REAL_TIME_INFERENCE": 0.02,
    "JOBS_LIGHT_COMPUTE":            0.02,
}

NODE_TYPES = [
    "i3.xlarge", "i3.2xlarge", "i3.4xlarge", "i3.8xlarge",
    "r5.xlarge", "r5.2xlarge", "r5.4xlarge", "r5.8xlarge",
    "m5.xlarge", "m5.2xlarge", "m5.4xlarge",
    "p3.2xlarge", "p3.8xlarge", "g4dn.xlarge", "g4dn.4xlarge",
]

DBR_VERSIONS = [
    "12.2.x-scala2.12", "13.0.x-scala2.12", "13.3.x-scala2.12",
    "14.0.x-scala2.12", "14.3.x-scala2.12", "15.0.x-scala2.12",
    "15.2.x-scala2.12", "15.4.x-scala2.12",
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen", "Charles",
    "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret",
    "Mark", "Sandra", "Donald", "Ashley", "Steven", "Dorothy", "Paul",
    "Kimberly", "Andrew", "Emily", "Joshua", "Donna", "Kenneth", "Michelle",
    "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa", "Timothy",
    "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary",
    "Amy", "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna",
    "Stephen", "Brenda", "Larry", "Pamela", "Justin", "Emma", "Scott",
    "Nicole", "Brandon", "Helen", "Benjamin", "Samantha", "Samuel", "Katherine",
    "Raymond", "Christine", "Gregory", "Debra", "Frank", "Rachel", "Alexander",
    "Carolyn", "Patrick", "Janet", "Jack", "Catherine", "Dennis", "Maria",
    "Jerry", "Heather", "Tyler", "Diane", "Aaron", "Ruth", "Jose", "Julie",
    "Nathan", "Olivia", "Henry", "Joyce", "Peter", "Virginia", "Douglas",
    "Victoria", "Zachary", "Kelly", "Kyle", "Lauren", "Noah", "Christina",
    "Ethan", "Joan", "Adrian", "Evelyn", "Aiden", "Judith", "Dylan", "Megan",
    "Priya", "Raj", "Anita", "Vikram", "Deepa", "Sanjay", "Neha", "Amit",
    "Wei", "Ming", "Yuki", "Hiro", "Jin", "Soo", "Chen", "Li",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Patel", "Shah", "Kumar", "Singh", "Gupta",
    "Sharma", "Chen", "Wang", "Li", "Zhang", "Liu", "Yang", "Wu",
    "Kim", "Park", "Cho", "Jung", "Tanaka", "Suzuki", "Watanabe",
    "O'Brien", "Murphy", "Sullivan", "Cohen", "Goldberg", "Katz",
]

REGIONS = ["us-east-1", "us-west-2"]
CLOUDS = ["AWS"]

# ─── Helper ───────────────────────────────────────────────────────────────────

def run_sql(label: str, sql: str) -> bool:
    resp = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )
    deadline = time.time() + 300
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            print(f"  TIMEOUT: {label}")
            return False
        time.sleep(3)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state == StatementState.SUCCEEDED:
        print(f"  OK: {label}")
        return True
    else:
        err = getattr(resp.status.error, "message", str(resp.status.state))
        print(f"  SKIP ({err[:120]}): {label}")
        return False


def generate_users(count: int) -> List[str]:
    """Generate realistic email addresses for a large enterprise."""
    users = []
    used = set()
    random.seed(42)  # Reproducible
    for _ in range(count * 2):  # over-generate to handle collisions
        if len(users) >= count:
            break
        first = random.choice(FIRST_NAMES).lower()
        last = random.choice(LAST_NAMES).lower()
        email = f"{first}.{last}@chase.com"
        if email not in used:
            used.add(email)
            users.append(email)
    return users


def sql_str(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# ─── Data generation ─────────────────────────────────────────────────────────

USERS = generate_users(1100)
NOW = datetime(2026, 4, 26)
THREE_YEARS_AGO = NOW - timedelta(days=1095)

# Pre-assign users to departments and teams
random.seed(42)
USER_DEPT = {u: random.choice(DEPARTMENTS) for u in USERS}
USER_TEAM = {u: random.choice(TEAMS) for u in USERS}
USER_WS = {u: random.choice(WORKSPACES) for u in USERS}

# ─── Generate cluster definitions ────────────────────────────────────────────

def generate_clusters(count: int = 200):
    random.seed(100)
    clusters = []
    for i in range(count):
        ws = random.choice(WORKSPACES)
        owner = random.choice(USERS)
        cid = f"cls-{i+1:04d}"
        name_parts = [random.choice(["etl", "analytics", "ml", "streaming", "adhoc", "prod", "staging", "dev"]),
                      random.choice(["pipeline", "cluster", "compute", "workload", "processing"]),
                      str(random.randint(1, 50))]
        cname = "-".join(name_parts)
        driver = random.choice(NODE_TYPES[:11])
        worker = random.choice(NODE_TYPES[:11])
        workers = random.choice([2, 4, 8, 16, 32])
        min_w = max(1, workers // 4)
        max_w = workers * 2
        auto_term = random.choice([60, 120, 240, 0])
        dbr = random.choice(DBR_VERSIONS)
        team = USER_TEAM[owner]
        dept = USER_DEPT[owner]
        days_ago = random.randint(30, 1000)
        deleted = random.random() < 0.15  # 15% terminated
        security = random.choice(["SINGLE_USER", "USER_ISOLATION", "NO_ISOLATION"])
        clusters.append({
            "ws": ws, "id": cid, "name": cname, "owner": owner,
            "driver": driver, "worker": worker, "workers": workers,
            "min_w": min_w, "max_w": max_w, "auto_term": auto_term,
            "dbr": dbr, "team": team, "dept": dept, "days_ago": days_ago,
            "deleted": deleted, "security": security,
        })
    return clusters


def generate_warehouses(count: int = 30):
    random.seed(200)
    whs = []
    sizes = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large"]
    types = ["PRO", "CLASSIC", "SERVERLESS"]
    for i in range(count):
        ws = random.choice(WORKSPACES)
        wid = f"wh-{i+1:04d}"
        name = f"{random.choice(['reporting', 'bi', 'adhoc', 'etl', 'prod', 'staging'])}-warehouse-{i+1}"
        wtype = random.choice(types)
        wsize = random.choice(sizes)
        min_c = random.choice([1, 1, 1, 2])
        max_c = random.choice([1, 2, 4, 8, 16])
        auto_stop = random.choice([5, 10, 15, 30])
        days_ago = random.randint(30, 900)
        whs.append({
            "ws": ws, "id": wid, "name": name, "type": wtype,
            "size": wsize, "min_c": min_c, "max_c": max_c,
            "auto_stop": auto_stop, "days_ago": days_ago,
        })
    return whs


def generate_jobs(count: int = 500):
    random.seed(300)
    jobs = []
    job_types = [
        "ETL-Daily", "ETL-Hourly", "ML-Training", "ML-Scoring",
        "Report-Generation", "Data-Quality", "Feature-Engineering",
        "Streaming-Ingest", "CDC-Pipeline", "Archive-Job",
        "Compliance-Check", "AML-Scan", "Fraud-Score",
        "Risk-Calc", "PnL-Report", "Regulatory-Filing",
        "Customer-360", "Segmentation", "Campaign-Analytics",
        "Real-Time-Alerts",
    ]
    schedules = [
        "0 0 * * * ?",    # hourly
        "0 0 8 * * ?",    # daily 8am
        "0 0 6 * * ?",    # daily 6am
        "0 30 7 * * ?",   # daily 7:30am
        "0 0 0 * * ?",    # midnight
        "0 0 */4 * * ?",  # every 4h
        "0 0 9 ? * MON",  # weekly Monday
    ]
    for i in range(count):
        ws = random.choice(WORKSPACES)
        creator = random.choice(USERS)
        jid = f"job-{i+1:05d}"
        jtype = random.choice(job_types)
        name = f"{jtype}-{USER_DEPT[creator].lower().replace(' ', '-')}-{random.randint(1,99)}"
        sched = random.choice(schedules)
        days_ago = random.randint(10, 1000)
        deleted = random.random() < 0.08
        team = USER_TEAM[creator]
        env = random.choice(["prod", "staging", "dev"])
        jobs.append({
            "ws": ws, "id": jid, "name": name, "creator": creator,
            "schedule": sched, "days_ago": days_ago, "deleted": deleted,
            "team": team, "env": env,
        })
    return jobs


CLUSTERS = generate_clusters()
WAREHOUSES = generate_warehouses()
JOBS = generate_jobs()


# ── Billing usage generation ─────────────────────────────────────────────────

def generate_billing_rows() -> List[str]:
    """
    Generate ~3 years of billing data targeting ~$5M/year spend.

    Strategy: Generate daily records. With $5M/year and ~365 days,
    that's ~$13,700/day average. We'll distribute across SKUs and users
    with realistic growth (year-over-year ~20% increase).
    """
    random.seed(42)
    rows = []
    record_counter = 0

    # Daily target spend by year: year1=$3.5M, year2=$4.5M, year3=$5.5M (avg ~$4.5M)
    # This gives a nice growth trajectory with year3 forecasting ~$5M+
    yearly_daily_target = {0: 9600, 1: 12300, 2: 15000}  # $/day

    start_date = THREE_YEARS_AGO
    current = start_date

    while current < NOW:
        year_idx = min(2, (current - start_date).days // 365)
        daily_target = yearly_daily_target[year_idx]

        # Day of week seasonality: weekends ~40% of weekday
        dow = current.weekday()
        if dow >= 5:
            daily_target = int(daily_target * 0.4)

        # Month-end spike for financial (regulatory reporting)
        if current.day >= 28:
            daily_target = int(daily_target * 1.3)

        # Quarter-end bigger spike
        if current.month in (3, 6, 9, 12) and current.day >= 25:
            daily_target = int(daily_target * 1.5)

        # Generate records per SKU
        remaining = daily_target
        for sku, weight in SKU_WEIGHTS.items():
            sku_budget = int(daily_target * weight)
            if sku_budget < 1:
                continue
            price = SKU_MAP[sku]
            total_dbu = sku_budget / price

            # Split across 5–25 individual records (users/jobs)
            n_records = random.randint(5, 25)
            for _ in range(n_records):
                record_counter += 1
                user = random.choice(USERS)
                ws = random.choice(WORKSPACES)
                team = USER_TEAM[user]
                dept = USER_DEPT[user]

                dbu_qty = round(total_dbu / n_records * random.uniform(0.5, 1.5), 2)
                hour_offset = random.randint(0, 23)
                duration_min = random.randint(10, 180)

                usage_start = current.replace(hour=hour_offset, minute=0, second=0)
                usage_end = usage_start + timedelta(minutes=duration_min)

                # Determine usage metadata based on SKU
                cluster_id = ""
                warehouse_id = ""
                job_id = ""
                job_run_id = ""
                dlt_pipeline_id = ""
                notebook_id = ""
                endpoint_name = ""
                endpoint_id = ""
                run_id = ""

                if "ALL_PURPOSE" in sku or "GPU" in sku:
                    cluster_id = random.choice(CLUSTERS)["id"]
                    notebook_id = f"nb-{random.randint(1, 5000)}"
                elif "SQL" in sku:
                    warehouse_id = random.choice(WAREHOUSES)["id"]
                elif "JOBS" in sku:
                    job = random.choice(JOBS)
                    job_id = job["id"]
                    job_run_id = f"run-{record_counter}"
                elif "DLT" in sku:
                    dlt_pipeline_id = f"dlt-{random.randint(1, 100):03d}"
                elif "INFERENCE" in sku:
                    endpoint_name = random.choice(["fraud-scorer", "credit-model", "recommender", "nlp-classifier"])
                    endpoint_id = f"ep-{random.randint(1,20):03d}"

                billing_product = "INTERACTIVE" if "ALL_PURPOSE" in sku else (
                    "SQL" if "SQL" in sku else (
                    "JOBS" if "JOBS" in sku else (
                    "DLT" if "DLT" in sku else "SERVING")))

                rid = f"r-{record_counter:08d}"

                rows.append(
                    f"('{rid}','{ACCOUNT_ID}','{ws[0]}','{sku}','AWS',"
                    f"'{usage_start.strftime('%Y-%m-%d %H:%M:%S')}','{usage_end.strftime('%Y-%m-%d %H:%M:%S')}',"
                    f"'{current.strftime('%Y-%m-%d')}',map('team','{sql_str(team)}','department','{sql_str(dept)}'),'DBU',{dbu_qty},'{billing_product}',"
                    f"'{billing_product}','ORIGINAL','{current.strftime('%Y-%m-%d')}',"
                    f"named_struct('run_as','{sql_str(user)}','created_by','{sql_str(user)}'),"
                    f"named_struct('cluster_id','{cluster_id}','warehouse_id','{warehouse_id}',"
                    f"'job_id','{job_id}','job_run_id','{job_run_id}',"
                    f"'dlt_pipeline_id','{dlt_pipeline_id}','notebook_id','{notebook_id}',"
                    f"'endpoint_name','{endpoint_name}','endpoint_id','{endpoint_id}','run_id','{run_id}'))"
                )

        current += timedelta(days=1)

    return rows


def generate_audit_rows() -> List[str]:
    """Generate audit log entries across 3 years for 1000+ users."""
    random.seed(55)
    rows = []
    actions = [
        ("accounts", "login"),
        ("accounts", "logout"),
        ("clusters", "create"),
        ("clusters", "start"),
        ("clusters", "terminate"),
        ("clusters", "resize"),
        ("jobs", "create"),
        ("jobs", "runNow"),
        ("jobs", "delete"),
        ("sql", "commandSubmit"),
        ("sql", "commandFinish"),
        ("notebook", "runCommand"),
        ("notebook", "attachNotebook"),
        ("databrickssql", "getWarehouse"),
        ("databrickssql", "createWarehouse"),
        ("secrets", "getSecret"),
        ("secrets", "listSecrets"),
        ("unityCatalog", "getTable"),
        ("unityCatalog", "createTable"),
        ("unityCatalog", "getSchema"),
        ("mlflow", "createRun"),
        ("mlflow", "logMetric"),
        ("workspace", "fileCreate"),
        ("workspace", "fileDelete"),
        ("iamRole", "changePermissions"),
    ]
    counter = 0
    current = THREE_YEARS_AGO

    # Generate ~50 events/day (sampled — actual enterprise would be millions)
    while current < NOW:
        n_events = random.randint(30, 80)
        for _ in range(n_events):
            counter += 1
            user = random.choice(USERS)
            ws = random.choice(WORKSPACES)
            service, action = random.choice(actions)
            hour = random.randint(6, 22)
            minute = random.randint(0, 59)
            evt_time = current.replace(hour=hour, minute=minute, second=random.randint(0, 59))
            status = 200 if random.random() < 0.95 else random.choice([401, 403, 500])
            err_msg = "" if status == 200 else "Access denied"
            result = "success" if status == 200 else "failure"
            ip = f"{random.randint(10,172)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

            rows.append(
                f"('{ACCOUNT_ID}','{ws[0]}','2.0',"
                f"'{evt_time.strftime('%Y-%m-%d %H:%M:%S')}','{current.strftime('%Y-%m-%d')}',"
                f"'{ip}','Databricks/API','sess-{counter:08d}',"
                f"named_struct('email','{sql_str(user)}','subjectName','{sql_str(user)}'),"
                f"'{service}','{action}','req-{counter:08d}',"
                f"map('user','{sql_str(user)}'),"
                f"named_struct('statusCode',{status},'errorMessage',{repr(err_msg) if err_msg else 'cast(null as STRING)'},'result','{result}'),"
                f"'WORKSPACE_LEVEL','evt-{counter:08d}',"
                f"named_struct('run_by','{sql_str(user)}','run_as','{sql_str(user)}'))"
            )

        current += timedelta(days=1)
    return rows


def generate_query_history_rows() -> List[str]:
    """Generate query history across 3 years."""
    random.seed(77)
    rows = []
    queries = [
        ("SELECT", "SELECT * FROM transactions WHERE date >= current_date - 30"),
        ("SELECT", "SELECT customer_id, SUM(amount) FROM payments GROUP BY 1"),
        ("SELECT", "SELECT risk_score, COUNT(*) FROM credit_assessments GROUP BY 1"),
        ("INSERT", "INSERT INTO daily_aggregates SELECT date, SUM(volume) FROM trades GROUP BY 1"),
        ("MERGE", "MERGE INTO customer_360 USING staging_customers ON id = s_id"),
        ("SELECT", "SELECT * FROM fraud_alerts WHERE score > 0.9 AND created_date = current_date"),
        ("CREATE", "CREATE TABLE IF NOT EXISTS regulatory_report_q4 AS SELECT * FROM compliance"),
        ("SELECT", "SELECT acct_type, AVG(balance) FROM accounts GROUP BY 1"),
        ("SELECT", "SELECT COUNT(DISTINCT customer_id) FROM digital_banking_events"),
        ("SELECT", "SELECT department, SUM(cost) FROM cost_allocation GROUP BY 1"),
    ]
    counter = 0
    current = THREE_YEARS_AGO

    while current < NOW:
        n_queries = random.randint(100, 400)  # 100-400 queries per day
        for _ in range(n_queries):
            counter += 1
            user = random.choice(USERS)
            wh = random.choice(WAREHOUSES)
            stmt_type, stmt_text = random.choice(queries)
            hour = random.randint(7, 21)
            start_time = current.replace(hour=hour, minute=random.randint(0, 59))
            duration = random.randint(50, 300000)  # 50ms to 5min
            read_bytes = random.randint(1000, 10737418240)  # up to 10GB
            read_rows_val = random.randint(100, 50000000)
            has_error = random.random() < 0.02
            error = "TABLE_OR_VIEW_NOT_FOUND" if has_error else ""

            rows.append(
                f"('stmt-{counter:08d}','{sql_str(user)}','{sql_str(user)}',"
                f"named_struct('warehouse_id','{wh['id']}'),'{stmt_type}',"
                f"'{sql_str(stmt_text)}',"
                f"'{start_time.strftime('%Y-%m-%d %H:%M:%S')}',{duration},{read_bytes},{read_rows_val},"
                f"{'cast(null as STRING)' if not has_error else repr(error)})"
            )

        current += timedelta(days=7)  # sample weekly to keep manageable
    return rows


def generate_job_run_rows() -> List[str]:
    """Generate job run timeline entries."""
    random.seed(88)
    rows = []
    counter = 0
    current = THREE_YEARS_AGO

    while current < NOW:
        # ~100-300 job runs per day
        n_runs = random.randint(80, 300)
        for _ in range(n_runs):
            counter += 1
            job = random.choice(JOBS)
            ws = job["ws"]
            hour = random.randint(0, 23)
            start = current.replace(hour=hour, minute=random.randint(0, 59))
            duration = random.randint(2, 240)  # 2-240 minutes
            end = start + timedelta(minutes=duration)
            result = random.choices(
                ["SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"],
                weights=[0.88, 0.07, 0.03, 0.02]
            )[0]
            term_code = "SUCCESS" if result == "SUCCEEDED" else (
                "RUN_EXECUTION_ERROR" if result == "FAILED" else (
                "USER_CANCELLED" if result == "CANCELLED" else "MAX_RUN_DURATION_EXCEEDED"))
            trigger = random.choice(["SCHEDULED", "MANUAL", "RETRY", "FILE_ARRIVAL"])

            rows.append(
                f"('{ACCOUNT_ID}','{ws[0]}','{job['id']}','run-{counter:08d}',"
                f"'{start.strftime('%Y-%m-%d %H:%M:%S')}','{end.strftime('%Y-%m-%d %H:%M:%S')}',"
                f"'{trigger}','JOB_RUN','{result}','{term_code}')"
            )

        current += timedelta(days=1)
    return rows


def generate_ai_gateway_rows() -> List[str]:
    """Generate AI gateway usage (last 1.5 years as AI adoption grew)."""
    random.seed(99)
    rows = []
    routes = [
        "gpt-4-turbo", "claude-3-sonnet", "databricks-dbrx",
        "databricks-meta-llama-3", "databricks-mixtral",
        "text-embedding-ada-002", "databricks-bge-large",
    ]
    counter = 0
    ai_start = NOW - timedelta(days=540)  # 1.5 years of AI usage
    current = ai_start

    while current < NOW:
        n_calls = random.randint(50, 300)  # growing AI adoption
        # Growth over time
        days_since_start = (current - ai_start).days
        growth_factor = 1 + (days_since_start / 540) * 3  # 4x growth over period
        n_calls = int(n_calls * growth_factor)

        for _ in range(min(n_calls, 500)):
            counter += 1
            user = random.choice(USERS[:400])  # ~400 AI power users
            route = random.choice(routes)
            hour = random.randint(8, 20)
            evt_time = current.replace(hour=hour, minute=random.randint(0, 59))
            input_tokens = random.randint(100, 8000)
            output_tokens = random.randint(50, 4000)
            total_tokens = input_tokens + output_tokens
            latency = random.uniform(100, 5000)

            rows.append(
                f"('{route}','{evt_time.strftime('%Y-%m-%d %H:%M:%S')}',"
                f"{total_tokens},{input_tokens},{output_tokens},{latency:.1f},'{sql_str(user)}')"
            )

        current += timedelta(days=1)
    return rows


# ── Build SQL statements ──────────────────────────────────────────────────────

def build_statements() -> List[Tuple[str, str]]:
    stmts = []

    # ── Schemas ──
    for schema in ["billing", "access", "compute", "lakeflow", "query",
                    "ai_gateway", "serving", "mlflow", "storage",
                    "information_schema", "networking", "lakeview",
                    "dashboards", "marketplace"]:
        stmts.append((f"schema {schema}", f"CREATE SCHEMA IF NOT EXISTS workspace.mock_system_{schema}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # billing.usage
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop billing.usage", "DROP TABLE IF EXISTS workspace.mock_system_billing.usage"))
    stmts.append(("create billing.usage", """
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
)"""))

    print("Generating billing usage rows (3 years)...")
    billing_rows = generate_billing_rows()
    print(f"  Generated {len(billing_rows)} billing rows")

    # Insert in chunks of 500 to avoid SQL size limits
    for i, chunk in enumerate(chunk_list(billing_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert billing.usage chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_billing.usage VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # billing.list_prices
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop billing.list_prices", "DROP TABLE IF EXISTS workspace.mock_system_billing.list_prices"))
    stmts.append(("create billing.list_prices", """
CREATE TABLE workspace.mock_system_billing.list_prices (
  price_start_time TIMESTAMP,
  price_end_time   TIMESTAMP,
  account_id       STRING,
  sku_name         STRING,
  cloud            STRING,
  currency_code    STRING,
  usage_unit       STRING,
  pricing          STRUCT<default: DOUBLE, promotional: DOUBLE, effective_list: DOUBLE>
)"""))

    price_rows = []
    for sku, price in SKU_MAP.items():
        price_rows.append(
            f"('{THREE_YEARS_AGO.strftime('%Y-%m-%d %H:%M:%S')}',cast(null as TIMESTAMP),"
            f"'{ACCOUNT_ID}','{sku}','AWS','USD','DBU',"
            f"named_struct('default',{price},'promotional',cast(null as DOUBLE),'effective_list',{price}))"
        )
    stmts.append(("insert billing.list_prices",
                   f"INSERT INTO workspace.mock_system_billing.list_prices VALUES\n" + ",\n".join(price_rows)))

    # ═══════════════════════════════════════════════════════════════════════════
    # access.workspaces_latest
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop access.workspaces_latest", "DROP TABLE IF EXISTS workspace.mock_system_access.workspaces_latest"))
    stmts.append(("create access.workspaces_latest", """
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
)"""))
    ws_rows = []
    for ws_id, ws_name, ws_region in WORKSPACES:
        ws_rows.append(
            f"('{ACCOUNT_ID}','{ws_id}','{ws_name}',"
            f"'https://{ws_name}.cloud.databricks.com',"
            f"'RUNNING','AWS','{ws_region}','ENTERPRISE',"
            f"'{THREE_YEARS_AGO.strftime('%Y-%m-%d %H:%M:%S')}')"
        )
    stmts.append(("insert access.workspaces_latest",
                   f"INSERT INTO workspace.mock_system_access.workspaces_latest VALUES\n" + ",\n".join(ws_rows)))

    # ═══════════════════════════════════════════════════════════════════════════
    # access.audit
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop access.audit", "DROP TABLE IF EXISTS workspace.mock_system_access.audit"))
    stmts.append(("create access.audit", """
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
)"""))

    print("Generating audit log rows (3 years)...")
    audit_rows = generate_audit_rows()
    print(f"  Generated {len(audit_rows)} audit rows")

    for i, chunk in enumerate(chunk_list(audit_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert audit chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_access.audit VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # compute.clusters
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop compute.clusters", "DROP TABLE IF EXISTS workspace.mock_system_compute.clusters"))
    stmts.append(("create compute.clusters", """
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
)"""))

    cluster_rows = []
    for c in CLUSTERS:
        ws = c["ws"]
        create_date = (NOW - timedelta(days=c["days_ago"])).strftime("%Y-%m-%d %H:%M:%S")
        del_time = f"'{(NOW - timedelta(days=random.randint(1, c['days_ago']))).strftime('%Y-%m-%d %H:%M:%S')}'" if c["deleted"] else "cast(null as TIMESTAMP)"
        change_date = (NOW - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
        cluster_rows.append(
            f"('{ACCOUNT_ID}','{ws[0]}','{c['id']}','{c['name']}','{sql_str(c['owner'])}',"
            f"'{create_date}',{del_time},"
            f"'{c['driver']}','{c['worker']}',{c['workers']},{c['min_w']},{c['max_w']},"
            f"{c['auto_term']},true,"
            f"map('team','{c['team']}','department','{c['dept']}'),'API','{c['dbr']}',"
            f"'{change_date} 10:00:00','{change_date}','{c['security']}')"
        )
    for i, chunk in enumerate(chunk_list(cluster_rows, 200)):
        values = ",\n".join(chunk)
        stmts.append((f"insert clusters chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_compute.clusters VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # compute.warehouses
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop compute.warehouses", "DROP TABLE IF EXISTS workspace.mock_system_compute.warehouses"))
    stmts.append(("create compute.warehouses", """
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
)"""))

    wh_rows = []
    for wh in WAREHOUSES:
        ws = wh["ws"]
        change = (NOW - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S")
        wh_rows.append(
            f"('{ACCOUNT_ID}','{ws[0]}','{wh['id']}','{wh['name']}',"
            f"'{wh['type']}','{wh['size']}',{wh['min_c']},{wh['max_c']},"
            f"{wh['auto_stop']},'{change}',cast(null as TIMESTAMP))"
        )
    stmts.append(("insert warehouses", f"INSERT INTO workspace.mock_system_compute.warehouses VALUES\n" + ",\n".join(wh_rows)))

    # ═══════════════════════════════════════════════════════════════════════════
    # compute.node_timeline
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop compute.node_timeline", "DROP TABLE IF EXISTS workspace.mock_system_compute.node_timeline"))
    stmts.append(("create compute.node_timeline", """
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
)"""))

    # Generate node metrics for last 90 days (recent data for health dashboards)
    random.seed(111)
    node_rows = []
    for day_offset in range(90):
        current_day = NOW - timedelta(days=day_offset)
        # Sample 20-40 clusters active per day
        active_clusters = random.sample(CLUSTERS, min(40, len(CLUSTERS)))
        for cl in active_clusters:
            ws = cl["ws"]
            n_nodes = random.randint(2, cl["workers"] + 1)
            for node_i in range(n_nodes):
                is_driver = (node_i == 0)
                cpu_user = random.uniform(5, 95)
                cpu_sys = random.uniform(1, 15)
                cpu_wait = random.uniform(0, 10)
                mem = random.uniform(20, 95)
                hour = random.randint(6, 22)
                start = current_day.replace(hour=hour, minute=0, second=0)
                end = start + timedelta(hours=random.randint(1, 8))

                node_rows.append(
                    f"('{ACCOUNT_ID}','{ws[0]}','{cl['id']}','node-{day_offset:03d}-{cl['id']}-{node_i}',"
                    f"'i-{random.randint(10000,99999):05d}{random.randint(10000,99999):05d}',"
                    f"'{start.strftime('%Y-%m-%d %H:%M:%S')}','{end.strftime('%Y-%m-%d %H:%M:%S')}',"
                    f"{str(is_driver).lower()},{str(is_driver).lower()},"
                    f"{cpu_user:.1f},{cpu_sys:.1f},{cpu_wait:.1f},{mem:.1f},{random.uniform(0, 2):.1f},"
                    f"{random.randint(100000, 10000000000)},{random.randint(100000, 10000000000)},"
                    f"'{cl['worker']}','10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}',"
                    f"{random.uniform(3600, 28800):.0f},{random.randint(4, 32)},"
                    f"{random.uniform(0.5, 28):.1f},{random.uniform(0, 5):.1f})"
                )

    print(f"  Generated {len(node_rows)} node timeline rows")
    for i, chunk in enumerate(chunk_list(node_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert node_timeline chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_compute.node_timeline VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # compute.cluster_events & warehouse_events (lightweight)
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop compute.cluster_events", "DROP TABLE IF EXISTS workspace.mock_system_compute.cluster_events"))
    stmts.append(("create compute.cluster_events", """
CREATE TABLE workspace.mock_system_compute.cluster_events (
  account_id   STRING,
  workspace_id STRING,
  cluster_id   STRING,
  timestamp    TIMESTAMP,
  type         STRING,
  details      MAP<STRING, STRING>
)"""))
    stmts.append(("drop compute.warehouse_events", "DROP TABLE IF EXISTS workspace.mock_system_compute.warehouse_events"))
    stmts.append(("create compute.warehouse_events", """
CREATE TABLE workspace.mock_system_compute.warehouse_events (
  account_id    STRING,
  workspace_id  STRING,
  warehouse_id  STRING,
  event_type    STRING,
  cluster_count INT,
  event_time    TIMESTAMP
)"""))

    # ═══════════════════════════════════════════════════════════════════════════
    # lakeflow.jobs
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop lakeflow.jobs", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.jobs"))
    stmts.append(("create lakeflow.jobs", """
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
)"""))

    job_rows = []
    for j in JOBS:
        ws = j["ws"]
        created = (NOW - timedelta(days=j["days_ago"])).strftime("%Y-%m-%d %H:%M:%S")
        changed = (NOW - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S")
        del_time = f"'{(NOW - timedelta(days=random.randint(1, j['days_ago']))).strftime('%Y-%m-%d %H:%M:%S')}'" if j["deleted"] else "cast(null as TIMESTAMP)"
        pause = "UNPAUSED" if random.random() < 0.85 else "PAUSED"
        job_rows.append(
            f"('{ACCOUNT_ID}','{ws[0]}','{j['id']}','{sql_str(j['name'])}',"
            f"'{sql_str(j['creator'])}','{sql_str(j['creator'])}',"
            f"map('Env','{j['env']}','team','{j['team']}'),"
            f"named_struct('quartz_cron_expression','{j['schedule']}','pause_status','{pause}'),"
            f"'{created}','{changed}',{del_time})"
        )
    for i, chunk in enumerate(chunk_list(job_rows, 200)):
        values = ",\n".join(chunk)
        stmts.append((f"insert jobs chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_lakeflow.jobs VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # lakeflow.job_run_timeline
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop lakeflow.job_run_timeline", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.job_run_timeline"))
    stmts.append(("create lakeflow.job_run_timeline", """
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
)"""))

    print("Generating job run timeline rows (3 years)...")
    job_run_rows = generate_job_run_rows()
    print(f"  Generated {len(job_run_rows)} job run rows")

    for i, chunk in enumerate(chunk_list(job_run_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert job_runs chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_lakeflow.job_run_timeline VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # lakeflow.job_tasks + job_task_run_timeline (lightweight)
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop lakeflow.job_tasks", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.job_tasks"))
    stmts.append(("create lakeflow.job_tasks", """
CREATE TABLE workspace.mock_system_lakeflow.job_tasks (
  account_id   STRING,
  workspace_id STRING,
  job_id       STRING,
  task_key     STRING,
  change_time  TIMESTAMP,
  delete_time  TIMESTAMP
)"""))
    task_rows = []
    for j in JOBS[:200]:  # Top 200 jobs with task details
        ws = j["ws"]
        n_tasks = random.randint(1, 8)
        for t in range(n_tasks):
            task_rows.append(
                f"('{ACCOUNT_ID}','{ws[0]}','{j['id']}','task-{t+1}',"
                f"'{(NOW - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S')}',cast(null as TIMESTAMP))"
            )
    for i, chunk in enumerate(chunk_list(task_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert job_tasks chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_lakeflow.job_tasks VALUES\n{values}"))

    stmts.append(("drop lakeflow.job_task_run_timeline", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.job_task_run_timeline"))
    stmts.append(("create lakeflow.job_task_run_timeline", """
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
)"""))

    # ═══════════════════════════════════════════════════════════════════════════
    # lakeflow.pipelines + pipeline_update_timeline
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop lakeflow.pipelines", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.pipelines"))
    stmts.append(("create lakeflow.pipelines", """
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
)"""))

    pipeline_rows = []
    pipeline_names = [
        "CDC-Customer-Accounts", "Fraud-Alerts-Stream", "Transaction-ETL",
        "Risk-Score-Pipeline", "AML-Detection", "Market-Data-Ingest",
        "Regulatory-Reporting", "Customer-360-Build", "Credit-Score-Update",
        "Payment-Processing", "Loan-Origination", "Card-Transaction-Stream",
        "KYC-Verification", "Portfolio-Analytics", "Trade-Settlement",
        "Compliance-Audit-Trail", "Digital-Banking-Events", "ATM-Transaction-Stream",
        "Branch-Performance", "Wealth-Portfolio-Sync",
    ]
    for i, pname in enumerate(pipeline_names):
        ws = WORKSPACES[i % len(WORKSPACES)]
        creator = random.choice(USERS)
        edition = random.choice(["CORE", "PRO", "ADVANCED"])
        created = (NOW - timedelta(days=random.randint(100, 900))).strftime("%Y-%m-%d %H:%M:%S")
        changed = (NOW - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S")
        pipeline_rows.append(
            f"('{ACCOUNT_ID}','{ws[0]}','dlt-{i+1:03d}','{pname}',"
            f"'{sql_str(creator)}','{sql_str(creator)}',"
            f"'CURRENT','{edition}','{created}','{changed}',cast(null as TIMESTAMP))"
        )
    stmts.append(("insert pipelines",
                   f"INSERT INTO workspace.mock_system_lakeflow.pipelines VALUES\n" + ",\n".join(pipeline_rows)))

    stmts.append(("drop lakeflow.pipeline_update_timeline", "DROP TABLE IF EXISTS workspace.mock_system_lakeflow.pipeline_update_timeline"))
    stmts.append(("create lakeflow.pipeline_update_timeline", """
CREATE TABLE workspace.mock_system_lakeflow.pipeline_update_timeline (
  account_id        STRING,
  workspace_id      STRING,
  pipeline_id       STRING,
  update_id         STRING,
  result_state      STRING,
  period_start_time TIMESTAMP,
  period_end_time   TIMESTAMP
)"""))

    # ═══════════════════════════════════════════════════════════════════════════
    # query.history
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop query.history", "DROP TABLE IF EXISTS workspace.mock_system_query.history"))
    stmts.append(("create query.history", """
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
)"""))

    print("Generating query history rows...")
    query_rows = generate_query_history_rows()
    print(f"  Generated {len(query_rows)} query history rows")

    for i, chunk in enumerate(chunk_list(query_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert query.history chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_query.history VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # ai_gateway.usage
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop ai_gateway.usage", "DROP TABLE IF EXISTS workspace.mock_system_ai_gateway.usage"))
    stmts.append(("create ai_gateway.usage", """
CREATE TABLE workspace.mock_system_ai_gateway.usage (
  route_name            STRING,
  event_time            TIMESTAMP,
  total_token_count     BIGINT,
  input_token_count     BIGINT,
  output_token_count    BIGINT,
  execution_duration_ms DOUBLE,
  requester             STRING
)"""))

    print("Generating AI gateway usage rows...")
    ai_rows = generate_ai_gateway_rows()
    print(f"  Generated {len(ai_rows)} AI gateway rows")

    for i, chunk in enumerate(chunk_list(ai_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert ai_gateway chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_ai_gateway.usage VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # serving.served_entities + endpoint_usage
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop serving.served_entities", "DROP TABLE IF EXISTS workspace.mock_system_serving.served_entities"))
    stmts.append(("create serving.served_entities", """
CREATE TABLE workspace.mock_system_serving.served_entities (
  endpoint_name     STRING,
  served_entity_name STRING,
  entity_type       STRING,
  workspace_id      STRING,
  change_time       TIMESTAMP
)"""))

    endpoints = [
        ("fraud-scorer", "fraud-detection-v3", "CUSTOM_MODEL"),
        ("credit-model", "credit-risk-xgb", "CUSTOM_MODEL"),
        ("recommender", "product-recommender-v2", "CUSTOM_MODEL"),
        ("nlp-classifier", "doc-classifier-bert", "CUSTOM_MODEL"),
        ("llm-gateway", "databricks-meta-llama-3", "FOUNDATION_MODEL"),
        ("embedding-service", "databricks-bge-large", "FOUNDATION_MODEL"),
        ("aml-detector", "aml-detection-ensemble", "CUSTOM_MODEL"),
        ("kyc-verifier", "kyc-document-ocr", "CUSTOM_MODEL"),
    ]
    ep_rows = []
    for ep_name, entity, etype in endpoints:
        ws = random.choice(WORKSPACES)
        ep_rows.append(
            f"('{ep_name}','{entity}','{etype}','{ws[0]}',"
            f"'{(NOW - timedelta(days=random.randint(30, 300))).strftime('%Y-%m-%d %H:%M:%S')}')"
        )
    stmts.append(("insert served_entities",
                   f"INSERT INTO workspace.mock_system_serving.served_entities VALUES\n" + ",\n".join(ep_rows)))

    stmts.append(("drop serving.endpoint_usage", "DROP TABLE IF EXISTS workspace.mock_system_serving.endpoint_usage"))
    stmts.append(("create serving.endpoint_usage", """
CREATE TABLE workspace.mock_system_serving.endpoint_usage (
  served_entity_name STRING,
  request_time       TIMESTAMP,
  total_token_count  BIGINT,
  status_code        INT
)"""))

    ep_usage_rows = []
    random.seed(133)
    for day_offset in range(365):  # Last year of endpoint usage
        d = NOW - timedelta(days=day_offset)
        for ep_name, entity, etype in endpoints:
            n_requests = random.randint(50, 500)
            for _ in range(min(n_requests, 100)):  # sample
                t = d.replace(hour=random.randint(0, 23), minute=random.randint(0, 59))
                tokens = random.randint(100, 5000) if "FOUNDATION" in etype else random.randint(0, 10)
                status = 200 if random.random() < 0.97 else random.choice([400, 500, 503])
                ep_usage_rows.append(
                    f"('{entity}','{t.strftime('%Y-%m-%d %H:%M:%S')}',{tokens},{status})"
                )

    print(f"  Generated {len(ep_usage_rows)} endpoint usage rows")
    for i, chunk in enumerate(chunk_list(ep_usage_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert endpoint_usage chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_serving.endpoint_usage VALUES\n{values}"))

    # ═══════════════════════════════════════════════════════════════════════════
    # mlflow tables
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop mlflow.experiments_latest", "DROP TABLE IF EXISTS workspace.mock_system_mlflow.experiments_latest"))
    stmts.append(("create mlflow.experiments_latest", """
CREATE TABLE workspace.mock_system_mlflow.experiments_latest (
  experiment_id    STRING,
  name             STRING,
  lifecycle_stage  STRING,
  creation_time    BIGINT,
  last_update_time BIGINT
)"""))

    ml_experiments = [
        "Fraud-Detection-V3", "Credit-Risk-XGBoost", "Customer-Churn-Prediction",
        "Product-Recommender", "NLP-Document-Classifier", "AML-Ensemble",
        "Transaction-Anomaly", "Loan-Default-Prediction", "Sentiment-Analysis",
        "Market-Risk-VAR", "Portfolio-Optimization", "KYC-OCR-Model",
        "Card-Fraud-RealTime", "Customer-LTV", "Cross-Sell-Propensity",
        "Branch-Demand-Forecast", "ATM-Cash-Optimization", "Interest-Rate-Model",
        "Mortgage-Prepayment", "Collections-Priority",
    ]
    exp_rows = []
    for i, ename in enumerate(ml_experiments):
        days_ago = random.randint(90, 900)
        update_ago = random.randint(1, 60)
        create_ts = int((NOW - timedelta(days=days_ago)).timestamp() * 1000)
        update_ts = int((NOW - timedelta(days=update_ago)).timestamp() * 1000)
        stage = "active" if random.random() < 0.85 else "deleted"
        exp_rows.append(f"('exp-{i+1:03d}','{ename}','{stage}',{create_ts},{update_ts})")
    stmts.append(("insert experiments",
                   f"INSERT INTO workspace.mock_system_mlflow.experiments_latest VALUES\n" + ",\n".join(exp_rows)))

    stmts.append(("drop mlflow.runs_latest", "DROP TABLE IF EXISTS workspace.mock_system_mlflow.runs_latest"))
    stmts.append(("create mlflow.runs_latest", """
CREATE TABLE workspace.mock_system_mlflow.runs_latest (
  experiment_id STRING,
  run_id        STRING,
  status        STRING,
  start_time    TIMESTAMP,
  end_time      TIMESTAMP,
  user_id       STRING
)"""))

    ml_run_rows = []
    random.seed(144)
    for i in range(len(ml_experiments)):
        n_runs = random.randint(20, 200)
        for r in range(n_runs):
            days_ago = random.randint(1, 365)
            start = NOW - timedelta(days=days_ago, hours=random.randint(0, 23))
            dur = random.randint(5, 480)
            end = start + timedelta(minutes=dur)
            status = random.choices(["FINISHED", "FAILED", "RUNNING", "KILLED"],
                                    weights=[0.75, 0.12, 0.08, 0.05])[0]
            user = random.choice(USERS[:300])  # ML engineers subset
            ml_run_rows.append(
                f"('exp-{i+1:03d}','mlrun-{i+1:03d}-{r+1:04d}','{status}',"
                f"'{start.strftime('%Y-%m-%d %H:%M:%S')}','{end.strftime('%Y-%m-%d %H:%M:%S')}',"
                f"'{sql_str(user)}')"
            )
    print(f"  Generated {len(ml_run_rows)} ML run rows")
    for i, chunk in enumerate(chunk_list(ml_run_rows, 500)):
        values = ",\n".join(chunk)
        stmts.append((f"insert mlflow.runs chunk {i+1}",
                       f"INSERT INTO workspace.mock_system_mlflow.runs_latest VALUES\n{values}"))

    stmts.append(("drop mlflow.registered_models_latest", "DROP TABLE IF EXISTS workspace.mock_system_mlflow.registered_models_latest"))
    stmts.append(("create mlflow.registered_models_latest", """
CREATE TABLE workspace.mock_system_mlflow.registered_models_latest (
  name                   STRING,
  creation_timestamp     BIGINT,
  last_updated_timestamp BIGINT,
  user_id                STRING
)"""))

    model_rows = []
    registered_models = [
        "fraud-detection-v3", "credit-risk-xgb", "customer-churn-model",
        "product-recommender-v2", "doc-classifier-bert", "aml-detection-ensemble",
        "transaction-anomaly-v1", "loan-default-pred", "sentiment-analyzer",
        "market-risk-var", "portfolio-optimizer", "kyc-document-ocr",
    ]
    for mname in registered_models:
        user = random.choice(USERS[:200])
        days_ago = random.randint(30, 600)
        update_ago = random.randint(1, 30)
        create_ts = int((NOW - timedelta(days=days_ago)).timestamp() * 1000)
        update_ts = int((NOW - timedelta(days=update_ago)).timestamp() * 1000)
        model_rows.append(f"('{mname}',{create_ts},{update_ts},'{sql_str(user)}')")
    stmts.append(("insert registered_models",
                   f"INSERT INTO workspace.mock_system_mlflow.registered_models_latest VALUES\n" + ",\n".join(model_rows)))

    # ═══════════════════════════════════════════════════════════════════════════
    # access.table_lineage + column_lineage
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop access.table_lineage", "DROP TABLE IF EXISTS workspace.mock_system_access.table_lineage"))
    stmts.append(("create access.table_lineage", """
CREATE TABLE workspace.mock_system_access.table_lineage (
  source_table_catalog STRING,
  source_table_schema  STRING,
  source_table_name    STRING,
  target_table_catalog STRING,
  target_table_schema  STRING,
  target_table_name    STRING,
  event_time           TIMESTAMP
)"""))

    lineage_pairs = [
        ("raw_transactions", "bronze_transactions"),
        ("bronze_transactions", "silver_transactions"),
        ("silver_transactions", "gold_transaction_summary"),
        ("raw_customers", "bronze_customers"),
        ("bronze_customers", "silver_customer_360"),
        ("silver_customer_360", "gold_customer_analytics"),
        ("raw_market_data", "bronze_market_data"),
        ("bronze_market_data", "silver_risk_metrics"),
        ("silver_risk_metrics", "gold_risk_dashboard"),
        ("raw_fraud_events", "bronze_fraud_alerts"),
        ("bronze_fraud_alerts", "silver_fraud_scores"),
        ("silver_fraud_scores", "gold_fraud_summary"),
        ("raw_loan_applications", "bronze_loan_data"),
        ("bronze_loan_data", "silver_credit_assessment"),
        ("silver_credit_assessment", "gold_lending_analytics"),
    ]
    lineage_rows = []
    for src, tgt in lineage_pairs:
        for day_offset in range(30):
            d = NOW - timedelta(days=day_offset)
            lineage_rows.append(
                f"('workspace','banking_data','{src}','workspace','banking_data','{tgt}',"
                f"'{d.strftime('%Y-%m-%d')} 08:00:00')"
            )
    stmts.append(("insert table_lineage",
                   f"INSERT INTO workspace.mock_system_access.table_lineage VALUES\n" + ",\n".join(lineage_rows)))

    stmts.append(("drop access.column_lineage", "DROP TABLE IF EXISTS workspace.mock_system_access.column_lineage"))
    stmts.append(("create access.column_lineage", """
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
)"""))
    col_lineage_rows = []
    col_pairs = [
        ("raw_transactions", "amount", "silver_transactions", "total_amount"),
        ("raw_transactions", "customer_id", "silver_transactions", "customer_id"),
        ("raw_customers", "email", "silver_customer_360", "contact_email"),
        ("raw_fraud_events", "score", "silver_fraud_scores", "risk_score"),
    ]
    for src_tbl, src_col, tgt_tbl, tgt_col in col_pairs:
        col_lineage_rows.append(
            f"('workspace','banking_data','{src_tbl}','{src_col}',"
            f"'workspace','banking_data','{tgt_tbl}','{tgt_col}',"
            f"'{(NOW - timedelta(days=1)).strftime('%Y-%m-%d')} 08:00:00')"
        )
    stmts.append(("insert column_lineage",
                   f"INSERT INTO workspace.mock_system_access.column_lineage VALUES\n" + ",\n".join(col_lineage_rows)))

    # ═══════════════════════════════════════════════════════════════════════════
    # storage.predictive_optimization_operations_history
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop storage.predictive_opt", "DROP TABLE IF EXISTS workspace.mock_system_storage.predictive_optimization_operations_history"))
    stmts.append(("create storage.predictive_opt", """
CREATE TABLE workspace.mock_system_storage.predictive_optimization_operations_history (
  catalog_name      STRING,
  schema_name       STRING,
  table_name        STRING,
  operation_type    STRING,
  operation_status  STRING,
  start_time        TIMESTAMP,
  end_time          TIMESTAMP,
  operation_metrics MAP<STRING, STRING>
)"""))

    storage_rows = []
    storage_tables = [
        "transactions", "customers", "accounts", "payments", "loans",
        "credit_scores", "market_data", "trade_history", "fraud_alerts",
    ]
    ops = ["OPTIMIZE", "VACUUM", "ZORDER"]
    for day in range(90):
        d = NOW - timedelta(days=day)
        for tbl in random.sample(storage_tables, random.randint(2, 6)):
            op = random.choice(ops)
            status = "SUCCEEDED" if random.random() < 0.92 else "FAILED"
            start = d.replace(hour=2, minute=random.randint(0, 59))
            end = start + timedelta(minutes=random.randint(3, 45))
            storage_rows.append(
                f"('workspace','banking_data','{tbl}','{op}','{status}',"
                f"'{start.strftime('%Y-%m-%d %H:%M:%S')}','{end.strftime('%Y-%m-%d %H:%M:%S')}',"
                f"map('files_removed','{random.randint(1, 500)}','bytes_removed','{random.randint(10000, 50000000000)}'))"
            )
    stmts.append(("insert storage.predictive_opt",
                   f"INSERT INTO workspace.mock_system_storage.predictive_optimization_operations_history VALUES\n" + ",\n".join(storage_rows)))

    # ═══════════════════════════════════════════════════════════════════════════
    # information_schema.tables
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop information_schema.tables", "DROP TABLE IF EXISTS workspace.mock_system_information_schema.tables"))
    stmts.append(("create information_schema.tables", """
CREATE TABLE workspace.mock_system_information_schema.tables (
  table_catalog STRING,
  table_schema  STRING,
  table_name    STRING,
  table_type    STRING,
  created       TIMESTAMP
)"""))
    info_rows = []
    for tbl in storage_tables:
        info_rows.append(
            f"('workspace','banking_data','{tbl}','MANAGED',"
            f"'{(NOW - timedelta(days=900)).strftime('%Y-%m-%d %H:%M:%S')}')"
        )
    stmts.append(("insert information_schema.tables",
                   f"INSERT INTO workspace.mock_system_information_schema.tables VALUES\n" + ",\n".join(info_rows)))

    # ═══════════════════════════════════════════════════════════════════════════
    # marketplace.listings + consumers + provider_analytics
    # ═══════════════════════════════════════════════════════════════════════════
    stmts.append(("drop marketplace.listings", "DROP TABLE IF EXISTS workspace.mock_system_marketplace.listings"))
    stmts.append(("create marketplace.listings", """
CREATE TABLE workspace.mock_system_marketplace.listings (
  listing_id   STRING,
  listing_name STRING,
  provider     STRING,
  category     STRING,
  status       STRING,
  created_time TIMESTAMP
)"""))
    mkt_rows = []
    listings = [
        ("Market Risk Data Feed", "Bloomberg", "DATA"),
        ("Fraud Intelligence", "LexisNexis", "DATA"),
        ("Credit Bureau Scores", "Experian", "DATA"),
        ("Economic Indicators", "Federal Reserve", "DATA"),
        ("Geospatial Banking Data", "SafeGraph", "DATA"),
    ]
    for i, (name, provider, cat) in enumerate(listings):
        mkt_rows.append(
            f"('mkt-{i+1:03d}','{name}','{provider}','{cat}','ACTIVE',"
            f"'{(NOW - timedelta(days=random.randint(90, 700))).strftime('%Y-%m-%d %H:%M:%S')}')"
        )
    stmts.append(("insert marketplace.listings",
                   f"INSERT INTO workspace.mock_system_marketplace.listings VALUES\n" + ",\n".join(mkt_rows)))

    stmts.append(("drop marketplace.listing_access", "DROP TABLE IF EXISTS workspace.mock_system_marketplace.listing_access"))
    stmts.append(("create marketplace.listing_access", """
CREATE TABLE workspace.mock_system_marketplace.listing_access (
  listing_id      STRING,
  consumer_id     STRING,
  access_type     STRING,
  access_time     TIMESTAMP
)"""))

    stmts.append(("drop marketplace.listing_funnel_events", "DROP TABLE IF EXISTS workspace.mock_system_marketplace.listing_funnel_events"))
    stmts.append(("create marketplace.listing_funnel_events", """
CREATE TABLE workspace.mock_system_marketplace.listing_funnel_events (
  listing_id STRING,
  event_type STRING,
  event_time TIMESTAMP,
  consumer   STRING
)"""))

    # ═══════════════════════════════════════════════════════════════════════════
    # networking / lakeview / dashboards (structure only)
    # ═══════════════════════════════════════════════════════════════════════════
    for tbl_spec in [
        ("networking.private_endpoint_rules", """
CREATE TABLE workspace.mock_system_networking.private_endpoint_rules (
  account_id    STRING,
  workspace_id  STRING,
  rule_name     STRING,
  resource_type STRING,
  status        STRING,
  created_time  TIMESTAMP
)"""),
        ("networking.firewall_rules", """
CREATE TABLE workspace.mock_system_networking.firewall_rules (
  account_id   STRING,
  workspace_id STRING,
  rule_name    STRING,
  cidr_block   STRING,
  status       STRING,
  created_time TIMESTAMP
)"""),
        ("lakeview.dashboards", """
CREATE TABLE workspace.mock_system_lakeview.dashboards (
  dashboard_id STRING,
  name         STRING,
  creator      STRING,
  created_time TIMESTAMP,
  updated_time TIMESTAMP
)"""),
        ("lakeview.dashboard_usage", """
CREATE TABLE workspace.mock_system_lakeview.dashboard_usage (
  dashboard_id STRING,
  user_email   STRING,
  view_time    TIMESTAMP
)"""),
        ("dashboards.dashboards", """
CREATE TABLE workspace.mock_system_dashboards.dashboards (
  dashboard_id STRING,
  name         STRING,
  creator      STRING,
  created_time TIMESTAMP,
  updated_time TIMESTAMP
)"""),
        ("dashboards.dashboard_usage", """
CREATE TABLE workspace.mock_system_dashboards.dashboard_usage (
  dashboard_id STRING,
  user_email   STRING,
  view_time    TIMESTAMP
)"""),
    ]:
        tbl_name, create_sql = tbl_spec
        stmts.append((f"drop {tbl_name}", f"DROP TABLE IF EXISTS workspace.mock_system_{tbl_name.replace('.', '.')}"))
        stmts.append((f"create {tbl_name}", create_sql))

    return stmts


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"=" * 70)
    print(f"Enterprise Mock Data Generator")
    print(f"Target: Chase/BOFA scale — $5M forecast, 1,100 users, 3 years")
    print(f"Host: {HOST}")
    print(f"Warehouse: {WAREHOUSE_ID}")
    print(f"=" * 70)
    print()

    statements = build_statements()
    print(f"\nTotal SQL statements to execute: {len(statements)}\n")

    errors = 0
    for i, (label, sql) in enumerate(statements):
        progress = f"[{i+1}/{len(statements)}]"
        try:
            if not run_sql(f"{progress} {label}", sql.strip()):
                pass  # SKIPs are logged but not counted as errors
        except Exception as exc:
            print(f"  ERROR: {label} — {exc}")
            errors += 1

    print(f"\n{'=' * 70}")
    print(f"Done. {len(statements)} statements executed, {errors} errors.")
    print(f"{'=' * 70}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
