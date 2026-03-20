"""
Enterprise Workforce & Revenue Analytics — IT Services Model Simulation
Author  : Hafiz Zaman Yaseen
Stack   : Python → BigQuery → Power BI
Scale   : 5,000 employees | Kimball Star Schema | Medallion Architecture
Schedule: Daily via GitHub Actions (cron 0 2 * * *)
"""

import bisect
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd
import requests
from faker import Faker
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from tqdm import tqdm


# -----------------------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------------------
GCP_KEY_PATH     = "gcp-key.json"
DATASET_NAME     = "it_services_simulation"
TOTAL_EMPLOYEES  = 5000
BATCH_SIZE       = 150000
OXR_APP_ID       = os.getenv("OXR_APP_ID", "")
HISTORY_START    = datetime(2023, 1, 1)
BENCH_PROJECT_ID = -1
RANDOM_SEED      = None

# Employee and project master data is persisted in BigQuery so the pipeline
# reads the same stable records on every run instead of regenerating them.
EMPLOYEES_SOURCE_TABLE = "dim_employees_source"
PROJECTS_SOURCE_TABLE  = "dim_projects_source"


# -----------------------------------------------------------------------------------------
# Geography and market distribution
# -----------------------------------------------------------------------------------------
REGIONS = [
    "Pakistan", "USA", "United Kingdom",
    "UAE", "Germany", "Saudi Arabia", "Singapore",
]
REGION_WEIGHTS = [0.75, 0.08, 0.05, 0.04, 0.03, 0.03, 0.02]

CLIENT_MARKETS = [
    "USA", "United Kingdom", "United Arab Emirates", "Saudi Arabia", "Germany",
    "Canada", "Australia", "Netherlands", "Singapore", "Qatar",
]
MARKET_WEIGHTS = [0.35, 0.18, 0.12, 0.10, 0.08, 0.06, 0.04, 0.03, 0.02, 0.02]


# -----------------------------------------------------------------------------------------
# Salary model — senior roles always band above junior within the same region
# -----------------------------------------------------------------------------------------
DESIGNATION_SALARY_FACTOR = {
    "consultant":     (0.60, 0.75),
    "Analyst":        (0.75, 0.88),
    "MANAGER":        (0.88, 1.00),
    "manager":        (0.88, 1.00),
    "SENIOR ANALYST": (1.00, 1.30),
}

REGION_SALARY_BASE = {
    "Pakistan":       (80000, 600000, "PKR"),
    "USA":            (5000,  15000,  "USD"),
    "United Kingdom": (4000,  10000,  "GBP"),
    "UAE":            (15000, 40000,  "AED"),
    "Germany":        (4000,  11000,  "EUR"),
    "Saudi Arabia":   (15000, 40000,  "SAR"),
    "Singapore":      (6000,  15000,  "SGD"),
}


# -----------------------------------------------------------------------------------------
# Billing rate model — market band × yearly growth × designation premium
# -----------------------------------------------------------------------------------------
MARKET_HOURLY_RATES = {
    "USA":                  (55, 95),
    "United Kingdom":       (50, 85),
    "Australia":            (50, 80),
    "Canada":               (48, 80),
    "Germany":              (45, 75),
    "Netherlands":          (45, 75),
    "United Arab Emirates": (42, 70),
    "Saudi Arabia":         (40, 68),
    "Singapore":            (38, 65),
    "Qatar":                (40, 65),
}

DESIGNATION_RATE_FACTOR = {
    "consultant":     0.90,
    "Analyst":        1.00,
    "MANAGER":        1.40,
    "manager":        1.40,
    "SENIOR ANALYST": 1.30,
}

RATE_MULTIPLIER = {
    2023: 1.00,
    2024: 1.08,
    2025: 1.17,
    2026: 1.26,
}


# -----------------------------------------------------------------------------------------
# Billability and workload patterns
# -----------------------------------------------------------------------------------------
MONTHLY_BILLABILITY = {
    1: 0.77,  2: 0.79,  3: 0.83,  4: 0.86,
    5: 0.88,  6: 0.87,  7: 0.91,  8: 0.90,
    9: 0.88, 10: 0.85, 11: 0.83, 12: 0.79,
}

DEPT_BENCH_PROBABILITY = {
    "Engineering":  0.08,
    "Data Science": 0.10,
    "Cloud":        0.09,
    "Sales":        0.12,
    "HR":           0.30,
    "QA":           0.20,
    "Unassigned":   0.25,
}

BENCH_INTERNAL_HOURS   = [0.0, 2.0, 2.5, 3.0, 4.0]
BENCH_INTERNAL_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.10]

NON_BILLABLE_HOURS   = [2.0, 3.0, 4.0, 5.0]
NON_BILLABLE_WEIGHTS = [0.30, 0.30, 0.20, 0.20]

ALLOCATION_VALUES  = [0.60, 0.80, 1.00]
ALLOCATION_WEIGHTS = [0.20, 0.30, 0.50]


# -----------------------------------------------------------------------------------------
# Logging and seeding
# -----------------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

if RANDOM_SEED is not None:
    random.seed(RANDOM_SEED)
    Faker.seed(RANDOM_SEED)

fake = Faker()
if RANDOM_SEED is not None:
    fake.seed_instance(RANDOM_SEED)
    logging.warning("RANDOM_SEED=%s is active — for dev/testing only.", RANDOM_SEED)


# -----------------------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------------------

def get_designation_salary(region, designation):
    """Return (salary, currency) sampled from the regional band for this designation."""
    base_min, base_max, currency = REGION_SALARY_BASE[region]
    min_f, max_f = DESIGNATION_SALARY_FACTOR.get(designation, (0.75, 1.00))
    return round(random.uniform(base_min * min_f, base_max * max_f), 2), currency


def get_hourly_rate(client_country, work_date, designation):
    """Return the USD billing rate for this market, year, and designation level."""
    low, high          = MARKET_HOURLY_RATES.get(client_country, (40, 65))
    base_rate          = float(random.randint(low, high))
    year_multiplier    = RATE_MULTIPLIER.get(work_date.year, 1.26)
    designation_factor = DESIGNATION_RATE_FACTOR.get(designation, 1.00)
    return round(base_rate * year_multiplier * designation_factor, 2)


def get_billable_hours(is_weekend=False):
    """Return sampled billable hours, capped at 8."""
    if is_weekend:
        return random.choices([2.0, 2.5, 3.0, 4.0], weights=[0.30, 0.30, 0.25, 0.15], k=1)[0]
    return random.choices([8.0, 7.5, 7.0, 6.5, 6.0], weights=[0.50, 0.20, 0.15, 0.10, 0.05], k=1)[0]


def get_non_billable_hours():
    """Internal and admin work tends to be lighter than client-billable days."""
    return random.choices(NON_BILLABLE_HOURS, weights=NON_BILLABLE_WEIGHTS, k=1)[0]


def get_bench_hours():
    """Bench time is not always zero — training, meetings, and internal work still happen."""
    return random.choices(BENCH_INTERNAL_HOURS, weights=BENCH_INTERNAL_WEIGHTS, k=1)[0]


def get_allocation_factor():
    """Partial allocation applies only when the employee is staffed on a client project."""
    return random.choices(ALLOCATION_VALUES, weights=ALLOCATION_WEIGHTS, k=1)[0]


def build_project_schedule(join_date, end_date, assigned_projects):
    """
    Build a realistic staffing timeline for one employee.
    The employee cycles through their assigned projects with optional bench gaps in between.
    """
    schedule      = []
    start_keys    = []
    current_dt    = max(HISTORY_START.date(), join_date)
    end_dt        = end_date.date()
    project_cycle = list(assigned_projects)
    project_index = 0

    while current_dt < end_dt:
        project_id    = project_cycle[project_index % len(project_cycle)]
        duration_days = random.randint(28, 168)
        project_end   = min(current_dt + timedelta(days=duration_days), end_dt)
        schedule.append((project_id, current_dt, project_end))
        start_keys.append(current_dt)
        gap_days   = random.choices([0, 7, 14, 21], weights=[0.30, 0.40, 0.20, 0.10], k=1)[0]
        current_dt = project_end + timedelta(days=gap_days)
        project_index += 1

    return schedule, start_keys


def get_active_project_fast(schedule, start_keys, work_date):
    """O(log n) binary search — returns the active project_id or None if on bench."""
    if not schedule:
        return None
    idx = bisect.bisect_right(start_keys, work_date) - 1
    if idx < 0:
        return None
    project_id, start_date, end_date = schedule[idx]
    if start_date <= work_date <= end_date:
        return project_id
    return None


def is_billable_today(project_id, work_date, department):
    """
    On-project work is mostly billable but there is always some internal pull.
    Bench rows are always non-billable regardless of any other factor.
    """
    if project_id is None:
        return False
    dept_bench_prob     = DEPT_BENCH_PROBABILITY.get(department, 0.15)
    seasonal_factor     = MONTHLY_BILLABILITY.get(work_date.month, 0.85)
    non_billable_chance = dept_bench_prob * (1 - seasonal_factor + 0.10)
    non_billable_chance = max(0.03, min(0.20, non_billable_chance))
    return random.random() > non_billable_chance


def ensure_fact_timesheets_table(bq_client):
    """
    Create fact_timesheets if it does not already exist.
    Partitioned by work_date (DATE column) and clustered by emp_id, project_id.
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{bq_client.project}.{DATASET_NAME}.fact_timesheets` (
        timesheet_id          STRING    NOT NULL,
        emp_id                INT64     NOT NULL,
        project_id            INT64     NOT NULL,
        work_date             DATE      NOT NULL,
        fx_month              DATE      NOT NULL,
        created_at            TIMESTAMP NOT NULL,
        hours_worked          FLOAT64   NOT NULL,
        is_billable           BOOL      NOT NULL,
        hourly_rate_usd       FLOAT64   NOT NULL,
        revenue_usd           FLOAT64   NOT NULL,
        allocation_pct        FLOAT64   NOT NULL,
        is_anomaly            BOOL      NOT NULL,
        is_weekend_work       BOOL      NOT NULL,
        is_correction         BOOL      NOT NULL,
        entry_type            STRING    NOT NULL,
        original_timesheet_id STRING,
        load_timestamp        TIMESTAMP NOT NULL,
        batch_id              STRING    NOT NULL
    )
    PARTITION BY work_date
    CLUSTER BY emp_id, project_id
    OPTIONS (
        description = "Bronze raw fact table. All rows stored including zero-hour. Gold view filters: hours_worked != 0 AND is_correction = FALSE."
    )
    """
    bq_client.query(ddl).result()

    for alter_ddl in [
        f"ALTER TABLE `{bq_client.project}.{DATASET_NAME}.fact_timesheets` ADD COLUMN IF NOT EXISTS created_at TIMESTAMP",
        f"ALTER TABLE `{bq_client.project}.{DATASET_NAME}.fact_timesheets` ADD COLUMN IF NOT EXISTS revenue_usd FLOAT64",
        f"ALTER TABLE `{bq_client.project}.{DATASET_NAME}.fact_timesheets` ADD COLUMN IF NOT EXISTS is_anomaly BOOL",
        f"ALTER TABLE `{bq_client.project}.{DATASET_NAME}.fact_timesheets` ADD COLUMN IF NOT EXISTS is_weekend_work BOOL",
    ]:
        bq_client.query(alter_ddl).result()


def get_fact_incremental_start(bq_client, fact_table_id):
    """Return the next work_date to generate based on what is already loaded."""
    query    = f"SELECT MAX(work_date) AS max_work_date FROM `{fact_table_id}`"
    rows     = list(bq_client.query(query).result())
    max_date = rows[0].max_work_date if rows else None
    if max_date is None:
        return HISTORY_START.date(), None
    return max_date + timedelta(days=1), max_date


def get_existing_fx_keys(bq_client, fx_table_id):
    """Return existing (fx_month, currency_code) pairs so we only append new months."""
    try:
        bq_client.get_table(fx_table_id)
    except NotFound:
        return pd.DataFrame(columns=["fx_month", "currency_code"])
    rows = [
        {"fx_month": row.fx_month, "currency_code": row.currency_code}
        for row in bq_client.query(
            f"SELECT fx_month, currency_code FROM `{fx_table_id}`"
        ).result()
    ]
    return pd.DataFrame(rows, columns=["fx_month", "currency_code"])


def validate_fact_batch(batch_rows):
    """Raise if any (emp_id, work_date, entry_type) appears more than once in the batch."""
    if not batch_rows:
        return
    df_batch  = pd.DataFrame(batch_rows)
    dup_check = df_batch.duplicated(subset=["emp_id", "work_date", "entry_type"], keep=False)
    if dup_check.any():
        raise ValueError("Duplicate natural key detected: emp_id + work_date + entry_type")


# -----------------------------------------------------------------------------------------
# Pipeline
# -----------------------------------------------------------------------------------------

def run_pipeline():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH
    bq_client   = bigquery.Client()
    cfg_replace = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    cfg_append  = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    end_date   = datetime.today()
    total_days = (end_date - HISTORY_START).days + 1

    logging.info("=" * 80)
    logging.info("  START : %s", HISTORY_START.strftime("%Y-%m-%d"))
    logging.info("  END   : %s", end_date.strftime("%Y-%m-%d"))
    logging.info("  DAYS  : %s", f"{total_days:,}")
    logging.info("=" * 80)

    try:
        # ---- Step 1: Date dimension ----
        logging.info("STEP 1 | Building dim_date...")
        date_list = [HISTORY_START + timedelta(days=i) for i in range(total_days)]
        df_date = pd.DataFrame({
            "date_key":    [d.date() for d in date_list],
            "year":        [d.year for d in date_list],
            "quarter":     [(d.month - 1) // 3 + 1 for d in date_list],
            "month":       [d.month for d in date_list],
            "month_name":  [d.strftime("%B") for d in date_list],
            "day_of_week": [d.strftime("%A") for d in date_list],
            "is_weekend":  [d.weekday() >= 5 for d in date_list],
        })
        bq_client.load_table_from_dataframe(
            df_date,
            f"{bq_client.project}.{DATASET_NAME}.dim_date",
            job_config=cfg_replace,
        ).result()
        logging.info("STEP 1 | dim_date loaded: %s rows", f"{len(df_date):,}")

        # ---- Step 2: Employee dimension ----
        # On first run the data is generated and saved to a source table in BigQuery.
        # All subsequent runs read from that table so the employee records stay consistent.
        logging.info("STEP 2 | Loading employee data...")
        employees     = []
        employee_meta = {}
        emp_source_id = f"{bq_client.project}.{DATASET_NAME}.{EMPLOYEES_SOURCE_TABLE}"

        source_exists = True
        try:
            bq_client.get_table(emp_source_id)
        except NotFound:
            source_exists = False

        if source_exists:
            logging.info("STEP 2 | Reading from source table: %s", EMPLOYEES_SOURCE_TABLE)
            df_emp = bq_client.query(
                f"""
                SELECT emp_id, name, department, designation,
                       local_salary, currency, region, join_date
                FROM `{emp_source_id}`
                """
            ).to_dataframe()
            for _, row in df_emp.iterrows():
                employees.append(row.to_dict())
                employee_meta[int(row["emp_id"])] = {
                    "join_date":   pd.to_datetime(row["join_date"]).date(),
                    "department":  row["department"] if pd.notna(row["department"]) else "Unassigned",
                    "designation": row["designation"],
                }
            logging.info("STEP 2 | %s employees loaded from source table", f"{len(employees):,}")
        else:
            logging.info("STEP 2 | First run — generating %s employees...", f"{TOTAL_EMPLOYEES:,}")
            for emp_id in range(1, TOTAL_EMPLOYEES + 1):
                base_region   = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]
                designation   = random.choice(["consultant", "MANAGER", "Analyst", "SENIOR ANALYST", "manager"])
                local_salary, currency = get_designation_salary(base_region, designation)
                final_region  = base_region.upper() if random.random() < 0.15 else base_region
                name          = f"   {fake.name().lower()}  " if random.random() < 0.20 else fake.name()
                department    = None if random.random() < 0.07 else random.choice(
                    ["Engineering", "Data Science", "Cloud", "Sales", "HR", "QA"]
                )
                join_date     = fake.date_between(start_date="-4y", end_date="today")

                employees.append({
                    "emp_id":       emp_id,
                    "name":         name,
                    "department":   department,
                    "designation":  designation,
                    "local_salary": local_salary,
                    "currency":     currency,
                    "region":       final_region,
                    "join_date":    pd.to_datetime(join_date).date(),
                })
                employee_meta[emp_id] = {
                    "join_date":   join_date,
                    "department":  department if department else "Unassigned",
                    "designation": designation,
                }

            bq_client.load_table_from_dataframe(
                pd.DataFrame(employees), emp_source_id,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
            ).result()
            logging.info("STEP 2 | Source table created: %s", EMPLOYEES_SOURCE_TABLE)

        bq_client.load_table_from_dataframe(
            pd.DataFrame(employees),
            f"{bq_client.project}.{DATASET_NAME}.dim_employees",
            job_config=cfg_replace,
        ).result()
        logging.info("STEP 2 | dim_employees loaded: %s rows", f"{len(employees):,}")

        # ---- Step 3: Project dimension ----
        logging.info("STEP 3 | Loading project data...")
        projects = []
        proj_source_id = f"{bq_client.project}.{DATASET_NAME}.{PROJECTS_SOURCE_TABLE}"

        proj_exists = True
        try:
            bq_client.get_table(proj_source_id)
        except NotFound:
            proj_exists = False

        if proj_exists:
            logging.info("STEP 3 | Reading from source table: %s", PROJECTS_SOURCE_TABLE)
            df_proj = bq_client.query(
                f"""
                SELECT project_id, project_name, client_name, client_country
                FROM `{proj_source_id}`
                """
            ).to_dataframe()
            projects = df_proj.to_dict("records")
            logging.info("STEP 3 | %s projects loaded from source table", f"{len(projects):,}")
        else:
            logging.info("STEP 3 | First run — generating dim_projects...")
            projects = [{
                "project_id":     BENCH_PROJECT_ID,
                "project_name":   "Bench",
                "client_name":    "Internal",
                "client_country": "Internal",
            }]
            for project_id in range(1, 101):
                market = random.choices(CLIENT_MARKETS, weights=MARKET_WEIGHTS, k=1)[0]
                projects.append({
                    "project_id":     project_id,
                    "project_name":   f"  PRJ-{fake.word().upper()}  ",
                    "client_name":    fake.company(),
                    "client_country": market,
                })

            bq_client.load_table_from_dataframe(
                pd.DataFrame(projects), proj_source_id,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
            ).result()
            logging.info("STEP 3 | Source table created: %s", PROJECTS_SOURCE_TABLE)

        bq_client.load_table_from_dataframe(
            pd.DataFrame(projects),
            f"{bq_client.project}.{DATASET_NAME}.dim_projects",
            job_config=cfg_replace,
        ).result()
        logging.info("STEP 3 | dim_projects loaded: %s rows", f"{len(projects):,}")

        project_country = {
            p["project_id"]: p["client_country"]
            for p in projects
            if p["project_id"] != BENCH_PROJECT_ID
        }
        project_ids = list(project_country.keys())

        # ---- Step 3.1: Pre-compute project schedules ----
        logging.info("STEP 3.1 | Building project schedules...")
        employee_schedule_map = {}
        for emp_id, meta in employee_meta.items():
            project_count     = random.choices([1, 2, 3], weights=[0.60, 0.30, 0.10], k=1)[0]
            assigned_projects = random.sample(project_ids, project_count)
            schedule, start_keys = build_project_schedule(meta["join_date"], end_date, assigned_projects)
            employee_schedule_map[emp_id] = (schedule, start_keys)
        logging.info("STEP 3.1 | Schedules ready for %s employees", f"{TOTAL_EMPLOYEES:,}")

        # ---- Step 3.5: FX exchange rates ----
        # Monthly snapshots from Open Exchange Rates API.
        # Formula: API returns 1 USD = X local → we store 1 local = 1/X USD.
        # fx_month is the first day of the month and serves as the join key to the fact table.
        logging.info("STEP 3.5 | Fetching FX rates...")

        currencies      = "PKR,GBP,EUR,AED,SAR,SGD"
        fx_rows         = []
        monthly_dates   = pd.date_range(start=HISTORY_START, end=end_date, freq="MS")
        last_known_rate = {}

        for month_start in monthly_dates:
            date_str  = month_start.strftime("%Y-%m-%d")
            month_key = month_start.date()
            url = (
                f"https://openexchangerates.org/api/historical/{date_str}.json"
                f"?app_id={OXR_APP_ID}&symbols={currencies}"
            )
            fetched = False

            for attempt in range(3):
                try:
                    response = requests.get(url, timeout=15)
                    if response.status_code == 200:
                        data = response.json()
                        for currency_code, rate in data["rates"].items():
                            rate_to_usd = round(1 / rate, 6)
                            fx_rows.append({
                                "fx_month":      month_key,
                                "base_currency": "USD",
                                "currency_code": currency_code,
                                "rate_to_usd":   rate_to_usd,
                            })
                            last_known_rate[currency_code] = rate_to_usd
                        fx_rows.append({
                            "fx_month":      month_key,
                            "base_currency": "USD",
                            "currency_code": "USD",
                            "rate_to_usd":   1.000000,
                        })
                        last_known_rate["USD"] = 1.000000
                        logging.info("STEP 3.5 | FX loaded for %s", date_str)
                        fetched = True
                        break
                    logging.warning("STEP 3.5 | API warning %s attempt %s: %s",
                                    date_str, attempt + 1, response.status_code)
                except Exception as exc:
                    logging.error("STEP 3.5 | Request failed %s attempt %s: %s",
                                  date_str, attempt + 1, exc)
                time.sleep(2 ** attempt)

            if not fetched:
                logging.warning("STEP 3.5 | Using last known rates for %s", date_str)
                missing = []
                for currency_code in currencies.split(","):
                    if currency_code not in last_known_rate:
                        missing.append(currency_code)
                        continue
                    fx_rows.append({
                        "fx_month":      month_key,
                        "base_currency": "USD",
                        "currency_code": currency_code,
                        "rate_to_usd":   last_known_rate[currency_code],
                    })
                if missing:
                    raise RuntimeError(
                        f"No prior FX rate for {', '.join(missing)} on {date_str}."
                    )
                fx_rows.append({
                    "fx_month":      month_key,
                    "base_currency": "USD",
                    "currency_code": "USD",
                    "rate_to_usd":   1.000000,
                })

            time.sleep(0.5)

        if fx_rows:
            fx_table_id = f"{bq_client.project}.{DATASET_NAME}.dim_exchange_rates"
            df_fx_raw   = pd.DataFrame(fx_rows)

            if df_fx_raw.duplicated(subset=["fx_month", "currency_code"]).any():
                raise ValueError("Duplicate FX keys detected before load.")

            df_fx       = df_fx_raw.drop_duplicates(subset=["fx_month", "currency_code"])
            existing_fx = get_existing_fx_keys(bq_client, fx_table_id)

            if not existing_fx.empty:
                df_fx = df_fx.merge(
                    existing_fx, on=["fx_month", "currency_code"],
                    how="left", indicator=True,
                ).query("_merge == 'left_only'").drop(columns=["_merge"])

            if not df_fx.empty:
                bq_client.load_table_from_dataframe(
                    df_fx, fx_table_id, job_config=cfg_append
                ).result()
                logging.info("STEP 3.5 | dim_exchange_rates appended: %s new rows", f"{len(df_fx):,}")
            else:
                logging.info("STEP 3.5 | No new FX rows to append.")
        else:
            logging.error("STEP 3.5 | FX table empty — check API key.")

        # ---- Step 4: Fact timesheets ----
        # Bronze layer — all rows are stored including zero-hour bench records.
        # Gold view handles filtering: WHERE hours_worked != 0 AND is_correction = FALSE.
        # Grain: one event per employee per day per entry_type.
        logging.info("STEP 4 | Preparing fact_timesheets...")
        ensure_fact_timesheets_table(bq_client)
        logging.info("STEP 4 | Load mode: APPEND_INCREMENTAL")

        batch          = []
        total_rows     = 0
        batch_id       = f"RUN-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8]}"
        fact_table_id  = f"{bq_client.project}.{DATASET_NAME}.fact_timesheets"
        fact_start_date, last_loaded_date = get_fact_incremental_start(bq_client, fact_table_id)

        if last_loaded_date is None:
            logging.info("STEP 4 | First load — starting from %s", fact_start_date.strftime("%Y-%m-%d"))
        else:
            logging.info("STEP 4 | Resuming from %s", fact_start_date.strftime("%Y-%m-%d"))

        if fact_start_date > end_date.date():
            logging.info("STEP 4 | Already up to date — nothing to load.")

        with tqdm(total=TOTAL_EMPLOYEES, desc="Processing employees") as progress:
            for emp_id, meta in employee_meta.items():
                join_date   = meta["join_date"]
                department  = meta["department"]
                designation = meta["designation"]
                schedule, start_keys = employee_schedule_map[emp_id]

                start_date  = max(fact_start_date, join_date)
                current_day = start_date
                end_day     = end_date.date()

                while current_day <= end_day:

                    if random.random() < 0.05:  # 5% leave / absence
                        current_day += timedelta(days=1)
                        continue

                    is_weekend = current_day.weekday() >= 5
                    if is_weekend and random.random() < 0.95:
                        current_day += timedelta(days=1)
                        continue

                    active_project_id = get_active_project_fast(schedule, start_keys, current_day)
                    project_id        = active_project_id if active_project_id is not None else BENCH_PROJECT_ID
                    fx_month          = current_day.replace(day=1)

                    is_billable     = False
                    hourly_rate_usd = 0.0
                    hours_worked    = 0.0
                    allocation_pct  = 1.0
                    base_hours      = 0.0

                    if active_project_id is None:
                        hours_worked = get_bench_hours()
                    else:
                        is_billable    = is_billable_today(active_project_id, current_day, department)
                        allocation_pct = get_allocation_factor()

                        if is_billable:
                            client_country  = project_country[active_project_id]
                            hourly_rate_usd = get_hourly_rate(client_country, current_day, designation)
                            base_hours      = get_billable_hours(is_weekend)
                        else:
                            base_hours = get_non_billable_hours()

                        hours_worked = round(base_hours * allocation_pct * 2) / 2

                    if abs(hours_worked) > 8:
                        logging.warning("STEP 4 | Hours out of range %s — emp %s on %s skipped",
                                        hours_worked, emp_id, current_day)
                        current_day += timedelta(days=1)
                        continue

                    if not (0 < allocation_pct <= 1):
                        logging.warning("STEP 4 | Allocation out of range %s — emp %s on %s skipped",
                                        allocation_pct, emp_id, current_day)
                        current_day += timedelta(days=1)
                        continue

                    work_timestamp  = pd.to_datetime(current_day)
                    work_date_value = work_timestamp.date()
                    revenue_usd     = round(hours_worked * hourly_rate_usd, 2) if is_billable else 0.0
                    is_anomaly      = hours_worked < 0
                    is_weekend_work = is_weekend

                    original_timesheet_id = str(uuid.uuid4())
                    batch.append({
                        "timesheet_id":          original_timesheet_id,
                        "emp_id":                emp_id,
                        "project_id":            project_id,
                        "work_date":             work_date_value,
                        "fx_month":              fx_month,
                        "created_at":            work_timestamp,
                        "hours_worked":          hours_worked,
                        "is_billable":           is_billable,
                        "hourly_rate_usd":       hourly_rate_usd,
                        "revenue_usd":           revenue_usd,
                        "allocation_pct":        allocation_pct,
                        "is_anomaly":            is_anomaly,
                        "is_weekend_work":       is_weekend_work,
                        "is_correction":         False,
                        "entry_type":            "original",
                        "original_timesheet_id": None,
                        "load_timestamp":        datetime.utcnow(),
                        "batch_id":              batch_id,
                    })

                    latest_version = {
                        "timesheet_id":    original_timesheet_id,
                        "hourly_rate_usd": hourly_rate_usd,
                        "is_billable":     is_billable,
                    }

                    # ~2% of rows get a correction entry to simulate real-world timesheet adjustments
                    if hours_worked > 0 and random.random() < 0.02:
                        max_corr_hours   = max(0.5, min(4.0, abs(hours_worked)))
                        correction_hours = -(round(random.uniform(0.5, max_corr_hours) * 2) / 2)
                        correction_id    = str(uuid.uuid4())

                        batch.append({
                            "timesheet_id":          correction_id,
                            "emp_id":                emp_id,
                            "project_id":            project_id,
                            "work_date":             work_date_value,
                            "fx_month":              fx_month,
                            "created_at":            work_timestamp,
                            "hours_worked":          correction_hours,
                            "is_billable":           latest_version["is_billable"],
                            "hourly_rate_usd":       latest_version["hourly_rate_usd"],
                            "revenue_usd":           (
                                round(correction_hours * latest_version["hourly_rate_usd"], 2)
                                if latest_version["is_billable"] else 0.0
                            ),
                            "allocation_pct":        allocation_pct,
                            "is_anomaly":            True,
                            "is_weekend_work":       is_weekend_work,
                            "is_correction":         True,
                            "entry_type":            "correction",
                            "original_timesheet_id": latest_version["timesheet_id"],
                            "load_timestamp":        datetime.utcnow(),
                            "batch_id":              batch_id,
                        })

                    if len(batch) >= BATCH_SIZE:
                        validate_fact_batch(batch)
                        bq_client.load_table_from_dataframe(
                            pd.DataFrame(batch), fact_table_id, job_config=cfg_append
                        ).result()
                        total_rows += len(batch)
                        logging.info("STEP 4 | Batch uploaded. Total rows: %s", f"{total_rows:,}")
                        batch = []

                    current_day += timedelta(days=1)

                progress.update(1)

        if batch:
            validate_fact_batch(batch)
            bq_client.load_table_from_dataframe(
                pd.DataFrame(batch), fact_table_id, job_config=cfg_append
            ).result()
            total_rows += len(batch)

        logging.info("=" * 80)
        logging.info("PIPELINE COMPLETE")
        logging.info("  batch_id    : %s", batch_id)
        logging.info("  rows loaded : %s", f"{total_rows:,}")
        logging.info("=" * 80)

    except Exception as exc:
        logging.error("PIPELINE FAILED: %s", exc)
        raise


if __name__ == "__main__":
    run_pipeline()
