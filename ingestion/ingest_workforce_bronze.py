"""
Script: ingest_workforce_bronze.py
Purpose: Ingests workforce timesheet data into BigQuery Bronze layer.
Architecture: Medallion (Bronze Layer - Ingestion)
Owner: Data Analytics (Zaman Yaseen)
Data Flow: Simulated HRIS + FX API -> BigQuery
"""

import os, logging, random, uuid, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from faker import Faker
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- 1. CONFIGURATION & ALERTS ---
OXR_APP_ID = os.getenv("OXR_APP_ID")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")

PROJECT_ID    = "enterprise-workforce-analytics"
DATASET_ID    = "bronze_layer"
FACT_TABLE    = "raw_hris_timesheets_v1"
RUN_ID        = str(uuid.uuid4())[:8]
HISTORY_START = datetime(2023, 1, 1).date()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
fake = Faker()
# random.seed(42)           # REMOVED: causes uniform synthetic data — no variance
# fake.seed_instance(42)    # REMOVED: causes uniform faker output — no variance

def get_clients():
    if os.path.exists("gcp-key.json"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"
    return bigquery.Client()

def send_observation_alert(message):
    if SLACK_WEBHOOK:
        try:
            payload = {"text": f"[*WORKFORCE-BRONZE-INGESTION*] | {message}"}
            requests.post(SLACK_WEBHOOK, json=payload, timeout=5)
        except Exception: pass
    logging.info(message)

# --- 2. TRANSFORMATION HELPERS ---
REGION_MAP = {
    "USA": "NA", "CA": "NA",
    "UK": "EU", "DE": "EU", "NL": "EU", "SE": "EU",
    "UAE": "MEA", "SA": "MEA", "QA": "MEA",
    "PK": "APAC", "IN": "APAC", "SG": "APAC", "AU": "APAC"
}

TZ_MAP = {
    "USA": "America/New_York",  "CA": "America/Toronto",
    "UK":  "Europe/London",     "DE": "Europe/Berlin",
    "NL":  "Europe/Amsterdam",  "SE": "Europe/Stockholm",
    "UAE": "Asia/Dubai",        "SA": "Asia/Riyadh",
    "QA":  "Asia/Qatar",        "PK": "Asia/Karachi",
    "IN":  "Asia/Kolkata",      "SG": "Asia/Singapore",
    "AU":  "Australia/Sydney"
}

CURRENCY_MAP = {
    "USA": "USD", "CA": "CAD", "UK": "GBP", "DE": "EUR", "NL": "EUR", "SE": "SEK",
    "UAE": "AED", "SA": "SAR", "QA": "QAR", "PK": "PKR", "IN": "INR", "SG": "SGD", "AU": "AUD"
}

CITY_MAP = {
    "USA": "New York",   "CA": "Toronto",   "UK": "London",    "DE": "Berlin",
    "NL":  "Amsterdam",  "SE": "Stockholm", "UAE": "Dubai",    "SA": "Riyadh",
    "QA":  "Doha",       "PK": "Lahore",    "IN": "Bangalore", "SG": "Singapore",
    "AU":  "Sydney"
}

# Bug 9 — NL/SE/QA added; Bug Issue 2 — all values are sets (O(1) lookup)
HOLIDAYS_MMDD = {
    "USA": {"01-01", "07-04", "11-28", "12-25"},
    "UK":  {"01-01", "05-06", "12-25", "12-26"},
    "UAE": {"01-01", "04-10", "12-02"},
    "PK":  {"02-05", "03-23", "08-14", "12-25"},
    "IN":  {"01-26", "08-15", "10-02"},
    "SA":  {"02-22", "09-23"},
    "CA":  {"01-01", "07-01", "12-25"},
    "DE":  {"01-01", "05-01", "10-03", "12-25"},
    "SG":  {"01-01", "08-09", "12-25"},
    "AU":  {"01-01", "01-26", "12-25"},
    "NL":  {"01-01", "04-21", "12-25", "12-26"},
    "SE":  {"01-01", "06-06", "12-25", "12-26"},
    "QA":  {"01-01", "12-18"},
}

def apply_messy(val):
    if val is None or random.random() > 0.35: return val
    noise = random.choice(["lower", "upper", "space", "typo"])
    val_str = str(val)
    if noise == "lower": return val_str.lower()
    if noise == "upper": return val_str.upper()
    if noise == "space": return f"  {val_str}  "
    if noise == "typo":  return val_str.replace("e", "a", 1).replace("i", "1", 1)
    return val

# Bug Issue 1 — moved to module level so dict is built once, not 30M+ times
HYBRID_MAPS = {
    "country":  {
        "USA": ["usa", "U.S.A", "United States", "us"],
        "UK":  ["uk", "U.K.", "United Kingdom"],
        "UAE": ["uae", "U.A.E"],
        "PK":  ["pk", "Pakistan"],
        "IN":  ["india", "ind"],
        "SA":  ["ksa", "Saudi Arabia"],
        "CA":  ["canada", "can"],
        "DE":  ["germany", "deu"],
        "SG":  ["singapore", "sg"],
        "AU":  ["australia", "aus"],
        "NL":  ["netherlands"],
        "SE":  ["sweden"],
        "QA":  ["qatar"],
    },
    "currency": {
        "USD": ["usd", "$"],  "GBP": ["gbp", "£"],  "AED": ["aed"],
        "PKR": ["pkr", "Rs"], "INR": ["inr", "₹"],  "EUR": ["eur", "€"],
        "CAD": ["cad"],       "SAR": ["sar"],        "QAR": ["qar"],
        "SGD": ["sgd"],       "AUD": ["aud"],        "SEK": ["sek"],
    },
    "emp_type": {"Full-time": ["full time", "FTE", "ft"], "Contractor": ["contract", "CWR"]},
    # Bug 17 — "On Hold" variants added
    "status":   {
        "Active":    ["active", "ACT"],
        "Completed": ["done", "Closed"],
        "On Hold":   ["on-hold", "ON_HOLD", "Paused"],
    },
    "proj_type":{"Fixed": ["fixed price", "F.P"], "T&M": ["Time & Material", "t&m"]},
    "segment":  {
        "B2B": ["b2b", "Business to Business", "Corporate"],
        "B2C": ["b2c", "Consumer"],
        "B2G": ["b2g", "Govt", "Government"],
    },
    "priority": {
        "Critical": ["P0", "critical"], "High": ["P1", "high"],
        "Medium":   ["P2", "med"],      "Low":  ["P3", "low"],
    },
}

def apply_hybrid(val, category):
    if val is None or random.random() > 0.20: return val
    return random.choice(HYBRID_MAPS.get(category, {}).get(val, [str(val).lower(), str(val).upper()]))

# --- 3. DATA QUALITY SIMULATION (NULL INJECTION) ---
def maybe_null(val, prob=0.05):
    """5% chance to return NULL, mimicking real-world missing data"""
    return None if random.random() < prob else val

# --- 4. EXCHANGE RATE (FX) INTEGRATION ---
FX_MONTHLY_MAP = {}
DEFAULT_FX = {
    "USD": 1.0, "PKR": 278.0, "INR": 83.0, "AED": 3.67, "SAR": 3.75,
    "QAR": 3.64, "GBP": 0.79, "EUR": 0.92, "CAD": 1.35, "AUD": 1.52,
    "SGD": 1.34, "SEK": 10.5, "SRC": "FALLBACK"
}

def prefetch_fx_history(start_date, end_date):
    curr = start_date.replace(day=1)
    while curr <= end_date:
        m_key = curr.strftime("%Y-%m")
        url = f"https://openexchangerates.org/api/historical/{curr.strftime('%Y-%m-%d')}.json?app_id={OXR_APP_ID}"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                rates = r.json().get('rates', {})
                fx_entry = {"USD": 1.0, "SRC": "API"}
                for c in ["PKR", "INR", "AED", "SAR", "QAR", "GBP", "EUR", "CAD", "AUD", "SGD", "SEK"]:
                    fx_entry[c] = rates.get(c, DEFAULT_FX[c])
                FX_MONTHLY_MAP[m_key] = fx_entry
            else: raise Exception()
        except:
            FX_MONTHLY_MAP[m_key] = DEFAULT_FX.copy()
        curr = (curr + timedelta(days=32)).replace(day=1)

# --- 5. MOCK DIMENSION SEEDING (5000 RECORDS) ---
# Bug 19 + 20 — BI Developer and Data Scientist added
BILLING_MATRIX = {
    "Data Engineer":  {"Junior": 65,  "Senior": 135, "Lead": 215},
    "Data Analyst":   {"Junior": 45,  "Senior": 95,  "Lead": 150},
    "QA Engineer":    {"Junior": 30,  "Senior": 70,  "Lead": 110},
    "BI Developer":   {"Junior": 55,  "Senior": 110, "Lead": 175},
    "Data Scientist": {"Junior": 80,  "Senior": 160, "Lead": 250},
}
# Bug 16 — NL/SE/QA/AU removed (not in EMPLOYEE_COUNTRIES, dead keys)
REGION_MULT = {
    "USA": 1.5, "CA": 1.4, "UK": 1.3, "DE": 1.3,
    "UAE": 1.2, "SA": 1.2, "SG": 1.2, "PK": 0.5, "IN": 0.55
}

# ── Monthly Revenue Variance Multipliers ─────────────────────────────────────
MONTHLY_REVENUE_MULT = {
    "2023-06": 0.78, "2023-07": 0.75, "2023-08": 0.82,
    "2023-12": 1.22, "2024-03": 0.85, "2024-06": 1.18,
    "2024-09": 1.28, "2024-12": 1.30, "2025-01": 0.72,
    "2025-06": 0.80, "2026-01": 0.68,
}

# ── Bench Rate Variance by Designation ───────────────────────────────────────
BENCH_PROB_BY_DESIGNATION = {
    "Data Scientist": 0.08,
    "Data Engineer":  0.12,
    "Data Analyst":   0.14,
    "BI Developer":   0.18,
    "QA Engineer":    0.28,
}
BENCH_PROB_BY_SENIORITY = {
    "Junior": 1.40,
    "Senior": 0.90,
    "Lead":   0.60,
}

# ── Department Allocation Weights ─────────────────────────────────────────────
DEPT_ALLOC_WEIGHTS = {
    "Data Engineering":       [0.10, 0.40, 0.50],
    "Analytics & BI":         [0.20, 0.50, 0.30],
    "Cloud & Infrastructure": [0.30, 0.55, 0.15],
    "Data Science & AI":      [0.40, 0.50, 0.10],
}

def seed_enterprise_dimensions():
    CLIENT_COUNTRIES = ["USA", "UK", "CA", "AU", "DE", "NL", "SE", "UAE", "SA", "QA"]
    CLIENT_WEIGHTS   = [0.35,  0.15, 0.10, 0.08, 0.07, 0.05, 0.05, 0.07,  0.05, 0.03]

    clients = {f"CLI-{i:03}": {
        "Name":     fake.company(),
        "Country":  random.choices(CLIENT_COUNTRIES, weights=CLIENT_WEIGHTS)[0],
        "Industry": random.choice(["FinTech", "Healthcare", "Retail", "EdTech"]),
        "Size":     random.choice(["Enterprise", "SMB", "Startup"]),
        "Segment":  random.choices(["B2B", "B2C", "B2G"], weights=[0.7, 0.2, 0.1])[0]
    } for i in range(1, 201)}

    # Bug 4 + 15 — MGR- prefix fixed to SYS- to match employee IDs
    mgr_ids = [f"SYS-{str(i).zfill(5)}" for i in range(1, 81)]

    projects = {}
    for j in range(1, 401):
        cli_id = random.choice(list(clients.keys()))
        projects[f"PRJ-{str(j).zfill(3)}"] = {
            "Name":         f"{fake.catch_phrase()} Implementation",
            "Client_ID":    cli_id,
            "Proj_Type":    random.choice(["Fixed", "T&M"]),
            # Bug 7 — Status variety: Active 60%, Completed 30%, On Hold 10%
            "Status":       random.choices(["Active", "Completed", "On Hold"], weights=[0.60, 0.30, 0.10])[0],
            "Priority":     random.choices(["Critical", "High", "Medium", "Low"], weights=[0.1, 0.3, 0.4, 0.2])[0],
            "Contract_Val": random.randint(50000, 1000000),
            "Skill":        random.choice(list(BILLING_MATRIX.keys())),
            "Start":        fake.date_between(start_date="-3y", end_date="-1y"),
            "End":          fake.date_between(start_date="today", end_date="+1y"),
            "PM_ID":        random.choice(mgr_ids)
        }

    EMPLOYEE_COUNTRIES = ["PK", "IN", "UAE", "SA", "USA", "UK", "CA", "DE", "SG"]
    EMPLOYEE_WEIGHTS   = [0.35, 0.25, 0.10,  0.05, 0.08,  0.07, 0.04, 0.03, 0.03]

    # Bug 3 — currency-specific salary ranges (realistic per-region bands)
    SAL_RANGE = {
        "USD": (3000,  15000), "GBP": (2500,  12000), "EUR": (2500,  11000),
        "CAD": (3500,  14000), "SGD": (4000,  18000),
        "AED": (8000,  40000), "SAR": (8000,  40000),
        "PKR": (80000, 350000), "INR": (50000, 200000),
    }
    # Bug 18 — DEPT_MAP: designation-correlated departments
    DEPT_MAP = {
        "Data Analyst":   ["Analytics & BI", "Data Science & AI"],
        "Data Engineer":  ["Data Engineering", "Cloud & Infrastructure"],
        "QA Engineer":    ["Data Engineering", "Analytics & BI"],
        "BI Developer":   ["Analytics & BI"],
        "Data Scientist": ["Data Science & AI"],
    }

    employees = {}
    for i in range(1, 5001):
        reg         = random.choices(EMPLOYEE_COUNTRIES, weights=EMPLOYEE_WEIGHTS)[0]
        curr        = CURRENCY_MAP[reg]
        designation = random.choice(list(BILLING_MATRIX.keys()))
        dept        = random.choice(DEPT_MAP[designation])
        skill       = random.choice(list(BILLING_MATRIX.keys()))
        exp_years   = random.randint(1, 12)
        # Bug 6 — seniority derived from experience (not random)
        seniority   = "Junior" if exp_years < 3 else ("Senior" if exp_years <= 6 else "Lead")
        sal_min, sal_max = SAL_RANGE.get(curr, (40000, 120000))

        employees[f"SYS-{str(i).zfill(5)}"] = {
            "Name":        fake.name(),
            "Designation": designation,
            "Seniority":   seniority,
            "Emp_Type":    random.choice(["Full-time", "Contractor"]),
            "Dept":        dept,
            "Cost_Center": f"CC-{dept[:3].upper()}-{reg}",
            "Skill":       skill,
            "Reg":         reg,
            "City":        CITY_MAP[reg],
            "TZ":          TZ_MAP[reg],
            "Currency":    curr,
            "Experience":  exp_years,
            "Mgr_ID":      random.choice(mgr_ids),
            "Join":        fake.date_between(start_date="-3y", end_date="-1y"),
            "Sal":         random.randint(sal_min, sal_max),
            # Bug 7 — 8% employees are Inactive
            "Status":      random.choices(["Active", "Inactive"], weights=[0.92, 0.08])[0],
        }
    return clients, projects, employees

# --- 6. GCP OBT DDL ---
FACT_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS `{table_id}` (
        timesheet_id STRING, emp_id STRING, manager_id STRING, project_id STRING, client_id STRING,
        work_date DATE, employee_name STRING, designation STRING, seniority_level STRING,
        employment_type STRING, employment_status STRING, department STRING, cost_center STRING,
        skill_primary STRING, experience_years INT64, location_country STRING, location_city STRING,
        location_region STRING, location_timezone STRING, salary_local FLOAT64, salary_currency STRING,
        project_name STRING, project_type STRING, project_status STRING, project_phase STRING,
        project_priority STRING, project_manager_id STRING, client_name STRING, client_industry STRING,
        client_segment STRING, client_size STRING, client_country STRING, client_region STRING,
        task_category STRING, revenue_type STRING, hours_worked FLOAT64, capacity_hours FLOAT64,
        billable_hours FLOAT64, non_billable_hours FLOAT64, available_hours FLOAT64, utilization_pct FLOAT64,
        utilization_category STRING, allocation_pct FLOAT64, hourly_rate_usd FLOAT64,
        cost_rate_local FLOAT64, revenue_usd FLOAT64, cost_usd FLOAT64, profit_usd FLOAT64,
        revenue_local FLOAT64, cost_local FLOAT64, fx_rate FLOAT64, fx_source STRING, data_quality_flag STRING,
        is_billable BOOL, is_holiday BOOL, is_anomaly BOOL, is_correction BOOL, entry_type STRING,
        original_timesheet_id STRING, record_created_at TIMESTAMP, record_updated_at TIMESTAMP,
        load_timestamp TIMESTAMP, batch_id STRING
    )
"""

def get_fact_start_date(bq_client, table_ref):
    try:
        rows = list(bq_client.query(f"SELECT MAX(work_date) AS max_date, COUNT(*) AS total FROM `{table_ref}`").result())
        max_date, total = rows[0].max_date, rows[0].total
        if max_date is None or total == 0: return HISTORY_START
        if total < 50000:
            bq_client.query(f"DROP TABLE IF EXISTS `{table_ref}`").result()
            bq_client.query(FACT_TABLE_DDL.format(table_id=table_ref)).result()
            return HISTORY_START
        return max_date + timedelta(days=1)
    except NotFound:
        bq_client.query(FACT_TABLE_DDL.format(table_id=table_ref)).result()
        return HISTORY_START

# --- 7. CORE ETL PIPELINE ---
def run_ultimate_pipeline():
    if not OXR_APP_ID: raise ValueError("Missing required environment variable: OXR_APP_ID")
    bq_client = get_clients()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{FACT_TABLE}"

    send_observation_alert(f"INFO: Ingestion started. Layer: Bronze | Batch: {RUN_ID}")
    start_dt = get_fact_start_date(bq_client, table_ref)
    end_dt = datetime.now().date()
    if start_dt > end_dt:
        send_observation_alert("INFO: Sync complete. No new data to ingest for the current period.")
        return

    prefetch_fx_history(start_dt, end_dt)
    clients, proj_db, emp_db = seed_enterprise_dimensions()

    curr_dt, buffer, total_loaded = start_dt, [], 0
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    # Bug 8 — defined once here (not 5M+ times inside the employee loop)
    # Reads enclosing-scope vars (fx, eid, emp, p_id, etc.) at call time via closure
    def build_row(h_wrk, b_hrs, nb_hrs, a_hrs, u_pct, u_cat, r_usd, c_usd, p_usd, is_corr, entry, orig_id):
        dq_flag = "VALID"
        if is_corr: dq_flag = "CORRECTED"
        elif fx["SRC"] == "FALLBACK" or h_wrk < 0 or h_wrk > 12: dq_flag = "ESTIMATED"

        return {
            "timesheet_id":         str(uuid.uuid4()) if is_corr else original_id,
            "emp_id":               eid,
            "manager_id":           emp["Mgr_ID"],
            "project_id":           p_id,
            "client_id":            client_id,
            "work_date":            curr_dt,
            "employee_name":        maybe_null(apply_messy(emp["Name"])),
            "designation":          maybe_null(apply_messy(emp["Designation"])),
            "department":           apply_messy(emp["Dept"]),
            "cost_center":          emp["Cost_Center"],
            "skill_primary":        apply_messy(emp["Skill"]),
            "location_city":        maybe_null(apply_messy(emp["City"])),
            "project_name":         maybe_null(apply_messy(p_name)),
            "client_name":          maybe_null(apply_messy(c_name)),
            "client_industry":      maybe_null(apply_messy(c_ind)),
            "task_category":        apply_messy(task_cat),
            "seniority_level":      emp["Seniority"],
            "employment_type":      apply_hybrid(emp["Emp_Type"], "emp_type"),
            "employment_status":    apply_hybrid(emp["Status"], "status"),
            "experience_years":     emp["Experience"],
            "location_country":     apply_hybrid(emp["Reg"], "country"),
            "location_region":      REGION_MAP[emp["Reg"]],
            "location_timezone":    emp["TZ"],
            "salary_local":         emp["Sal"],
            "salary_currency":      apply_hybrid(emp["Currency"], "currency"),
            "project_type":         apply_hybrid(p_type, "proj_type"),
            "project_status":       apply_hybrid(p_status, "status"),
            "project_phase":        proj_phase,
            "project_priority":     apply_hybrid(p_pri, "priority"),
            "project_manager_id":   pm_id,
            "client_segment":       apply_hybrid(c_seg, "segment"),
            "client_size":          c_size,
            "client_country":       apply_hybrid(c_ctry, "country"),
            "client_region":        c_reg,
            "revenue_type":         rev_type,
            "hours_worked":         h_wrk,
            "capacity_hours":       cap_hrs,
            "billable_hours":       b_hrs,
            "non_billable_hours":   nb_hrs,
            "available_hours":      a_hrs,
            "utilization_pct":      u_pct,
            "utilization_category": u_cat,
            "allocation_pct":       alloc_pct,
            "hourly_rate_usd":      hourly_rate,
            "cost_rate_local":      cost_rate_local,
            "revenue_usd":          r_usd,
            "cost_usd":             c_usd,
            "profit_usd":           p_usd,
            # Bug 11 — revenue_local uses client currency (cli_rate), not employee currency
            "revenue_local":        round(r_usd * cli_rate, 2),
            "cost_local":           round(c_usd * cur_rate, 2),
            "fx_rate":              cur_rate,
            "fx_source":            fx["SRC"],
            "data_quality_flag":    dq_flag,
            "is_billable":          is_billable,
            "is_holiday":           is_holiday,
            # Bug 2 — corrections must not be flagged as anomalies
            "is_anomaly":           (not is_corr) and (h_wrk < 0 or h_wrk > 12),
            "is_correction":        is_corr,
            "entry_type":           entry,
            "original_timesheet_id": orig_id,
            "record_created_at":    now_ts,
            "record_updated_at":    now_ts,
            "load_timestamp":       now_ts,
            "batch_id":             RUN_ID,
        }

    while curr_dt <= end_dt:
        now_ts  = datetime.now(timezone.utc)   # Bug 13 — one call per day, not per employee
        m_key   = curr_dt.strftime("%Y-%m")
        curr_md = curr_dt.strftime("%m-%d")
        fx      = FX_MONTHLY_MAP.get(m_key, DEFAULT_FX)

        is_weekend = curr_dt.weekday() >= 5

        # Bug 14 — pre-compute eligible projects per skill once per day (O(n²) not O(n³))
        eligible_by_skill = {
            skill: [
                pid for pid, p in proj_db.items()
                if p["Skill"] == skill and p["Start"] <= curr_dt <= p["End"]
            ]
            for skill in BILLING_MATRIX.keys()
        }

        for eid, emp in emp_db.items():
            if curr_dt < emp["Join"]: continue

            emp_holidays = HOLIDAYS_MMDD.get(emp["Reg"], set())
            is_holiday = (curr_md in emp_holidays) or (not is_weekend and random.random() < 0.02)

            # Bug 14 — use pre-computed dict instead of inner list comprehension
            eligible = eligible_by_skill.get(emp["Skill"], [])
            if is_weekend or is_holiday or not eligible:
                p_id = "BENCH"
            else:
                bench_prob = (
                    BENCH_PROB_BY_DESIGNATION.get(emp["Skill"], 0.16)
                    * BENCH_PROB_BY_SENIORITY.get(emp["Seniority"], 1.0)
                )
                bench_prob = min(bench_prob, 0.90)
                p_id = "BENCH" if random.random() < bench_prob else random.choice(eligible)

            is_billable = (p_id != "BENCH")
            # Bug 10 — bench employees always alloc_pct=0.0; hours not scaled by alloc
            alloc_pct = 0.0 if p_id == "BENCH" else random.choices([0.6, 0.8, 1.0], weights=[0.2, 0.3, 0.5])[0]
            cap_hrs   = 0.0 if (is_weekend or is_holiday) else 8.0
            if p_id == "BENCH":
                base_hrs = random.choice([2.0, 3.0, 4.0])
                hrs_wrk  = round(base_hrs * 2) / 2 if cap_hrs > 0 else 0.0
            else:
                base_hrs = 8.0
                hrs_wrk  = round(base_hrs * alloc_pct * 2) / 2 if cap_hrs > 0 else 0.0

            # Bug 5 — holiday bench entries classified as Non-Billable, not Internal
            if p_id == "BENCH" and is_holiday: rev_type = "Non-Billable"
            elif p_id == "BENCH":              rev_type = "Internal"
            elif is_billable:                  rev_type = "Billable"
            else:                              rev_type = "Non-Billable"

            if p_id == "BENCH":
                client_id = "CLI-INTERNAL"
                p_name, p_type, p_status, pm_id, p_pri = "Bench", "Internal", "Active", emp["Mgr_ID"], "Low"
                c_name, c_ind, c_size, c_ctry, c_reg, c_seg = (
                    "Internal", "Internal", "Enterprise", emp["Reg"], REGION_MAP[emp["Reg"]], "Internal"
                )
                task_cat   = "Leave/Training" if is_holiday else "Internal Training"
                proj_phase = "N/A"
            else:
                proj = proj_db[p_id]
                cli  = clients[proj["Client_ID"]]
                client_id = proj["Client_ID"]
                p_name, p_type, p_status, pm_id, p_pri = (
                    proj["Name"], proj["Proj_Type"], proj["Status"], proj["PM_ID"], proj["Priority"]
                )
                c_name, c_ind, c_size, c_ctry, c_reg, c_seg = (
                    cli["Name"], cli["Industry"], cli["Size"],
                    cli["Country"], REGION_MAP[cli["Country"]], cli["Segment"]
                )
                task_cat = random.choice(["Development", "Meeting", "Bug Fix", "Deployment"])

                elapsed_pct = (curr_dt - proj["Start"]).days / max(1, (proj["End"] - proj["Start"]).days)
                if elapsed_pct < 0.15:   proj_phase = "Planning"
                elif elapsed_pct > 0.85: proj_phase = "Closure"
                else:                    proj_phase = "Execution"

            emp_curr = emp["Currency"]
            cur_rate = fx.get(emp_curr, 1.0)
            # Bug 11 — client currency for revenue_local (not employee currency)
            cli_curr = CURRENCY_MAP.get(c_ctry, "USD")
            cli_rate = fx.get(cli_curr, 1.0)

            hourly_rate     = BILLING_MATRIX[emp["Skill"]][emp["Seniority"]] * REGION_MULT[emp["Reg"]] if is_billable else 0.0
            cost_rate_local = round(emp["Sal"] / 160, 2)
            cost_usd        = round((cost_rate_local / cur_rate) * hrs_wrk, 2)
            rev_usd         = round(hrs_wrk * hourly_rate, 2)
            profit_usd      = round(rev_usd - cost_usd, 2)

            bill_hrs  = hrs_wrk if is_billable else 0.0
            nbill_hrs = hrs_wrk if not is_billable else 0.0
            avail_hrs = max(0.0, cap_hrs - hrs_wrk)
            util_pct  = round(bill_hrs / cap_hrs, 4) if cap_hrs > 0 else 0.0

            if cap_hrs == 0:        util_cat = "Zero Capacity"
            elif util_pct < 0.5:    util_cat = "Low"
            elif util_pct <= 0.85:  util_cat = "Medium"
            else:                   util_cat = "High"

            original_id = str(uuid.uuid4())

            buffer.append(build_row(
                hrs_wrk, bill_hrs, nbill_hrs, avail_hrs, util_pct, util_cat,
                rev_usd, cost_usd, profit_usd, False, "original", None
            ))

            # 2% Correction Logic
            if hrs_wrk > 0 and random.random() < 0.02:
                corr_h  = -(round(random.uniform(0.5, hrs_wrk) * 2) / 2)
                cb_hrs  = corr_h if is_billable else 0.0
                cnb_hrs = corr_h if not is_billable else 0.0
                cr_usd  = round(corr_h * hourly_rate, 2)
                cc_usd  = round((cost_rate_local / cur_rate) * corr_h, 2)
                cp_usd  = round(cr_usd - cc_usd, 2)
                buffer.append(build_row(
                    corr_h, cb_hrs, cnb_hrs, 0.0, 0.0, util_cat,
                    cr_usd, cc_usd, cp_usd, True, "correction", original_id
                ))

        if len(buffer) >= 20000:
            df = pd.DataFrame(buffer)
            bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
            total_loaded += len(buffer)
            buffer = []
        curr_dt += timedelta(days=1)

    if buffer:
        df = pd.DataFrame(buffer)
        bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        total_loaded += len(buffer)

    send_observation_alert(f"SUCCESS: Ingestion complete. Rows: {total_loaded:,} | Batch: {RUN_ID}")

if __name__ == "__main__":
    try:
        run_ultimate_pipeline()
    except Exception as e:
        error_msg = f"ERROR: Ingestion failed. Context: {str(e)}"
        send_observation_alert(error_msg)
        raise e
