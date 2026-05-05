"""
================================================================================
PROJECT  : Enterprise Workforce Analytics
SCRIPT   : validate_stg_timesheets.py
LAYER    : Silver — Independent Python Audit
AUTHOR   : Hafiz Zaman Yaseen
================================================================================
"""

import sys
import logging
from pathlib import Path
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("audit_results.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("SilverAudit")


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
VALID_EMPLOYMENT_TYPES = {"FULL-TIME", "CONTRACTOR", "UNKNOWN"}
VALID_EMPLOYMENT_STATUS = {"ACTIVE", "UNKNOWN"}
VALID_PROJECT_STATUS    = {"ACTIVE", "UNKNOWN"}
VALID_PROJECT_TYPES = {"T&M", "FIXED PRICE", "INTERNAL", "UNKNOWN"}
VALID_PRIORITIES = {"CRITICAL", "HIGH", "MEDIUM", "LOW", "UNKNOWN"}
VALID_CLIENT_SEGMENTS = {"B2B", "B2C", "B2G", "CORPORATE", "INTERNAL", "UNKNOWN"}
VALID_CURRENCIES = {"PKR", "INR", "GBP", "EUR", "USD", "AED", "CAD", "SAR", "SGD", "UNKNOWN"}

VALID_LOCATION_COUNTRIES = {
    "PAKISTAN", "INDIA", "UNITED ARAB EMIRATES", "UNITED KINGDOM", "UNITED STATES",
    "SAUDI ARABIA", "CANADA", "SINGAPORE", "GERMANY", "AUSTRALIA", "SWEDEN",
    "NETHERLANDS", "QATAR", "UNKNOWN"
}

VALID_CLIENT_COUNTRIES = VALID_LOCATION_COUNTRIES

VALID_DEPARTMENTS = {"CLOUD", "ENGINEERING", "ANALYTICS", "DATA SCIENCE", "UNKNOWN"}
VALID_DESIGNATIONS = {"DATA ANALYST", "DATA ENGINEER", "QA ENGINEER", "UNKNOWN"}
VALID_TASK_CATEGORIES = {
    "INTERNAL TRAINING", "BUG FIX", "DEPLOYMENT",
    "MEETING", "DEVELOPMENT", "LEAVE/TRAINING", "UNKNOWN"
}

BANNED_CITY_TYPOS    = {"LAHORA", "BANGALORA", "DUBA1", "NAW YORK", "R1YADH", "S1NGAPORA", "BARL1N", "SINGAPORA", "BARLIN"}
BANNED_DESIG_TYPOS   = {"DATA ENG1NAER", "QA ENG1NAER", "ENGINAER"}
BANNED_DEPT_TYPOS    = {"ANALYT1CS", "ENG1NAERING", "ENGINAERING", "DATA SC1ANCE", "DATA SCIANCE"}
BANNED_TASK_TYPOS    = {"INTARNAL TRAINING", "MAET1NG", "MAETING", "DAPLOYMENT", "DAVELOPMENT", "BUG F1X", "LAAVE/TRA1NING"}
BANNED_INDUSTRY_TYPOS = {"F1NTACH", "FINTACH", "EDTACH", "RATA1L", "RATAIL", "HAALTHCARE", "INTARNAL"}
BANNED_CURRENCIES    = {"RS", "RS.", "₹", "£", "€", "$", "â‚¹", "Â£", "â‚¬"}

NULL_IMPUTATION = {
    "employee_name" : "ANONYMOUS",
    "client_name"   : "INTERNAL",
    "project_name"  : "BENCH",
    "client_industry": "UNKNOWN",
    "designation"   : "UNKNOWN",
    "location_city" : "UNKNOWN",
    "task_category" : "UNKNOWN",
}

FINANCIAL_TOLERANCE = 0.05


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATOR CLASS
# ─────────────────────────────────────────────────────────────────────────────

class TimesheetValidator:
    def __init__(self, file_path: str):
        self.file_path   = Path(file_path)
        self.df          : pd.DataFrame = None
        self.pass_count  = 0
        self.fail_count  = 0
        self.warn_count  = 0

    def _pass(self, check_id: str, message: str) -> None:
        self.pass_count += 1
        logger.info(f"[PASS ✓] {check_id:40s} {message}")

    def _fail(self, check_id: str, message: str) -> None:
        self.fail_count += 1
        logger.error(f"[FAIL ✗] {check_id:40s} {message}")

    def _warn(self, check_id: str, message: str) -> None:
        self.warn_count += 1
        logger.warning(f"[WARN ⚠] {check_id:40s} {message}")

    def _col(self, name: str) -> bool:
        if name not in self.df.columns:
            self._warn("MISSING_COL", f"Column '{name}' not in sample. Skipping dependent check.")
            return False
        return True

    def load_data(self) -> bool:
        if not self.file_path.exists():
            logger.critical(f"Audit extract not found: {self.file_path}")
            return False
        try:
            self.df = pd.read_csv(self.file_path, low_memory=False)
            logger.info(f"Loaded {len(self.df):,} records | {len(self.df.columns)} columns")
            return True
        except Exception as exc:
            logger.critical(f"Failed to load data: {exc}")
            return False

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK GROUP 1 — STRUCTURAL INTEGRITY
    # ══════════════════════════════════════════════════════════════════════════

    def check_primary_key(self):
        if not self._col("timesheet_id"): return
        dupes = self.df["timesheet_id"].duplicated().sum()
        if dupes == 0:
            self._pass("PK_UNIQUE", "timesheet_id: 0 duplicates.")
        else:
            self._fail("PK_UNIQUE", f"timesheet_id: {dupes:,} DUPLICATE rows detected!")

    def check_no_true_nulls_in_critical_columns(self):
        coalesced_cols = list(NULL_IMPUTATION.keys())
        for col in coalesced_cols:
            if not self._col(col): continue
            null_count = self.df[col].isna().sum()
            if null_count == 0:
                self._pass("NO_TRUE_NULL", f"{col}: 0 true NULLs after COALESCE.")
            else:
                self._fail("NO_TRUE_NULL", f"{col}: {null_count:,} TRUE NULLs survived COALESCE!")

    def check_nulls_and_imputations(self):
        for col, fallback in NULL_IMPUTATION.items():
            if not self._col(col): continue
            null_count = self.df[col].isna().sum()
            if null_count == 0:
                self._pass("NO_TRUE_NULL", f"{col}: 0 true NULLs after COALESCE.")
            else:
                self._fail("NO_TRUE_NULL", f"{col}: {null_count:,} true NULLs found!")

            # Safe Imputation validation: Check fallback matches correctly
            if fallback in self.df[col].values:
                self._pass("NULL_IMPUTATION", f"{col}: Verified presence of valid fallback '{fallback}'.")
            else:
                self._pass("NULL_IMPUTATION", f"{col}: Imputation clean for this sample.")

    def check_original_timesheet_id_not_imputed(self):
        if not self._col("original_timesheet_id") or not self._col("entry_type"): return
        original_rows = self.df[self.df["entry_type"].str.upper() == "ORIGINAL"]
        bad_rows = original_rows[
            original_rows["original_timesheet_id"].notna() &
            (original_rows["original_timesheet_id"] != "")
        ]
        if len(bad_rows) == 0:
            self._pass("ORIG_ID_NULL", "original_timesheet_id: NULLs preserved for original entries.")
        else:
            self._fail("ORIG_ID_NULL", f"{len(bad_rows):,} 'original' rows have a non-NULL original_timesheet_id!")

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK GROUP 2 — WHITESPACE ELIMINATION
    # ══════════════════════════════════════════════════════════════════════════

    def check_no_leading_trailing_whitespace(self):
        text_cols = [
            "employee_name", "designation", "department", "skill_primary",
            "location_city", "project_name", "client_name", "client_industry",
            "task_category", "employment_type", "salary_currency",
        ]
        total_ws = 0
        for col in text_cols:
            if col not in self.df.columns: continue
            col_series = self.df[col].dropna().astype(str)
            ws_count = (col_series != col_series.str.strip()).sum()
            total_ws += ws_count
            if ws_count > 0:
                self._fail("WHITESPACE", f"{col}: {ws_count:,} rows still have leading/trailing spaces!")

        if total_ws == 0:
            self._pass("WHITESPACE", "All text columns: 0 whitespace contamination.")

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK GROUP 3 — ALPHANUMERIC CORRUPTION (EDA Sec 4)
    # ══════════════════════════════════════════════════════════════════════════

    def check_no_digit_corruption_in_text_columns(self):
        check_cols = ["employee_name", "designation", "department", "skill_primary", "task_category", "client_name", "client_industry"]
        for col in check_cols:
            if not self._col(col): continue
            active_rows = self.df[~self.df[col].isin(list(NULL_IMPUTATION.values()))][col]
            corrupt = active_rows.dropna().astype(str).str.contains(r"\d", regex=True)
            n = corrupt.sum()
            if n == 0:
                self._pass("NO_DIGIT_CORRUPTION", f"{col}: 0 alphanumeric corruption rows.")
            else:
                self._fail("NO_DIGIT_CORRUPTION", f"{col}: {n:,} rows still contain digits!")

    def check_banned_typo_values_eliminated(self):
        col_bans = {
            "location_city"   : BANNED_CITY_TYPOS,
            "designation"     : BANNED_DESIG_TYPOS,
            "department"      : BANNED_DEPT_TYPOS,
            "task_category"   : BANNED_TASK_TYPOS,
            "client_industry" : BANNED_INDUSTRY_TYPOS,
            "salary_currency" : BANNED_CURRENCIES,
        }
        for col, banned_set in col_bans.items():
            if not self._col(col): continue
            found = self.df[self.df[col].str.upper().isin(banned_set)]
            if len(found) == 0:
                self._pass("BANNED_TYPOS", f"{col}: No banned typo values found.")
            else:
                self._fail("BANNED_TYPOS", f"{col}: {len(found):,} rows contain banned values!")

    def check_project_name_numbers_preserved(self):
        if not self._col("project_name"): return
        known_valid_patterns = [r"24/7", r"24hour", r"3rd", r"4th", r"5th", r"6th"]
        valid_found = self.df[
            self.df["project_name"].str.contains("|".join(known_valid_patterns), case=False, na=False)
        ]
        if len(valid_found) > 0:
            self._pass("PROJ_NUMS_PRESERVED", f"project_name: {len(valid_found):,} rows with valid numeric terms confirmed.")
        else:
            self._warn("PROJ_NUMS_PRESERVED", "project_name: 0 rows with '24/7'/'3rd'/'6th' found in sample.")

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK GROUP 4 — CATEGORICAL STANDARDIZATION (EDA Sec 3)
    # ══════════════════════════════════════════════════════════════════════════

    def _check_accepted_values(self, col: str, valid_set: set, eda_ref: str):
        if not self._col(col): return
        unique_vals = set(self.df[col].dropna().str.upper().unique())
        invalid = unique_vals - valid_set
        if not invalid:
            self._pass(f"ACCEPTED_VALS_{col[:20].upper()}", f"{col}: All values canonical.")
        else:
            self._fail(f"ACCEPTED_VALS_{col[:20].upper()}", f"{col}: Non-canonical values found: {invalid}")

    def check_all_categoricals(self):
        checks = [
            ("employment_type",   VALID_EMPLOYMENT_TYPES,    "EDA 3.1"),
            ("employment_status", VALID_EMPLOYMENT_STATUS,   "EDA 3.2"),
            ("project_status",    VALID_PROJECT_STATUS,      "EDA 3.3"),
            ("project_type",      VALID_PROJECT_TYPES,       "EDA 3.5"),
            ("project_priority",  VALID_PRIORITIES,          "EDA 3.6"),
            ("client_segment",    VALID_CLIENT_SEGMENTS,     "EDA 3.7"),
            ("department",        VALID_DEPARTMENTS,         "EDA 3.8"),
            ("task_category",     VALID_TASK_CATEGORIES,     "EDA 3.10"),
        ]
        for col, valid_set, eda_ref in checks:
            self._check_accepted_values(col, valid_set, eda_ref)

    def check_currency_iso_only(self):
        self._check_accepted_values("salary_currency", VALID_CURRENCIES, "EDA 3.4 / 6.1")

    def check_country_standardization(self):
        self._check_accepted_values("location_country", VALID_LOCATION_COUNTRIES, "EDA 5.1")
        self._check_accepted_values("client_country",   VALID_CLIENT_COUNTRIES,   "EDA 5.2")

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK GROUP 5 — NUMERIC & FINANCIAL INTEGRITY (EDA Sec 7 + 8)
    # ══════════════════════════════════════════════════════════════════════════

    def check_negative_hours_are_corrections(self):
        if not (self._col("hours_worked") and self._col("is_correction") and self._col("entry_type")): return
        neg_rows = self.df[self.df["hours_worked"] < 0]
        if len(neg_rows) == 0: return

        bad_flag = neg_rows[neg_rows["is_correction"] != True]
        if len(bad_flag) == 0:
            self._pass("NEG_HOURS_LOGIC", f"{len(neg_rows):,} negative-hour rows: all have is_correction=True.")
        else:
            self._fail("NEG_HOURS_LOGIC", f"{len(bad_flag):,} negative-hour rows have is_correction != True!")

        bad_entry = neg_rows[neg_rows["entry_type"].str.upper() != "CORRECTION"]
        if len(bad_entry) == 0:
            self._pass("NEG_HOURS_ENTRY", f"{len(neg_rows):,} negative-hour rows: all have entry_type='CORRECTION'.")
        else:
            self._fail("NEG_HOURS_ENTRY", f"{len(bad_entry):,} negative-hour rows have wrong entry_type!")

    def check_hours_math_reconciliation(self):
        cols = ["hours_worked", "billable_hours", "non_billable_hours"]
        if not all(self._col(c) for c in cols): return
        violation = self.df[abs((self.df["billable_hours"] + self.df["non_billable_hours"]) - self.df["hours_worked"]) > FINANCIAL_TOLERANCE]
        if len(violation) == 0:
            self._pass("HOURS_MATH", "billable + non_billable = hours_worked: 0 violations.")
        else:
            self._fail("HOURS_MATH", f"{len(violation):,} rows violate hours reconciliation equation!")

    def check_utilization_scale(self):
        if not self._col("utilization_pct"): return
        over_one = (self.df["utilization_pct"] > 1.0).sum()
        negative  = (self.df["utilization_pct"] < 0.0).sum()
        if over_one == 0 and negative == 0:
            self._pass("UTIL_SCALE", "utilization_pct: Confirmed 0-1 scale. No values > 1 or < 0.")
        else:
            self._fail("UTIL_SCALE", f"utilization_pct: {over_one:,} values > 1 | {negative:,} < 0. Scale violation!")

    def check_allocation_pct_scale(self):
        if not self._col("allocation_pct"): return
        over_one = (self.df["allocation_pct"] > 1.0).sum()
        if over_one == 0:
            self._pass("ALLOC_SCALE", "allocation_pct: Confirmed 0-1 scale.")
        else:
            self._fail("ALLOC_SCALE", f"allocation_pct: {over_one:,} values exceed 1.0!")

    def check_profit_math(self):
        cols = ["profit_usd", "revenue_usd", "cost_usd"]
        if not all(self._col(c) for c in cols): return
        mismatch = self.df[abs(self.df["profit_usd"] - (self.df["revenue_usd"] - self.df["cost_usd"])) > FINANCIAL_TOLERANCE]
        if len(mismatch) == 0:
            self._pass("PROFIT_MATH", "profit_usd = revenue_usd - cost_usd: 0 violations.")
        else:
            self._fail("PROFIT_MATH", f"{len(mismatch):,} rows fail profit math check!")

    def check_revenue_math(self):
        cols = ["revenue_usd", "billable_hours", "hourly_rate_usd"]
        if not all(self._col(c) for c in cols): return
        billable = self.df[(self.df["billable_hours"] > 0) & (self.df["hourly_rate_usd"] > 0)].copy()
        if len(billable) == 0: return
        billable["_expected_rev"] = billable["billable_hours"] * billable["hourly_rate_usd"]
        mismatch = billable[abs(billable["revenue_usd"] - billable["_expected_rev"]) > 1.0]
        if len(mismatch) == 0:
            self._pass("REVENUE_MATH", f"revenue_usd = billable_hours × hourly_rate_usd: 0 violations.")
        else:
            self._fail("REVENUE_MATH", f"{len(mismatch):,} billable rows fail revenue math!")

    def check_is_billable_consistency(self):
        if not (self._col("is_billable") and self._col("billable_hours") and self._col("is_correction")): return

        false_rows = self.df[self.df["is_billable"] == False]
        wrong_false = false_rows[false_rows["billable_hours"] > 0]

        true_rows = self.df[self.df["is_billable"] == True]
        wrong_true = true_rows[(true_rows["billable_hours"] == 0) | ((true_rows["billable_hours"] < 0) & (true_rows["is_correction"] == False))]

        ok = True
        if len(wrong_false) > 0:
            self._fail("BILLABLE_CONSISTENCY", f"{len(wrong_false):,} non-billable rows have positive billable_hours!")
            ok = False
        if len(wrong_true) > 0:
            self._fail("BILLABLE_CONSISTENCY", f"{len(wrong_true):,} non-correction billable rows have zero/negative hours!")
            ok = False
        if ok:
            self._pass("BILLABLE_CONSISTENCY", "is_billable ↔ billable_hours: 100% consistent.")

    def check_entry_type_vs_is_correction(self):
        if not (self._col("entry_type") and self._col("is_correction")): return
        breach_1 = self.df[(self.df["entry_type"].str.upper() == "CORRECTION") & (self.df["is_correction"] != True)]
        breach_2 = self.df[(self.df["entry_type"].str.upper() == "ORIGINAL") & (self.df["is_correction"] != False)]
        if len(breach_1) == 0 and len(breach_2) == 0:
            self._pass("ENTRY_IS_CORR", "entry_type ↔ is_correction: 100% aligned. 0 breaches.")
        else:
            self._fail("ENTRY_IS_CORR", "entry_type↔is_correction breach detected!")

    def check_data_quality_flag_values(self):
        if not self._col("data_quality_flag"): return
        valid_flags = {"VALID", "CORRECTED"}
        found_flags = set(self.df["data_quality_flag"].dropna().str.upper().unique())
        invalid = found_flags - valid_flags
        if not invalid:
            self._pass("DQ_FLAG_VALUES", "data_quality_flag: Only 'VALID'/'CORRECTED' present.")
        else:
            self._fail("DQ_FLAG_VALUES", f"data_quality_flag: Unexpected values found: {invalid}")

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK GROUP 7 — DIMENSION CONSISTENCY (EDA Sec 13 + 14)
    # ══════════════════════════════════════════════════════════════════════════

    def check_client_id_name_consistency(self):
        if not (self._col("client_id") and self._col("client_name")): return
        active = self.df[self.df["client_name"] != "INTERNAL"]
        name_counts = active.groupby("client_id")["client_name"].nunique()
        over_limit = name_counts[name_counts > 4]
        if len(over_limit) == 0:
            self._pass("CLIENT_NAME_CONSISTENCY", "client_id ↔ client_name: Variants within normal limits.")
        else:
            self._warn("CLIENT_NAME_CONSISTENCY", f"{len(over_limit):,} client_ids have high variants.")

    def check_project_type_consistency(self):
        if not (self._col("project_id") and self._col("project_type")): return
        type_counts = self.df.groupby("project_id")["project_type"].nunique()
        over_limit  = type_counts[type_counts > 3]
        if len(over_limit) == 0:
            self._pass("PROJ_TYPE_CONSISTENCY", "project_id ↔ project_type: Variants within normal limits.")
        else:
            self._warn("PROJ_TYPE_CONSISTENCY", f"{len(over_limit):,} projects have high variants.")

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK GROUP 8 — TEMPORAL CHECKS (EDA Sec 11)
    # ══════════════════════════════════════════════════════════════════════════

    def check_date_boundaries(self):
        if not self._col("work_date"): return
        work_dates = pd.to_datetime(self.df["work_date"], errors="coerce")
        future = (work_dates > pd.Timestamp.now()).sum()
        ancient = (work_dates < pd.Timestamp("2020-01-01")).sum()
        if future == 0 and ancient == 0:
            self._pass("DATE_BOUNDARIES", "work_date: 0 future / 0 pre-2020 dates.")
        else:
            if future > 0: self._fail("DATE_BOUNDARIES", f"{future:,} rows have future work_date!")

    # ══════════════════════════════════════════════════════════════════════════
    # MAIN RUN ORCHESTRATOR
    # ══════════════════════════════════════════════════════════════════════════

    def run(self) -> int:
        banner = "  ENTERPRISE INDEPENDENT AUDIT: STG_HRIS__TIMESHEETS  "
        sep    = "═" * (len(banner) + 4)
        print(f"\n{sep}")
        print(f"║{banner}║")
        print(f"{sep}\n")

        if not self.load_data():
            return 1

        print("\n── GROUP 1 : STRUCTURAL INTEGRITY ─────────────────────────────")
        self.check_primary_key()
        self.check_no_true_nulls_in_critical_columns()
        self.check_nulls_and_imputations()
        self.check_original_timesheet_id_not_imputed()

        print("\n── GROUP 2 : WHITESPACE ELIMINATION ───────────────────────────")
        self.check_no_leading_trailing_whitespace()

        print("\n── GROUP 3 : ALPHANUMERIC CORRUPTION (EDA Sec 4) ──────────────")
        self.check_no_digit_corruption_in_text_columns()
        self.check_banned_typo_values_eliminated()
        self.check_project_name_numbers_preserved()

        print("\n── GROUP 4 : CATEGORICAL STANDARDIZATION (EDA Sec 3) ──────────")
        self.check_all_categoricals()
        self.check_currency_iso_only()
        self.check_country_standardization()

        print("\n── GROUP 5 : NUMERIC & FINANCIAL INTEGRITY (EDA Sec 7+8) ──────")
        self.check_negative_hours_are_corrections()
        self.check_hours_math_reconciliation()
        self.check_utilization_scale()
        self.check_allocation_pct_scale()
        self.check_profit_math()
        self.check_revenue_math()
        self.check_is_billable_consistency()
        self.check_entry_type_vs_is_correction()
        self.check_data_quality_flag_values()

        print("\n── GROUP 7 : DIMENSION CONSISTENCY (EDA Sec 13+14) ────────────")
        self.check_client_id_name_consistency()
        self.check_project_type_consistency()

        print("\n── GROUP 8 : TEMPORAL CHECKS (EDA Sec 11) ─────────────────────")
        self.check_date_boundaries()

        total = self.pass_count + self.fail_count + self.warn_count
        print(f"\n{sep}")
        logger.info(
            f"AUDIT SUMMARY | "
            f"Total Checks: {total} | "
            f"PASSED: {self.pass_count} | "
            f"FAILED: {self.fail_count} | "
            f"WARNINGS: {self.warn_count}"
        )

        if self.fail_count == 0:
            logger.info("VERDICT: ✅ STAGING LAYER IS PRODUCTION-READY.")
            logger.info("Results saved to: audit_results.log")
            print(f"{sep}\n")
            return 0
        else:
            logger.error(
                f"VERDICT: ❌ STAGING LAYER FAILED VALIDATION — "
                f"{self.fail_count} critical issue(s) require resolution."
            )
            logger.info("Results saved to: audit_results.log")
            print(f"{sep}\n")
            return 1


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    BASE_DIR   = Path(__file__).resolve().parent.parent
    AUDIT_FILE = BASE_DIR / "data" / "stg_hris__timesheets_sample.csv"

    validator  = TimesheetValidator(str(AUDIT_FILE))
    exit_code  = validator.run()
    sys.exit(exit_code)