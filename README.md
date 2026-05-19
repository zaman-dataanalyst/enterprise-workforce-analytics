## 📊 Phase 5: Power BI Dashboard (In Progress)

### Business Problem

IT Staff Augmentation firms in Pakistan face two critical 
margin erosion problems that traditional Excel reporting 
cannot solve in time:

**Problem 1 — Bench Cost Leakage**
When employees are not assigned to any client project, they 
sit idle on "bench." The company continues paying their 
salary while generating zero revenue from them. With 5,000 
employees across 9 countries, identifying which employees 
are on bench — and for how long — was impossible in 
real time. Excel reports took 30 days to surface this 
information. By then, the financial damage was already done.

**Problem 2 — FX Volatility Impact**
The company bills international clients in USD but pays 
employee salaries in PKR. When PKR depreciates against USD 
— as it did multiple times in 2023 and 2024 — the dollar 
revenue converts to fewer rupees automatically. Profit 
margins shrink without any operational change. This currency 
risk was invisible in existing reports.

**Problem 3 — 30-Day Reporting Lag**
Traditional Excel-based reporting required 30 days to 
compile and deliver insights. Decisions were being made on 
month-old data. By the time management identified a bench 
cost spike or FX impact, the loss had already occurred.

---

### Solution

A fully automated end-to-end analytics pipeline was built 
to solve all three problems simultaneously:

**Automated Daily Pipeline**
A Python ingestion script runs automatically every night 
via GitHub Actions CI/CD. It generates fresh timesheet 
data with real-time currency conversion using the 
OpenExchangeRates (OXR) live API — ensuring every record 
reflects the actual exchange rate of that day.

**Medallion Architecture on BigQuery**
Raw data lands in the Bronze layer. dbt cleans and 
standardizes it in the Silver layer — fixing alphanumeric 
corruption, whitespace, and categorical fragmentation 
across 3.75M records. The Gold layer produces a 
Kimball-style star schema (7 dimensions + 1 fact table) 
ready for direct BI consumption.

**Data Quality Guarantee**
49 dbt tests run on every pipeline execution. An 
independent Python audit suite validates zero data loss 
between Bronze and Gold — row_diff=0, hour_diff=0, 
revenue_diff=0 across USD 1,999,685,164 total revenue.

**Power BI Dashboard — 3 Pages (In Progress)**

*Page 1 — CEO Executive View (Descriptive)*
Real-time KPIs: Total Revenue, Bench Cost, Utilization %, 
Active Headcount — each with MoM and YoY variance. 
Revenue vs Bench Cost 36-month trend. Revenue by Region 
and Department breakdown.

*Page 2 — Operations Root Cause Analysis (Diagnostic)*
AI Decomposition Tree drilling Bench Cost from Region → 
Department → Designation → Employee. FX Volatility impact 
chart showing direct correlation between PKR/USD rate and 
revenue loss. Employee performance matrix with RAG 
conditional formatting.

*Page 3 — Predictive Risk & Forecast (Predictive)*
AI Key Influencers identifying bench cost drivers. 90-day 
revenue forecast with confidence bands. At-risk bench 
employee table. Custom sklearn Linear Regression model 
showing Actual vs Predicted revenue with R² and RMSE.

**Business Impact**
Management can now identify bench cost spikes, FX 
volatility impact, and at-risk employees within hours — 
not 30 days. The pipeline runs daily with zero manual 
intervention.

---

> **Status:** Dashboard build in progress.
> Pipeline: ✅ Complete | Tests: 49/49 PASS |
> Dashboard: ⏳ In Progress
