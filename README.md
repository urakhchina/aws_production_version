# Mariano on the Road - Production

An end-to-end sales intelligence platform that predicts every account’s next order, tracks portfolio health, and powers automated email outreach—daily purchase reminders to customers and weekly action digests to sales reps.

## Table of Contents
1. [Overview](#overview)  
2. [Project Structure](#project-structure)  
3. [Core Features](#core-features)  
4. [System Architecture](#system-architecture)  
5. [Endpoints](#endpoints)  
6. [Scheduled Tasks](#scheduled-tasks)  
7. [Deployment](#deployment)  
8. [Technical Notes](#technical-notes)

---

## Overview

## Overview

**Mariano on the Road** is a data-driven sales-intelligence platform designed to optimize customer engagement and retention. The system:

- Processes historical and weekly sales data from multiple distributors  
- Calculates predictive metrics such as next expected purchase dates, account-health scores, and portfolio-level pacing  
- **Sends automated purchase-reminder emails directly to customers on their predicted due dates, records whether they order, and escalates non-purchasers for rep follow-up**  
- Sends automated weekly digest emails to sales representatives with prioritized action lists  
- Provides interactive dashboards for sales reps and managers to drill into accounts, products, and trends  
- Tracks product coverage and surfaces cross-sell / upsell recommendations  
- Identifies at-risk accounts early so teams can intervene proactively  

This platform eliminates the need for third-party automation services by delivering a fully customized, end-to-end solution within our own infrastructure.


---

## Project Structure

```
project/ 
  ├── app.py                    # Main Flask application with endpoint registration
  ├── config.py                 # Configuration settings with environment variable support
  ├── models.py                 # SQLAlchemy database models
  ├── pipeline.py               # Data processing and analytics pipeline
  ├── scheduler_custom.py       # Automated weekly digest scheduler
  ├── requirements.txt          # Project dependencies
  ├── data/
  │   ├── raw/                  # Weekly sales data files
  │   ├── processed/            # Processed output files
  │   └── uploads/              # Temporary storage for webhook uploads
  ├── routes/
  │   ├── api_routes.py                # Core API endpoints
  │   ├── api_routes_churn.py          # Churn analysis endpoints
  │   ├── api_routes_historical.py     # Historical data analysis endpoints
  │   ├── api_routes_strategic.py      # Strategic account endpoints
  │   ├── compatibility_routes.py      # Legacy support endpoints
  │   ├── dashboard_routes.py          # Dashboard UI routes
  │   └── webhook_routes.py            # Data ingestion webhook endpoints
  ├── services/
  │   ├── communication_engine.py      # Weekly digest generation and sending
  │   ├── email_service.py             # Email delivery functionality
  │   └── ai_suggestion_service.py     # AI-powered recommendations
  ├── static/                   # CSS, JavaScript, and static assets
  └── templates/                # HTML templates for dashboards
      ├── churn_dashboard.html
      ├── strategic_dashboard.html
      └── sales_manager_dashboard.html
```

---

## Core Features

### 1. Advanced Analytics & Prediction
- **Next-Purchase Forecasting** – machine-learning models predict each account’s next expected order date from historical cadence and velocity  
- **Account-Health Scoring** – composite score blending recency, frequency, monetary value, pace-to-plan, and product-coverage signals  
- **Portfolio Pace & YoY Metrics** – real-time year-over-year revenue/transaction deltas and pacing to quota at rep, region, and company levels  

### 2. Automated Communication
- **On-Due-Date Customer Reminders** – the platform emails stores automatically on their predicted due date, logs the send, and tracks whether an order follows  
- **Weekly Digest to Reps** – personalized Monday email summarising:  
  - Accounts due this week  
  - Accounts reminded last week that **still haven’t purchased** (“Action Needed”)  
  - Cross- and up-sell recommendations from product-coverage analysis  
  - Key portfolio KPIs (overdue %, low-health count, strong pace, etc.)  

### 3. Interactive Dashboards
- **Strategic Dashboard** – drillable overview of high-priority and high-potential accounts with quick-action links  
- **Engagement Dashboard** – live status of customer reminders (Sent / Purchased / No Action) so reps can follow up immediately  
- **Sales-Manager Dashboard** – roll-up view of team performance, pipeline pacing, and rep comparison  

### 4. Robust Data Integration
- **Secure Webhook Ingestion** – HMAC-authenticated SAP payloads land weekly, triggering automated ETL & metric refresh  
- **Chain & Store Mapping** – logic consolidates multi-location chains into canonical accounts  
- **Product-Coverage Engine** – tracks top-30 SKUs per account, flags gaps, and feeds cross-sell recommendations into emails & UI  

---

## System Architecture

# System Architecture

```
+-------------------------------+     +---------------------------+
|     Cleaning / Normalizing    |     |  Store-Mapping & Chain    |
|  (pandas, sqlalchemy-utils)   +----->  Consolidation Logic      |
+-------------------------------+     +---------------------------+
                                      |
                                      v
                          +-------------------------+
                          |   Metric Calculator     |
                          |   • Y/Y revenue & pace  |
                          |   • Next-purchase date  |
                          |   • Health & coverage   |
                          +-----------+-------------+
                                      |
                                      v
                          +-------------------------+
                          | Postgres (Account +    |
                          | Prediction tables)     |
                          +-----------+-------------+
                                      |
               +----------------------+----------------------+
               |                                             |
               v                                             v
+-------------------------------+           +-------------------------------+
|   🔔 Customer-Reminder Task   |  daily    |   📬 Weekly Digest Task        |
|   flask send-reminders        | <-------- |   flask send-digests          |
|   • query next-due=TODAY      |  state    |   • accounts due this week    |
|   • email store → SMTP        |  change   |   • "reminded & no purchase"  |
|   • set reminder_state=SENT   |           |   • top opportunities         |
+-------------------------------+           +-------------------------------+
               |                                             |
               |  order placed? (checked in weekly ETL)      |
               +--------------------+                        |
                                    |                        |
                                    v                        |
                          +-------------------------+        |
                          |  State Updater (ETL)    |        |
                          |  • if last_order>sent   |        |
                          |    reminder_state=PURCH |        |
                          +-------------------------+        |
                                                             |
                                                             v
                                             +-------------------------------+
                                             |  Dashboards & REST API        |
                                             |  • Rep / Manager views        |
                                             |  • Live reminder status       |
                                             +-------------------------------+
```


### Data-flow Steps

1. **Data Ingestion**  
   - Weekly SAP files arrive through an HMAC-secured webhook (`/webhook/sales`).  
   - Files are stored in S3 / `/data/uploads`, and launches an asynchronous ETL job.  
   - Store-mapping logic merges chain locations into canonical “accounts.”

2. **Data Processing Pipeline**  
   - ETL cleans and normalizes transactions, aggregates Y/Y revenue, and tracks SKU coverage.  
   - Predictive module recalculates next expected purchase date, health score, and other metrics.  
   - Results are persisted to Postgres (`account`, `account_prediction`, `product_metrics`).

3. **Automated Email to Customers (🔔 Daily Reminder)**  
   - **CLI** `flask send-reminders` runs every morning via cron.  
   - Queries accounts where `next_expected_purchase_date = today` **and** `reminder_state IS NULL`.  
   - Sends a friendly “you might be due” email (SMTP) to the account’s buyer.  
   - Sets `reminder_state = 'SENT'` and `reminder_sent_at = now()`.  
   - On the next weekly ETL, if a purchase is detected after `reminder_sent_at`, state is updated to `PURCHASED`; otherwise it stays `SENT` for follow-up.

4. **Weekly Digest Emails to Sales Reps (📬)**  
   - Monday cron triggers `flask send-digests`.  
   - For each rep the engine assembles:  
     - **Accounts Due This Week** (`next_expected_purchase_date` within the coming 7 days).  
     - **Action Needed** – accounts with `reminder_state='SENT'` but **no purchase** > 7 days after reminder.  
     - Top-10 priority accounts, pacing KPIs, and cross-sell suggestions.  
   - Digest is delivered via direct SMTP and logged for audit.

5. **User Interfaces**  
   - React dashboards pull from a REST/Flask API, showing live metrics, reminder status, and drill-downs for reps and managers.  
   - “Reminder Status” column immediately flags SENT vs PURCHASED accounts for manual outreach.

This architecture keeps all automation in-house—no external services—while cleanly separating ingestion, prediction, customer outreach, and rep enablement.




### Database Models

| Model | Purpose | Key Fields / Highlights |
|-------|---------|-------------------------|
| **AccountPrediction** | **Live “single-row” view** of each account’s current health, pacing, and reminder status. | `canonical_code`, `next_expected_purchase_date`, `health_score`, `pace_pct`, <br>`customer_email`, `reminder_state` *(NULL \| SENT \| PURCHASED)*, `reminder_sent_at` |
| **AccountHistoricalRevenue** | Year-by-year revenue & SKU-coverage aggregates used for long-range trend calculations. | `year`, `total_revenue`, `unique_skus`, `avg_order_value`, … |
| **AccountSnapshot** | Weekly time-series snapshot of the **entire** `AccountPrediction` row (supports “look-back” analysis and drill-down charts). | `captured_at`, JSON payload of KPIs |
| **SalesRepPerformance** | Aggregated performance metrics per rep (YTD, PY, pace, quota attainment, etc.). | `rep_id`, `week`, `ytd_revenue`, `pace_vs_ly`, `overdue_accounts`, … |
| **ActivityLog** | Unified audit trail of outbound emails, dashboard interactions, and manual notes for each account. | `event_type`, `account_code`, `actor`, `payload`, `created_at` |
---

## Endpoints

### Data Ingestion
- **`/webhook/sales` (POST)**: Secure HMAC-authenticated endpoint for incoming sales data

### API Endpoints
- **`/api/churn/analysis`**: Churn risk analysis and high-risk accounts
- **`/api/strategic/accounts`**: Strategic account data with filtering
- **`/api/sales-manager/overview`**: Sales manager dashboard metrics
- **`/api/sales-manager/top_accounts_by_rep`**: Top accounts by revenue per rep
- **`/api/sales-manager/accounts/<card_code>/history`**: Account historical performance

### Dashboards
- **`/dashboard/`**: Main churn dashboard
- **`/dashboard/strategic`**: Strategic accounts dashboard
- **`/dashboard/sales-manager`**: Sales management dashboard

---

## Scheduled Tasks

| Task | Frequency & Trigger | What it Does | Key Code / CLI Command |
|------|--------------------|--------------|------------------------|
| **Daily Customer Reminder** | **Every day @ 07:30 AM** (cron on EB instance) | •  Finds **accounts whose `next_expected_purchase_date` is *today*** and whose `reminder_state` is `NULL`.<br>•  Sends a friendly “It looks like you usually order around now” email **directly to the store’s buyer**.<br>•  Sets `reminder_state = 'SENT'` and stamps `reminder_sent_at`. | `flask send-reminders` |
| **Weekly Digest to Reps** | **Every Tuesday @ 08:00 AM** | •  Aggregates each rep’s portfolio KPIs for the coming week.<br>•  Lists **Accounts Due This Week** (based on `next_expected_purchase_date`).<br>•  Lists **Accounts Needing Action** (state `SENT`, > 7 days, no purchase).<br>•  Suggests cross-sell SKUs and includes direct dashboard links. | `flask send-weekly-digests` |
| **Sales Data Refresh** | **Webhook-driven** (usually Mondays after SAP export) | •  Processes new CSV/XLS files, updates transaction tables.<br>•  Recalculates predictions (`AccountPrediction`) and snapshots (`AccountSnapshot`).<br>•  **Promotes “SENT → PURCHASED”** when a post-reminder order is detected. | `process_file_async` inside webhook worker |
| **Database Back-up** | Nightly, 02:00 AM | Dumps PostgreSQL to S3 (retention 14 days). | Managed EB cron + AWS Backup |

---

## Deployment

### Environment Configuration
The application is fully **12-factor**; all secrets and deploy-specific values are supplied via environment variables:

| Variable | Purpose |
|----------|---------|
| `SQLALCHEMY_DATABASE_URI` | PostgreSQL connection string |
| `HMAC_SECRET_KEY` | Shared secret for webhook signature validation |
| `SMTP_SERVER`, `SMTP_PORT`, `EMAIL_USERNAME`, `EMAIL_PASSWORD` | Outbound e-mail credentials |
| `TEST_MODE` | When `True`, e-mails are **printed to logs** instead of being sent |

### AWS Elastic Beanstalk
* Deployment is a **Docker / Gunicorn** EB environment (`Dockerfile` at the repo root).  
* EB health check endpoint: `GET /health` (returns `200 OK` + JSON status).  
* Cron tasks are declared in `.ebextensions/cron.config` and write to `/var/log/cron.log`.

---

## Technical Notes

### Security
* **HMAC + Timestamp** verification on the sales-data webhook prevents replay attacks.  
* All secrets live in **AWS Secrets Manager** and are injected via EB environment variables.  
* Thorough input validation and SQL-Alchemy ORM protect against injection.

### Reliability
* File ingestion & heavy transforms run in a **separate worker** to keep the web tier responsive.  
* Database writes are wrapped in `session.begin()` blocks for atomic commits.  
* Extensive structured logging (`services/logger.py`) feeds CloudWatch + optional Sentry.

### Performance
* Predominant analytical queries are pre-aggregated nightly and indexed (`idx_next_expected_purchase_date`, `idx_reminder_state`).  
* Dashboard endpoints paginate and stream JSON; front-end uses virtual scrolling for large tables.

---
