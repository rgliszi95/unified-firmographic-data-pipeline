# 📘 Firmographics Data Pipeline — End-to-End Architecture (Airflow + S3 + Snowflake + dbt)

## 📌 Overview

This project implements an **end-to-end data pipeline** that ingests, stages, transforms, and models firmographic data from:

* **S&P 500 company list (scraped from Wikipedia)**
* **Fortune 1000 company metrics (via API)**

The pipeline uses **Airflow**, **AWS S3**, **Snowflake**, and **dbt** to produce a **clean star schema** suitable for analytics and investment-driven insights.

---

# 🧵 End-to-End Stages

## 1️⃣ Scrape S&P 500 List (Wikipedia)

* Scrape live S&P500 companies from Wikipedia.
* Convert the scraped data to **JSON format** before loading.
* Using JSON ensures schema drift tolerance if the website structure changes.

## 2️⃣ Fetch Fortune 1000 Data (API)

* Retrieve Fortune 1000 data directly from the API in **native JSON**.
* Dataset includes:

  * Rank & order
  * Revenue, assets, profit
  * Market value
  * Employee count
  * Numerous category indicators (e.g., `is_fastest_growing`)

---

## 3️⃣ Load to S3 (Staging Area)

* Both datasets land in an **S3 bucket** that acts as a raw staging zone.
* S&P500 → JSON
* Fortune1000 → already JSON
* Snowflake is connected to S3 using an **External Stage + Storage Integration**.
* JSON is used intentionally to avoid ingestion failures on source schema changes.

---

## 4️⃣ Load to Snowflake RAW Layer

* Data is loaded **as JSON `VARIANT`** into the `RAW` schema.
* Additional metadata columns are stored:

  * file name
  * ingestion timestamp
  * Airflow execution timestamp

This layer preserves the original structure of both sources.

---

## 5️⃣ STAGING Layer (dbt)

* JSON is **flattened, normalized, casted**, and cleaned.
* Datasets are standardized into consistent column names.
* Key goals:

  * Extract nested fields
  * Enforce typing (numeric, boolean, timestamps)
  * Apply basic quality fixes
* Outputs:

  * `stg_wiki_sp500`
  * `stg_fortune500`

Every staging model is **incremental**.

---

## 6️⃣ CORE Layer (dbt)

* S&P500 and Fortune1000 are **joined** to create:

  * `cr_company_complete`
* Only companies appearing in **both** datasets are kept.
* Duplicate or irrelevant columns are removed.
* Missing or inconsistent data is imputed when possible.

This layer produces a **fully cleaned, unified dataset** for modeling.

---

## 7️⃣ ANALYTICS Layer (dbt Star Schema)

### ⭐ Dimension Tables

* **`dim_company`**
  Static attributes that rarely change (symbol, industry, sector, founded_year).

* **`dim_location`**
  Headquarters-related information. Changes tracked via SCD2 snapshot.

* **`dim_fortune_metrics`**
  Fortune-specific evaluation fields (rank, company_order, category flags).

### 📈 Fact Table

* **`fact_company_performance`**
  Metrics such as revenue, profit, market value, assets, employee count.

### 🕒 Slowly Changing Dimensions (SCD2)

* Snapshots:

  * `company_location_snapshot`
  * `fortune_metrics_snapshot`

Snapshot tables store **full history**.
Dimension tables store **current only** (active records where `valid_to IS NULL`).
This avoids accidental misuse by inexperienced analysts.

### 🔑 Keys

* All dimensions and facts have **surrogate keys**.
* Fact table contains **FK references** to every dimension.

---

## 8️⃣ Incremental Strategy

In `staging`, `core`, and `analytics`:

* **Everything is incremental**, except:

  * `dim_company` (does not change often)
  * Snapshot tables (dbt snapshots are inherently incremental)

---

## 9️⃣ Documentation Export

After transformation finishes:

* `dbt docs generate` runs
* All documentation artifacts are uploaded to S3
* Stored in folders **partitioned by dbt invocation ID**

Each SQL model has its own matching `.yml` test file.

---

# 🧪 Testing Strategy

## Generic Tests

* `not_null` on primary keys
* `unique` on surrogate keys
* `relationships` between fact and dimensions

## Custom Tests

* Ensure **profit never exceeds revenue**:

  ```
  revenue >= profit
  ```

---

# 🌐 Data Sources

### Wikipedia S&P500 (Web Scraping)

* Provides tickers, company names, sectors, and industries.
* Dataset is static for this assignment but pipeline supports updates.

### Fortune 1000 API

* Provides rich firmography including:

  * Revenue, profit, assets
  * Rank
  * Market cap
  * Category flags
  * Employee number

---

# 🛠️ Used Technologies

| Technology      | Purpose                                                 |
| --------------- | ------------------------------------------------------- |
| **AWS S3, IAM** | Staging zone, metadata/artifact storage, secure access  |
| **Airflow**     | Pipeline orchestration                                  |
| **Snowflake**   | Data warehouse for raw + transformed + analytics layers |
| **dbt**         | Transformations, tests, SCD2 snapshots, documentation   |
| **Docker**      | Local Airflow runtime                                   |

---

# 🚀 Deployment Requirements

### Airflow

* Local Airflow using Docker
* AWS + Snowflake connections configured in Airflow UI

### AWS

* S3 bucket for:

  * staging data
  * documentation artifacts
* IAM user with S3 + Snowflake integration permissions

### Snowflake

* Required schemas:

  * `RAW`
  * `STAGING`
  * `CORE`
  * `ANALYTICS`
  * `SNAPSHOTS`
* Proper roles, users, and grants configured

### dbt

* All models, snapshots, tests, macros included and runnable

---

# 📁 Repository Structure

```
airflow-docker/
│
├── dags/
│   └── ingest_firmographics_to_snowflake.py
│
├── firmographics_dbt/
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── parse_numeric.sql
│   │
│   ├── models/
│   │   ├── core/
│   │   │   ├── cr_company_complete.sql
│   │   │   └── cr_company_complete.yml
│   │   │
│   │   ├── staging/
│   │   │   ├── schema.yml
│   │   │   ├── stg_fortune500.sql
│   │   │   ├── stg_fortune500.yml
│   │   │   ├── stg_wiki_sp500.sql
│   │   │   └── stg_wiki_sp500.yml
│   │   │
│   │   ├── star/
│   │       ├── dim_company.sql
│   │       ├── dim_company.yml
│   │       ├── dim_fortune_metrics.sql
│   │       ├── dim_fortune_metrics.yml
│   │       ├── dim_location.sql
│   │       ├── dim_location.yml
│   │       ├── fact_company_performance.sql
│   │       └── fact_company_performance.yml
│   │
│   ├── snapshots/
│   │   ├── company_location_snapshot.sql
│   │   ├── company_location_snapshot.yml
│   │   ├── fortune_metrics_snapshot.sql
│   │   └── fortune_metrics_snapshot.yml
│   │
│   ├── tests/
│   │   └── test_fortune_profit_not_exceed_revenue.sql
│   │
│   └── dbt_project.yml
│
├── docker-compose.yaml
└── Dockerfile.airflow
```

---

# 📈 Use Case

The final modeled dataset is designed for **investment insights**, enabling:

* Company performance comparison
* Ranking and benchmarking
* Sector and industry analysis
* Profitability and growth evaluation
* Leadership and governance studies
* Market opportunity research

