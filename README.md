# 🏎️ F1 Data Engineering Pipeline

A complete end-to-end Formula 1 data engineering pipeline built on **Databricks** using **Lakeflow Spark Declarative Pipelines (SDP)**. The project ingests live and historical F1 data from the [OpenF1 API](https://openf1.org/), processes it through a medallion architecture (Bronze → Silver → Gold), and produces analytics-ready datasets for driver, team, and championship performance analysis.

## 📌 Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Data Source](#data-source)
* [Project Structure](#project-structure)
* [Bronze Layer (Ingestion)](#bronze-layer-ingestion)
* [Silver Layer (Cleansing & Standardization)](#silver-layer-cleansing--standardization)
* [Gold Layer (Analytics)](#gold-layer-analytics)
* [Unity Catalog Structure](#unity-catalog-structure)
* [Pipeline Configuration](#pipeline-configuration)
* [Getting Started](#getting-started)
* [Key Features](#key-features)
* [Technologies](#technologies)
* [Author](#author)

## Overview

This project demonstrates a production-grade data pipeline for Formula 1 telemetry, race results, and championship data. It follows the **medallion architecture** pattern popularized by Databricks, where data flows through three layers of increasing quality and structure:

1. **Bronze** — Raw API data ingested into Delta Lake tables with metadata columns (`ingestion_ts`, `source`)
2. **Silver** — Cleansed, standardized, and validated data with data quality expectations enforced via SDP
3. **Gold** — Aggregated, analytics-ready materialized views for reporting and dashboards

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         OpenF1 API                                  │
│                   https://api.openf1.org/v1/                          │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Python scripts — API ingestion)                      │
│  • Sessions   • Meetings   • Drivers   • Position                    │
│  • Laps       • Car Data   • Championship Drivers                    │
│  Raw data → Delta tables with ingestion_ts & source metadata        │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (SDP Streaming Tables — Python)                        │
│  • Silver Sessions   • Silver Meetings   • Silver Drivers           │
│  • Silver Position  • Silver Laps       • Silver Car Data          │
│  • Silver Championship Drivers                                       │
│  Schema standardization + data quality expectations (drop invalid)  │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (SDP Materialized Views — SQL)                          │
│  • Gold Team Performance           • Gold Driver Performance         │
│  • Gold Driver Championship                                         │
│  Aggregations: wins, podiums, fastest laps, sector times, standings │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Source

All data is fetched from the **[OpenF1 API](https://openf1.org/)** — a free, open-source API providing real-time and historical Formula 1 data. The following endpoints are used:

| Endpoint                  | Description                                                  |
| ------------------------- | ------------------------------------------------------------ |
| `/v1/sessions`            | Race weekend sessions (Practice, Qualifying, Race)           |
| `/v1/meetings`            | Grand Prix meeting metadata (circuit, country, dates)        |
| `/v1/drivers`             | Driver details per session (name, team, number)              |
| `/v1/position`            | Real-time position changes during sessions                    |
| `/v1/laps`                | Lap-by-lap timing data (sector times, speeds, durations)       |
| `/v1/car_data`            | Car telemetry (speed, RPM, gear, throttle, brake, DRS)       |
| `/v1/championship_drivers` | Driver championship standings (fetched from final race/year) |

## Project Structure

```
F1_Pipeline/
├── f1_data_engineering/
│   ├── bronze/                          # Bronze layer — API ingestion scripts (Python)
│   │   ├── bronze_sessions.py           # Sessions ingestion (MERGE into Delta)
│   │   ├── bronze_meeting.py            # Meetings ingestion (MERGE into Delta)
│   │   ├── bronze_drivers.py            # Drivers ingestion per race session (APPEND)
│   │   ├── bronze_position.py           # Position data per session (APPEND)
│   │   ├── bronze_laps.py               # Lap data per race session (APPEND)
│   │   ├── bronze_car_data.py           # Car telemetry — speed ≥ 300 km/h (APPEND)
│   │   └── bronze_driver_champ.py      # Championship standings from final race (APPEND)
│   │
│   ├── silver/                          # Silver layer — SDP streaming tables (Python)
│   │   ├── silver_sessions.py           # Standardized sessions + duration calculation
│   │   ├── silver_meeting.py            # Standardized meetings + duration calculation
│   │   ├── silver_drivers.py            # Cleansed driver information
│   │   ├── silver_position.py           # Standardized position data
│   │   ├── silver_laps.py               # Standardized lap timing data
│   │   ├── silver_car_data.py           # Telemetry with DRS status translation
│   │   └── silver_driver_champ.py       # Championship standings with type casting
│   │
│   ├── gold/                            # Gold layer — SDP materialized views (SQL)
│   │   ├── gold_team_performance.sql            # Team wins & podiums per year
│   │   ├── gold_driver_performance_summary.sql # Driver performance metrics per year
│   │   └── gold_driver_champ.sql                # Championship standings with team info
│   │
│   └── README.md                        # This file
│
└── transformations/                     # (Reserved for additional pipeline sources)
```

## Bronze Layer (Ingestion)

The bronze layer consists of Python scripts that fetch data from the OpenF1 API and write it into Delta Lake tables. Each script:

* Creates the target Delta table if it doesn't exist (with an explicit schema)
* Identifies new or unloaded sessions to avoid duplicate ingestion (idempotent loads)
* Fetches data per session from the API with error handling and rate-limit protection
* Appends metadata columns (`ingestion_ts`, `source = 'openf1'`) for traceability

### Bronze Tables

| Table                                | Script                     | Write Mode | Key Columns                                              |
| ------------------------------------ | -------------------------- | ---------- | ------------------------------------------------------- |
| `f1.bronze_sch.sessions`             | `bronze_sessions.py`       | MERGE      | session_key, meeting_key, session_name, year, location |
| `f1.bronze_sch.meetings`             | `bronze_meeting.py`        | MERGE      | meeting_key, circuit_key, meeting_name, year            |
| `f1.bronze_sch.drivers`             | `bronze_drivers.py`        | APPEND     | session_key, driver_number, full_name, team_name        |
| `f1.bronze_sch.position`            | `bronze_position.py`       | APPEND     | session_key, driver_number, position, date              |
| `f1.bronze_sch.laps`                | `bronze_laps.py`           | APPEND     | session_key, driver_number, lap_number, lap_duration    |
| `f1.bronze_sch.car_data`            | `bronze_car_data.py`       | APPEND     | session_key, driver_number, speed, rpm, throttle       |
| `f1.bronze_sch.bronze_championship_drivers` | `bronze_driver_champ.py` | APPEND  | session_key, driver_number, position_current, points    |

## Silver Layer (Cleansing & Standardization)

The silver layer is implemented as **SDP Streaming Tables** using Python decorators (`@dp.table`, `@dp.expect_all_or_drop`). Each table:

* Reads from its corresponding bronze table via `spark.readStream`
* Enforces **data quality expectations** that drop invalid rows (null keys, out-of-range values, etc.)
* Standardizes data types and timestamps
* Derives computed columns (session duration, DRS status, etc.)
* Removes technical metadata columns (`source`)

### Silver Data Quality Expectations

| Table                         | Key Expectations                                                        |
| ----------------------------- | ----------------------------------------------------------------------- |
| `silver_sessions`             | Non-null session_key, meeting_key, session_name; year ≥ 1950            |
| `silver_meeting`              | Non-null meeting_key, circuit_key, meeting_name; year ≥ 1950           |
| `silver_drivers`              | Non-null session_key, driver_number, full_name, team_name              |
| `silver_position`             | Non-null session_key, driver_number; position > 0                      |
| `silver_laps`                 | Non-null keys; lap_number > 0; lap_duration ≥ 0; all sector times ≥ 0   |
| `silver_car_data`             | speed ≥ 0; rpm ≥ 0; throttle 0–100; brake 0–100; gear 0–8               |
| `silver_championship_drivers` | Non-null keys; position_current > 0; points_current ≥ 0                |

### Silver Transformations

* **Silver Sessions** — Adds `session_duration_minutes` derived from `date_end - date_start`
* **Silver Meetings** — Adds `meeting_duration_hours` derived from timestamps
* **Silver Car Data** — Translates raw DRS integer codes (0, 1 = OFF; 10, 12, 14 = ON) into a human-readable `drs_status` column
* **All tables** — Casts integer columns to `LongType` / `DoubleType` for schema consistency

## Gold Layer (Analytics)

The gold layer consists of **SDP Materialized Views** defined in SQL. These are batch-computed aggregations that join silver-layer tables to produce business-ready analytics.

### Gold Team Performance

**Table:** `f1.gold_sch.gold_team_performance`

* Aggregates wins and podium finishes per team per year
* Collects driver names for each team
* Ordered by year (descending) and total wins (descending)

| Column             | Description                                      |
| ------------------ | ------------------------------------------------ |
| `team_name`        | Constructor / team name                          |
| `driver_names`     | Comma-separated list of drivers for the team     |
| `total_wins`       | Count of race wins (position = 1) per year       |
| `podium_finishes`  | Count of podium finishes (position ≤ 3) per year |
| `year`             | Championship year                                 |

### Gold Driver Performance Summary

**Table:** `f1.gold_sch.gold_driver_performance_summary`

* Comprehensive per-driver, per-year performance metrics
* Joins driver, position, lap, and session data
* Calculates wins, podiums, fastest laps, average finish position, best sector times, and total laps completed

| Column                   | Description                                          |
| ------------------------ | ---------------------------------------------------- |
| `driver_number`          | Driver's race number                                 |
| `driver_name`            | Full name of the driver                              |
| `team_name`              | Team / constructor name                              |
| `race_year`              | Year of the races                                    |
| `wins`                   | Total race wins (position = 1)                      |
| `podium_finishes`        | Total podium finishes (position ≤ 3)                |
| `fastest_laps`           | Count of fastest laps set in races                   |
| `total_races`            | Total races participated in                           |
| `avg_finish_position`    | Average finishing position                           |
| `best_sector_1_time`     | Best Sector 1 time (seconds)                        |
| `best_sector_2_time`     | Best Sector 2 time (seconds)                        |
| `best_sector_3_time`     | Best Sector 3 time (seconds)                        |
| `best_lap_time`          | Fastest overall lap time (seconds)                   |
| `total_laps_completed`   | Total laps completed across all races that year      |

### Gold Driver Championship

**Table:** `f1.gold_sch.gold_driver_championship`

* Championship standings from the final race of each year
* Enriched with driver names and team information

| Column              | Description                                   |
| ------------------- | --------------------------------------------- |
| `session_key`       | Session key of the final race                  |
| `position`          | Championship standing position                 |
| `driver_number`     | Driver's race number                           |
| `driver_name`       | Full name of the driver                        |
| `team_name`         | Team / constructor name                        |
| `points`            | Championship points                           |
| `championship_year` | Year of the championship                        |

## Unity Catalog Structure

All tables are published to the **`f1`** Unity Catalog with three schemas:

```
f1 (Catalog)
├── bronze_sch        # Raw ingested data (7 tables)
├── silver_sch        # Cleansed & validated streaming tables (7 tables)
└── gold_sch          # Analytics-ready materialized views (3 views)
```

## Pipeline Configuration

| Setting         | Value           |
| --------------- | --------------- |
| Pipeline Name   | F1_Pipeline     |
| Catalog         | `f1`            |
| Schema          | `silver_sch`    |
| Compute         | Serverless      |
| Photon          | Enabled         |
| Channel         | Current         |
| Continuous Mode | Disabled        |

### Pipeline Libraries

The pipeline includes source code from two glob paths:

* `f1_data_engineering/**` — All bronze, silver, and gold transformation files
* `transformations/**` — Reserved for additional pipeline sources

## Getting Started

### Prerequisites

* A Databricks workspace with Unity Catalog enabled
* Serverless compute enabled
* `f1` catalog created in Unity Catalog
* Permissions to create schemas (`bronze_sch`, `silver_sch`, `gold_sch`)

### Setup

1. **Clone the repository** into a Databricks Git folder or workspace directory.

2. **Create Unity Catalog schemas:**
   ```sql
   CREATE SCHEMA IF NOT EXISTS f1.bronze_sch;
   CREATE SCHEMA IF NOT EXISTS f1.silver_sch;
   CREATE SCHEMA IF NOT EXISTS f1.gold_sch;
   ```

3. **Run bronze ingestion scripts** — Execute the Python scripts in `bronze/` in a Databricks notebook or job to populate the raw Delta tables. Recommended order:
   1. `bronze_sessions.py` (must run first — other scripts depend on session keys)
   2. `bronze_meeting.py`
   3. `bronze_drivers.py`
   4. `bronze_position.py`
   5. `bronze_laps.py`
   6. `bronze_car_data.py`
   7. `bronze_driver_champ.py`

4. **Run the SDP pipeline** — Create or configure a Lakeflow Spark Declarative Pipeline pointing to the `f1_data_engineering/` directory, then start an update to process silver and gold layers.

5. **Query the gold tables** — Once the pipeline completes, analyze the results:
   ```sql
   SELECT * FROM f1.gold_sch.gold_team_performance ORDER BY year DESC, total_wins DESC;
   SELECT * FROM f1.gold_sch.gold_driver_performance_summary ORDER BY race_year DESC, wins DESC;
   SELECT * FROM f1.gold_sch.gold_driver_championship ORDER BY championship_year DESC, position;
   ```

## Key Features

* **Idempotent ingestion** — Bronze scripts detect already-loaded sessions and skip them on re-runs
* **Rate-limit aware** — API requests include delays and retry logic for HTTP 429 responses
* **Data quality enforcement** — Silver layer drops rows failing expectations, ensuring clean downstream data
* **Schema management** — Explicit schemas defined for all bronze tables; type standardization in silver
* **Medallion architecture** — Clear separation of concerns across Bronze (raw), Silver (clean), and Gold (analytics)
* **Serverless + Photon** — Pipeline runs on Databricks serverless compute with Photon acceleration
* **Traceability** — `ingestion_ts` and `source` metadata columns in bronze layer for full data lineage

## Technologies

* **Databricks** — Cloud data platform
* **Lakeflow Spark Declarative Pipelines (SDP)** — Pipeline orchestration and data quality
* **Apache Spark / PySpark** — Distributed data processing
* **Delta Lake** — ACID transactional storage format
* **Unity Catalog** — Data governance and access control
* **OpenF1 API** — Formula 1 data source
* **Python** — Bronze ingestion and silver streaming tables
* **SQL** — Gold layer materialized views

## Author

**Melvin Tharakan** — Data Engineer

---

_This project is for educational and portfolio purposes. F1 data is sourced from the [OpenF1 API](https://openf1.org/), an open-source community project not affiliated with Formula 1._
