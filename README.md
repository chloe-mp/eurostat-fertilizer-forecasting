# European Fertilizer Forecasting Pipeline

[![CI](https://github.com/chloe-mp/eurostat-fertilizer-forecasting/actions/workflows/ci.yml/badge.svg)](https://github.com/chloe-mp/eurostat-fertilizer-forecasting/actions/workflows/ci.yml) [![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff) [![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)

End-to-end ML pipeline forecasting nitrogen and phosphorus fertilizer
usage across EU-27, built on public Eurostat data.

> Distributed Airflow orchestration · Prophet time-series modeling ·
> Continuous evaluation · Interactive Streamlit dashboard.

---

## Why this project

Across Europe, nitrogen (N) and phosphorus (P) inputs to agricultural
soils are tightly framed by EU regulation — the Common Agricultural
Policy (CAP), the Nitrates Directive, national fertilization plans.
Tracking how these inputs evolve is useful both for compliance
monitoring and for anticipating agro-environmental transitions.

This project forecasts fertilizer usage across EU-27 countries from
public Eurostat Nutrient Budget data, covering **1985–2028** (40 years
of history + 5 years of forecast). Beyond the modeling itself, the
goal was to build the project as a realistic data engineering
exercise: a multi-source medallion architecture, distributed
orchestration with Airflow, and continuous model evaluation.

---

## Headline numbers

After the latest run (March 2026):

| Metric                                | Value                          |
|---------------------------------------|--------------------------------|
| Total time series modeled             | **438**                        |
| Total forecasts generated             | **11 899**                     |
| Median MAE                            | **6 763 tonnes**               |
| Median 80% IC coverage                | **0.80** *(well calibrated)*   |
| Series skipped (insufficient history) | 1 250                          |
| Series failed                         | 0                              |
| Negative predictions (clamped)        | 18 (0.15%)                     |

The **median coverage of exactly 0.80** is a strong signal that
Prophet's confidence intervals are well-calibrated on this dataset.
The MAE distribution is heavy-tailed (mean 22k, median 6.8k tonnes),
which is expected when modeling both tiny aggregates (e.g. Malta,
MAE ~1.5 t) and the EU-27 aggregate side-by-side (MAE ~422k t).

---

## Architecture

The project follows a **medallion lakehouse** pattern (Bronze / Silver
/ Gold) orchestrated with Apache Airflow on a distributed Docker stack.

    ┌────────────────────────────────────────────────────┐
    │              Apache Airflow                        │
    │  (CeleryExecutor + Redis + Postgres metadata DB)   │
    └──────────────────┬─────────────────────────────────┘
                       │
            ┌──────────┴──────────┐
            ▼                     ▼
    ┌─────────────┐       ┌──────────────┐
    │ Eurostat    │       │  Modeling    │
    │ JSON API    │       │  pipeline    │
    └──────┬──────┘       └──────┬───────┘
           │                     │
           ▼                     │
    ┌──────────────────┐         │
    │   BRONZE (raw)   │         │
    │   ETL_Eurostat   │         │
    └────────┬─────────┘         │
             ▼                   │
    ┌──────────────────┐         │
    │  SILVER          │         │
    │  (cleaning,      │         │
    │   aggregation)   │         │
    └────────┬─────────┘         │
             ▼                   ▼
            ┌──────────────────────┐
            │     GOLD             │
            │  ML_Eurostat         │
            │  Prophet × 438 series│
            └────────┬─────────────┘
                     ▼
            ┌────────────────────┐
            │    PostgreSQL      │
            │  predictions       │
            │  + model_metrics   │
            │  + run_log         │
            └────────┬───────────┘
                     ▼
            ┌────────────────────┐
            │ Streamlit + Plotly │
            │     dashboard      │
            └────────────────────┘

### Why this stack

- **CeleryExecutor over LocalExecutor**: chosen to allow horizontal
  scaling of Prophet fits across workers — relevant when running
  hundreds of parallel time-series models.
- **Bronze/Silver/Gold separation**: makes it easy to reprocess any
  layer without re-fetching upstream sources, and keeps deterministic
  transformations isolated from modeling.
- **Continuous evaluation logged to Postgres**: every run writes
  per-series metrics (MAE, RMSE, IC coverage) and global run logs,
  enabling drift detection across runs.

---

## Pipeline stages

### Bronze — Raw ingestion (`dags/ETL_Eurostat.py`)

- Fetches Eurostat datasets via the JSON statistics API:
  - `aei_fm_usefert` — mineral fertilizer use (N/P)
  - `aei_pr_gnb` — Gross Nutrient Balance (inputs / outputs / balance)
- Parses Eurostat's compressed multi-dimensional JSON format into
  flat tabular records with Polars.
- Loads into PostgreSQL via `COPY` for performance.

### Silver — Cleaning and aggregation

- SQL transformations (CTEs + FULL OUTER JOIN) to merge mineral and
  GNB data into a unified per-country/nutrient/year table.
- Idempotent UPSERT logic (`ON CONFLICT DO UPDATE`) for rerun safety.

### Gold — Modeling (`dags/ML_Eurostat.py`)

- Trains a Prophet model per (country × nutrient × indicator)
  combination.
- **438 series modeled successfully**, **1 250 skipped** (fewer than
  10 historical years).
- Forecast horizon: 5 years.
- **Cross-validation** (Prophet's `cross_validation` +
  `performance_metrics`) computed for series with ≥ 15 observations,
  yielding MAE / RMSE / coverage logged per series.
- Negative-prediction handling: predictions on indicators that cannot
  physically be negative (mineral / inputs / outputs) are flagged and
  clamped to 0 post-hoc, with the clamping count tracked in run logs.
- Models serialized to disk (`models/prophet/*.pkl`) for
  reproducibility and offline inspection.

### Dashboard (`dashboard_engrais.py`)

- Streamlit + Plotly interactive UI.
- Filters by country, nutrient, indicator.
- Side-by-side display of historical data, forecasts, 80% confidence
  intervals, and per-series quality metrics.
- KPI cards (last historical value, forecast value, MAE, coverage).
- Custom CSS for a clean editorial design.

---

## Tech stack

| Layer              | Tools                                       |
|--------------------|---------------------------------------------|
| Orchestration      | Apache Airflow (CeleryExecutor)             |
| Worker queue       | Redis 7.2                                   |
| Storage / metadata | PostgreSQL 13                               |
| Data ingestion     | Polars, Eurostat JSON API                   |
| Modeling           | Prophet (Meta) + cross-validation           |
| Validation         | Pydantic                                    |
| Dashboard          | Streamlit + Plotly                          |
| Containerization   | Docker / docker-compose (multi-service)     |

---

## Running locally

```bash
git clone https://github.com/chloe-mp/eurostat-fertilizer-forecasting
cd eurostat-fertilizer-forecasting
docker-compose up -d
# Airflow UI:  http://localhost:8080  (airflow / airflow)
# Trigger DAGs in order:
#   1. engrais_etl_eurostat_complet
#   2. engrais_ml_eurostat (auto-triggered by the ETL)
# Then run the dashboard:
streamlit run dashboard_engrais.py
```

---

## Limitations & next steps

### Modeling
- **Prophet's assumptions limit it to additive seasonality and smooth
  trends.** This is acceptable for fertilizer usage (slow-evolving)
  but may miss regime changes — e.g., post-2027 CAP reforms or sudden
  N-fertilizer price shocks.
- **No comparison baseline.** A natural extension is benchmarking
  Prophet against modern foundation models for time-series
  (TimeGPT, Chronos, Lag-Llama, Moirai), particularly to assess
  zero-shot performance on short series (< 20 observations) currently
  excluded.
- **No probabilistic calibration analysis.** 80% IC coverage is
  tracked, but reliability diagrams and CRPS scoring would give a
  fuller picture of distributional accuracy.
- **Worst-performing series are aggregate-level** (e.g. EU27_2020
  with MAE = 422k tonnes on N output balance). Modeling aggregates
  separately from member states would likely improve accuracy.

### Infrastructure
- **No model versioning.** Migration to MLflow (or a lightweight
  alternative) would track lineage across runs.
- **Single-node Celery workers.** Deployment on Kubernetes with
  KEDA-based autoscaling would be the natural step for scaling
  beyond the current 438 parallel fits.
- **No CI/CD on DAGs.** GitHub Actions for DAG validation
  (`airflow dags test`) and unit tests on transformation logic
  would harden the project.

### Reproducibility
- The full stack runs locally via `docker-compose up`, but cloud
  deployment (e.g. Cloud Composer, MWAA) hasn't been tested.

---

## Acknowledgments

Eurostat — Nutrient Budgets dataset (public domain).
Built as a personal project alongside ongoing work on agentic AI
systems and multi-agent research ([ECAI 2025](https://hal.science/hal-05350815v1)).

---

*Made by [Chloé Petridis](https://github.com/chloe-mp) — open to
discussion on time-series approaches beyond Prophet, especially for
handling regime changes.*
