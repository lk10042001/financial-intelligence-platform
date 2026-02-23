# Financial Intelligence Platform

An end-to-end production-grade financial analytics system built as four loosely coupled modules sharing a common PostgreSQL database and served through a unified FastAPI layer. Mirrors LSEG's Fixed Income Reference and Entity Content team workflows.

**Stack:** Python · XGBoost · TensorFlow · LSTM · FinBERT · LangChain · PostgreSQL · Pinecone · yfinance · FRED API · SEC EDGAR · FastAPI · Apache Airflow · MLflow · Docker · Azure App Service · GitHub Actions · Power BI · Streamlit

---

## Architecture

```
Data Sources              Orchestration        Storage          Serving
────────────              ─────────────        ───────          ───────
yfinance API   ──┐
FRED API       ──┼──► Apache Airflow ──► PostgreSQL ──► FastAPI ──► Streamlit
SEC EDGAR      ──┘       (Docker)          (Docker)      (Azure)    Dashboard
Financial News                              MLflow
                                           Tracking
```

---

## Module 1 — Entity Content Pipeline

Automated daily pipeline that ingests legal entity reference data from three real financial data sources, runs clustering and anomaly detection, generates severity-scored alerts, and loads all results into PostgreSQL. Orchestrated by Apache Airflow running in Docker.

### Data Sources
- **yfinance** — real-time financial data for 48 S&P 500 companies (market cap, PE ratio, revenue, debt-to-equity, current ratio)
- **FRED API** — 6 macroeconomic indicators (Fed Funds Rate, CPI, GDP growth, Unemployment, 10Y/2Y Treasury yields)
- **SEC EDGAR** — 10-K filing verification for all 48 entities

### Pipeline Steps
1. Data ingestion and quality checks — null imputation with median, duplicate removal
2. EDA — statistical summary, skewness analysis, correlation heatmap
3. Log transform applied to all 5 features (all had skewness above 1)
4. Elbow Method to determine optimal K — sharpest inertia drop at K=4
5. K-Means clustering (K=4) for entity grouping
6. DBSCAN anomaly detection — identifies entities that fit no cluster
7. Severity scoring — distance from nearest cluster center (HIGH / MEDIUM / LOW)
8. All results saved to PostgreSQL with automated alerts

### Results

| Metric | Value |
|--------|-------|
| Entities processed | 48 |
| Sectors covered | 8 |
| Silhouette Score | 0.3357 |
| Davies-Bouldin Score | 0.9393 |
| Anomalies detected | 8 |
| HIGH alerts | 1 — TSLA (score: 3.43) |
| MEDIUM alerts | 6 — NVDA, AMD, GS, BA, LYFT, SNAP |
| Fed Funds Rate | 3.64% |
| 10Y Treasury | 4.21% |
| 2Y Treasury | 3.54% |
| Yield Spread | 0.67% |

### PostgreSQL Tables
`entity_raw` · `entity_clusters` · `entity_enriched` · `entity_alerts` · `entity_sec_filings` · `entity_summary_report`

### Airflow DAG
`entity_content_pipeline` — 4 tasks — `@daily` schedule

```
ingest_entity_data >> run_clustering >> generate_alerts >> generate_report
```

---

## Project Structure

```
financial-intelligence-platform/
│
├── module1_entity_pipeline/
│   ├── notebooks/
│   │   └── Module1_Entity_Pipeline.ipynb
│   ├── dags/
│   │   └── entity_pipeline_dag.py
│   └── src/
│       ├── ingest_entities.py
│       ├── cluster_entities.py
│       ├── generate_alerts.py
│       └── generate_report.py
│
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

## Setup

**Prerequisites:** Docker Desktop, Python 3.11, Anaconda

**1. Clone the repo**
```bash
git clone https://github.com/lk10042001/financial-intelligence-platform.git
cd financial-intelligence-platform
```

**2. Create a `.env` file in the project root**

Add your database credentials and FRED API key. Get a free FRED API key at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html)

**3. Start Docker containers**
```bash
docker-compose up -d
```

**4. Run the notebook**

Open `module1_entity_pipeline/notebooks/Module1_Entity_Pipeline.ipynb` in Jupyter and run all cells.

**5. View Airflow**

Open the Airflow UI in your browser after Docker starts and trigger the `entity_content_pipeline` DAG.

---

## Key Design Decisions

**Why K=4?** The Elbow Method showed the sharpest inertia drop at K=4. This also maps to real business segments: mega-caps (Apple, Microsoft), large-caps, mid-caps, and financial sector entities each have distinct financial profiles.

**Why DBSCAN alongside K-Means?** K-Means forces every entity into a cluster and cannot identify true outliers. DBSCAN identifies entities that do not fit any cluster (cluster = -1). Together they provide both cluster assignment and genuine anomaly detection — two separate business insights.

**Why log transform?** All 5 features had skewness above 1 before transformation (pe_ratio: 5.80, debt_to_equity: 2.98, market_cap: 2.53). Clustering on raw skewed financial data gives misleading results because distance calculations are dominated by the largest values. Log1p normalises the distributions before scaling.

**Why PostgreSQL over CSV?** Queryable, auditable, production-grade storage that mirrors how LSEG manages entity reference data. Enables Power BI live connection, downstream API serving, and consistent reruns with `if_exists='replace'`.
