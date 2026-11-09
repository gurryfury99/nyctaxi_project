# NYC Taxi Lakehouse Pipeline (Azure Databricks)

An end-to-end data engineering project that ingests NYC TLC Yellow Taxi trip data, processes it through a **medallion architecture** (bronze, silver, gold) on **Azure Databricks**, and exports a partitioned JSON copy to Azure Data Lake Storage.

> **Credit:** This project was built by following the Udemy course
> *Azure Databricks and Spark SQL (Python)* (instructor listed as Malvik Vaghadia),
> adapted to my own Azure environment, folder layout and data months.
> It is a learning project, not original course material.

## What it does

Each month the job downloads one month of Yellow Taxi data, appends it to the bronze layer, cleanses and enriches it in silver, aggregates it to a daily summary in gold, and exports the enriched data as JSON to a separate storage account.

**Data sources (NYC TLC, public):**
- Monthly trip data: `yellow_tripdata_YYYY-MM.parquet`
- Zone reference data: `taxi_zone_lookup.csv`

## Architecture

Catalog: `nyctaxi_workspace` (Unity Catalog)

| Layer | Schema | Table / location | What happens |
|---|---|---|---|
| Landing | `00_landing` (volume) | `data_sources/nyctaxi_yellow/<yyyy-MM>/`, `data_sources/lookup/` | Raw parquet and CSV files downloaded from TLC |
| Bronze | `nyctaxi_01_bronze` | `yellow_trips_raw` | Raw trips plus a `processed_timestamp` column, appended monthly |
| Silver | `nyctaxi_02_silver` | `yellow_trip_cleansed` | Rows validated, codes decoded (vendor, rate type, payment type), `trip_duration` (minutes) calculated, `tip_amount` kept |
| Silver | `nyctaxi_02_silver` | `yellow_trip_quarantine` | Rows that failed validation, with the reason(s) in `reject_reasons`. Nothing is dropped silently |
| Silver | `nyctaxi_02_silver` | `taxi_zone_lookup` | Zone reference table built as a Slowly Changing Dimension Type 2 (`effective_date`, `end_date`) |
| Silver | `nyctaxi_02_silver` | `yellow_trips_enriched` | Trips joined to the zone table twice, adding pickup and dropoff borough and zone |
| Gold | `nyctaxi_03_gold` | `daily_trip_summary` | Per day: trip count, average passengers, average distance, average/min/max fare, total revenue |
| Export | `nyctaxi_04_export` | `yellow_trips_export` (external table) | JSON files in ADLS Gen2, partitioned by `vendor` and `year_month` |

## Orchestration

`nyctaxi_job` is a Databricks Job deployed with a Databricks bundle (`databricks.yml`, `dev` target) and sourced from this repo's `main` branch. It has nine notebook tasks and two if/else conditions.

- The ingest tasks (`00_ingest_yellow_trips`, `00_ingest_lookup`) set a `continue_downstream` task value.
- If the month's file already exists or the download fails, the value is `no` and the downstream tasks are skipped.
- Otherwise the job runs bronze, silver, gold and export in order.
- The yellow trip pipeline is incremental, one month per run. Each notebook takes two parameters: `months_ago` (default 3, the month to process relative to today) and `target_month` (`YYYY-MM`, optional). Set `target_month` to backfill or re-run any month through the same code path.
- Loads are idempotent: each table is written with Delta `replaceWhere` for just that month, so re-running a month never duplicates rows. The ingest step skips a month only when it is really in bronze, so a failure after the download no longer leaves a gap.

## Data quality

Cleansing rules (`modules/transformations/cleansing.py`) were chosen by profiling real TLC files. Rows are flagged, not dropped: failures go to `yellow_trip_quarantine` with a reason. Rules cover pickups outside the month, dropoff before or equal to pickup, trips over 6 hours, zero or negative or over-100-mile distances, negative or over-$500 fares, more than 6 passengers, and invalid zone IDs. Null passenger count, rate code and surcharges are **not** errors: about a quarter of rows are Flex Fare trips where those fields are legitimately empty.

On the May 2026 file, 95.6% of 4.09M trips passed and 4.4% were quarantined.

## Testing

```
pip install -r requirements-dev.txt
pytest                   # unit tests plus end-to-end checks on the sample parquet
pytest -m "not sample_data"   # unit tests only (faster)
```

The transformation logic lives in `modules/` as plain PySpark functions and is tested on local Spark, so it can be checked without a Databricks workspace. The Delta-specific writes (`modules/utils/delta_io.py`) and the notebooks themselves can only be verified on Databricks.

Architecture diagrams are in [`docs/`](docs/).

## Repository structure

```
.
├── databricks.yml                 # Databricks bundle definition (dev target)
├── modules/                       # Reusable, tested Python code imported by the notebooks
│   ├── data_loader/file_downloader.py
│   ├── transformations/           # cleansing, zones (SCD2), enrichment, gold, metadata
│   └── utils/                     # date_utils, delta_io
├── tests/                         # pytest suite (local Spark)
├── transfromations/notebooks/     # Live pipeline notebooks, run by nyctaxi_job
│   ├── 00_landing/
│   ├── 01_bronze/
│   ├── 02_silver/
│   ├── 03_gold/
│   └── 04_export/
├── one_off/                       # One-time setup and historical backfill
│   ├── catalog_schemas_volume.ipynb
│   ├── intial_load/notebooks/     # Initial-load versions of each layer (full overwrite),
│   │                              # plus rebuild_bronze_with_source_month.py
│   └── nyctaxi_data/              # Sample parquet files
├── ad-hoc/                        # Analysis and maintenance
│   ├── yellow_taxi_eda_2.py       # Row counts per month for every table
│   └── purge_tables_from_date.py  # Deletes a month so the job can reload it (dry run by default)
└── docs/                          # Architecture and task-graph diagrams
```

## One-off initial load

The history was loaded once before the monthly job took over:
1. `catalog_schemas_volume.ipynb` creates the catalog, schemas and landing volume.
2. The backfill notebook downloads 2025-12 through 2026-04 into the landing volume.
3. The initial-load notebooks build bronze, silver and gold from that history.

The scheduled job then adds the following month (2026-05) incrementally.

## Analysis

`ad-hoc/yellow_taxi_data_eda.ipynb` holds Spark SQL queries over the enriched table:
- daily trips and revenue
- trips and revenue by pickup borough
- trips and revenue by pickup hour
- the top borough-to-borough routes

## Tech stack

Azure Databricks, Azure Data Lake Storage Gen2, Unity Catalog (catalog, schemas, volumes, external location), Delta Lake, PySpark, Spark SQL, Databricks Jobs, Databricks bundles, Git.

## Setup notes

- Update the workspace host in `databricks.yml` and the storage account path in `04_export/yellow_trips_export.py` for your own environment.
- Unity Catalog needs a storage credential and external location for the export container.
- The notebooks add the repo root to `sys.path` so `modules/` can be imported when run from the job.

## Next steps

- Gold marts for hourly demand, borough and zone performance, payment mix and tip rate, plus a dashboard.
- A `prod` bundle target, CI (bundle validate and tests on every push), and job failure alerts.
- Extend the history beyond five months so trends are meaningful.

## License

MIT
