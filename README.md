# Maritime Shipment & Cargo Tracking Pipeline (Lakehouse on Databricks)

An end-to-end Medallion Lakehouse data pipeline built on Databricks that ingests, cleans, models, and analyzes Automatic Identification System (AIS) vessel tracking data alongside global port reference data.

---

## 1. System Architecture

```
                  ┌────────────────────────────────────────┐
                  │              DATA SOURCES              │
                  └───────────────────┬────────────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          │                           │                           │
          ▼                           ▼                           ▼
┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
│ NOAA AIS (CSV)   │        │ AISStream (JSON) │        │ NGA WPI (CSV)    │
│ Full Load        │        │ Incremental Feed │        │ World Port Index │
│ ~218 MB          │        │ Live WebSocket   │        │ ~1.3 MB          │
└─────────┬────────┘        └─────────┬────────┘        └─────────┬────────┘
          │                           │                           │
          └───────────────────────────┼───────────────────────────┘
                                      ▼
                        ┌───────────────────────────┐
                        │       BRONZE LAYER        │
                        │ • Strict StructType Schema│
                        │ • Non-conforming ->       │
                        │   bronze.quarantine       │
                        │ • mergeSchema=True        │
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │       SILVER LAYER        │
                        │ (Star Schema Fact & Dims) │
                        │ • Deduplication & Cleaning│
                        │ • MERGE INTO (Idempotent) │
                        │   - dim_vessel (mmsi)     │
                        │   - dim_port (port_code)  │
                        │   - fact_vessel_position  │
                        │     (mmsi, timestamp)     │
                        │   - dim_voyage (voyage_id)│
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │        GOLD LAYER         │
                        │ (Business Aggregations)   │
                        │ • gold_vessel_activity    │
                        │ • gold_port_performance   │
                        │ • gold_route_performance  │
                        │ • gold_daily_activity     │
                        └─────────────┬─────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │   POWER BI DASHBOARDS     │
                        │ 1. Vessel Operations      │
                        │ 2. Port & Route Analytics │
                        │ 3. Traffic Overview       │
                        └───────────────────────────┘
```

---

## 2. Lakehouse Medallion Data Models

### 2.1 Bronze Tables (Raw + Metadata)
All Bronze tables include strict schema validation (`inferSchema` is strictly forbidden), plus standard metadata columns:
- `source` (STRING: `'noaa'`, `'aisstream'`, `'wpi'`)
- `ingestion_timestamp` (TIMESTAMP)
- `batch_id` (STRING)
- `load_timestamp` (TIMESTAMP)

| Table | Description | Partitioning |
| :--- | :--- | :--- |
| `bronze.raw_noaa_ais` | Historical vessel positions from NOAA MarineCadastre | Append |
| `bronze.raw_ais_messages` | Live AIS position reports & ShipStaticData from AISStream | Append |
| `bronze.raw_wpi_ports` | Global port directory from NGA Pub 150 | Append |
| `bronze.quarantine` | Corrupted rows and schema drift violations | Append |

### 2.2 Silver Layer (Star Schema — Fact & Dimensions)
All Silver tables are populated using **idempotent `MERGE INTO`** (zero duplicate records upon repeated executions).

#### `silver.dim_vessel`
*Vessel dimension capturing static attributes, dimensions, and live voyage voyage data updates (AIS Type 5).*
- **Primary Key:** `mmsi` (BIGINT)
- **Columns:**
  - `mmsi` (BIGINT, PK) — Maritime Mobile Service Identity
  - `imo` (BIGINT) — International Maritime Organization identifier
  - `vessel_name` (STRING) — Normalized ship name
  - `call_sign` (STRING) — Radio call sign
  - `vessel_type` (INT) — Vessel category code (e.g. Tanker, Cargo, Tug)
  - `length` (DOUBLE) — Vessel overall length (meters)
  - `width` (DOUBLE) — Vessel beam (meters)
  - `draught` (DOUBLE) — Current static draught (meters)
  - `destination` (STRING) — Reported destination
  - `eta` (STRING) — Estimated time of arrival
  - `load_timestamp` (TIMESTAMP) — Time of latest update

#### `silver.dim_port`
*Port dimension loaded from the NGA World Port Index with converted decimal coordinates.*
- **Primary Key:** `port_code` (STRING, e.g. `US HOU`, `US GLS`)
- **Columns:**
  - `port_code` (STRING, PK) — UN/LOCODE or WPI unique identifier
  - `port_number` (INT) — WPI Index number
  - `port_name` (STRING) — Official port name
  - `country` (STRING) — Country name
  - `country_code` (STRING) — ISO country code
  - `latitude` (DOUBLE) — Latitude in decimal degrees
  - `longitude` (DOUBLE) — Longitude in decimal degrees
  - `harbor_size` (STRING) — Port size classification (L, M, S, V)
  - `harbor_type` (STRING) — Coastal, river, or breakwater type
  - `load_timestamp` (TIMESTAMP)

#### `silver.fact_vessel_position`
*Central positional fact table capturing validated high-frequency vessel movements.*
- **Composite Primary Key:** `(mmsi, timestamp)`
- **Partitioned By:** `source`
- **Columns:**
  - `position_id` (STRING, UUID)
  - `mmsi` (BIGINT, FK -> dim_vessel)
  - `timestamp` (TIMESTAMP)
  - `latitude` (DOUBLE) — Cleaned: `[-90.0, 90.0]`
  - `longitude` (DOUBLE) — Cleaned: `[-180.0, 180.0]`
  - `sog` (DOUBLE) — Speed Over Ground in knots (`[0.0, 102.2]`)
  - `cog` (DOUBLE) — Course Over Ground in degrees (`[0.0, 360.0]`)
  - `heading` (DOUBLE) — True heading in degrees
  - `nav_status` (INT) — Underway, at anchor, moored, etc.
  - `source` (STRING) — `'noaa'` or `'aisstream'`
  - `batch_id` (STRING)
  - `load_timestamp` (TIMESTAMP)

#### `silver.dim_voyage`
*Derived voyage sequences based on temporal gaps (> 4 hours).*
- **Primary Key:** `voyage_id` (STRING: `<mmsi>-<seq>`)
- **Columns:** `voyage_id`, `mmsi`, `departure_port`, `arrival_port`, `start_ts`, `end_ts`, `total_points`, `avg_sog`, `max_sog`, `load_timestamp`

### 2.3 Gold Layer (Business Aggregations Only)
Built on top of the Silver star schema to feed Power BI dashboards:
1. `gold.gold_vessel_activity`: Distance travelled, active days, average/max speeds per vessel.
2. `gold.gold_port_performance`: Port visits, unique vessel traffic, average speeds per port/day.
3. `gold.gold_route_performance`: Transit times and delays per corridor.
4. `gold.gold_daily_maritime_activity`: Daily regional density and vessel traffic volume.

### 2.4 Observability (`maritime_ops.pipeline_execution_logs`)
Every pipeline step records execution metrics:
- `log_id` (STRING UUID)
- `layer` (STRING, e.g. `'Raw-to-Bronze (NOAA)'`, `'Bronze-to-Silver (dim_vessel)'`)
- `parameter` (STRING, e.g. batch file path or date)
- `start_time` / `end_time` (TIMESTAMP)
- `status` (`'Success'` or `'Failure'`)
- `rows_inserted` / `rows_updated` (BIGINT)
- `error_message` (STRING)

---

## 3. Notebook Execution Guide

The pipeline notebooks are located in `notebooks/`:

```
notebooks/
├── 00_setup_schemas.py                 # Initializes bronze, silver, gold, maritime_ops & quarantine
├── 01_bronze_noaa_full_load.py         # Full load ingestion (parameterised)
├── 02_bronze_aisstream_incremental.py  # Incremental live JSON ingestion (parameterised)
├── 03_bronze_wpi_reference.py          # Reference port data ingestion (parameterised)
├── 04_silver_dim_vessel.py             # Upsert dim_vessel via MERGE (Type 1 & 5)
├── 05_silver_dim_port.py               # Upsert dim_port via MERGE
├── 06_silver_fact_position.py          # Upsert fact_vessel_position via MERGE on (mmsi, ts)
├── 07_silver_dim_voyage.py             # Derive voyage dimension from positional facts
├── 08_gold_aggregations.py             # Build all 4 Gold summary tables
└── 99_audit_logger.py                  # Shared logging utility
```

### Parameter Usage & Backfills

Each ingestion notebook uses **Databricks Widgets** so it can be parameterized via UI or automated Databricks Workflows/Jobs:

#### 1. Full Load Execution
Open `01_bronze_noaa_full_load` in Databricks and specify:
- `source_path`: Path to full load CSV (e.g. `/FileStore/tables/AIS_Full_Load.csv`)
- `batch_id`: `2024-01-full-load`

#### 2. Incremental Daily Execution
Open `02_bronze_aisstream_incremental` and specify:
- `batch_file`: Path to daily JSON Lines file (e.g. `/FileStore/tables/incremental_load/ais_daily_20261003.json`)
- `batch_id`: `incremental-20261003`

#### 3. Backfill Execution
To reprocess an earlier historical batch or date:
Pass the historical file path and batch ID to `01_bronze_noaa_full_load` or `02_bronze_aisstream_incremental`. Because Silver writes use `MERGE INTO`, **re-running on historical batches is completely idempotent** and produces zero duplicate rows.

---

## 4. Local AISStream Collector

The collector runs locally on your machine outside Databricks and writes clean daily JSON Lines files:

```bash
# Run foreground
python src/collector/aisstream_collector.py

# Or run in background for 24-hour collection:
nohup python src/collector/aisstream_collector.py > collector.log 2>&1 &
```

- Bounding box: Galveston Bay / Houston entrance corridor `[[29.351, -94.702], [29.201, -94.536]]` (exact match to NOAA full load).
- Output: `data/samples/incremental_load/ais_daily_YYYYMMDD.json`.
