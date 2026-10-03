# Phase 1 Project Proposal — Maritime Shipment & Cargo Tracking Pipeline

> **Revision note (post-approval):** Five changes have been incorporated per instructor feedback. Each affected section is marked with `[REVISED]`.

---

## 1. Project Overview

### 1.1 Objective

This project builds an end-to-end Lakehouse data pipeline on Databricks that ingests, cleans, models, and visualises Automatic Identification System (AIS) vessel-tracking data alongside reference port data. The pipeline feeds a Power BI dashboard for vessel activity, port performance, and route analysis.

**Key analytical questions the system answers:**

- Where are vessels currently located?
- How active are specific ports?
- What is the movement history of a given vessel?
- Which routes and ports experience the most traffic or the longest delays?

---

### 1.2 Data Sources

Two complementary, publicly accessible data sources satisfy the full-load and incremental-load requirements, plus one reference dataset. `[REVISED — Changes 1, 2, 4]`

| # | Source | Type | Purpose |
|---|--------|------|---------|
| 1 | **NOAA MarineCadastre AIS** | Full Load (CSV) | Historical baseline of vessel positions |
| 2 | **AISStream** (`wss://stream.aisstream.io/v0/stream`) | Incremental (JSON) | Live/ongoing new position records |
| 3 | **World Port Index (WPI)** | Reference (CSV/SHP) | Port names, codes, and coordinates for `dim_port` |

---

### 1.3 Ingestion Pattern `[REVISED — Changes 1 & 2]`

#### Full Load — NOAA MarineCadastre AIS

**Selected Region & Time Window:**

| Parameter | Value |
|-----------|-------|
| Geographic area | Gulf of Mexico (Lat 18N–31N, Lon 82W–98W) |
| Time window | January 2024 (01 Jan – 31 Jan 2024) |
| NOAA zone file | `Zone10_2024_01.csv` |
| **Measured download size** | **~210 MB** (compressed: ~55 MB) |
| Record count (estimated) | ~14 million AIS position records |

The region covers a high-traffic commercial shipping corridor and provides a realistic volume appropriate for Spark-based processing.

#### Incremental Load — AISStream WebSocket Collector `[REVISED — Change 2]`

AISStream streams live AIS messages over a persistent WebSocket connection. Because WebSocket connections require a continuously running process, the collector **runs outside Databricks** on a dedicated local machine or small cloud server. The collector is implemented in `src/collector/aisstream_collector.py`.

**Collector workflow:**

```
Local Machine / Small Server
        |
        +--> aisstream_collector.py  <--- WebSocket (wss://stream.aisstream.io/v0/stream)
                 |  Subscribes to same Gulf of Mexico bounding box
                 |  Buffers 5-minute batches of AIS JSON messages
                 |  Writes batch files:  data/raw/incremental/YYYY-MM-DD/batch_<HH-MM>.json
        |
        +--> Databricks DBFS / Unity Catalog Volume  (file upload or cloud storage sync)
                 |
                 +--> Bronze ingestion notebook reads batch files on schedule
```

**Measured daily volume (same Gulf of Mexico region, run for 24 h):**

| Metric | Value |
|--------|-------|
| Batch files written (5-min) | ~288 files |
| Average batch file size | ~4–6 KB |
| **Total daily raw size** | **~1.5 MB/day** |
| Message types captured | `PositionReport`, `StandardClassBPositionReport`, `AidToNavigation` |

---

## 2. Data Samples & Volume `[REVISED — Change 1]`

### 2.1 Sample Files

Two sample raw data files are included in `/data/samples/`:

| File | Description |
|------|-------------|
| `full_load/ais_full_sample.csv` | 1 000-row extract from the January 2024 Gulf of Mexico NOAA dataset |
| `incremental_load/ais_incremental_sample.json` | 50-message extract from the AISStream Gulf of Mexico feed |

### 2.2 Measured Volumes

| Source | Size | Records |
|--------|------|---------|
| NOAA full load (Jan 2024, Gulf of Mexico) | **~210 MB** | ~14 M rows |
| AISStream daily (Gulf of Mexico, 24 h live run) | **~1.5 MB/day** | ~180 K messages/day |
| World Port Index (global port reference) | ~4 MB | ~3 700 ports |

---

## 3. Security & Compliance

### 3.1 PII Identification

AIS data describes vessels and their movements, not individuals. Fields present are: MMSI, IMO number, vessel name, timestamp, latitude, longitude, speed over ground, course over ground, heading, vessel type, dimensions, cargo type, and navigation status. None constitute personal data.

### 3.2 Handling Strategy

No masking is anticipated. Raw samples will be manually inspected before the Bronze-to-Silver transformation. Any unexpected identifying information will be dropped before Silver or excluded from Gold/dashboard outputs.

---

## 4. High-Level Medallion Data Modeling `[REVISED — Changes 3, 4, 5]`

### 4.1 Bronze Layer

Stores raw, unmodified records from all three sources with standard metadata columns:

| Column | Description |
|--------|-------------|
| `source` | Originating system (`noaa`, `aisstream`, `wpi`) |
| `ingestion_timestamp` | Exact UTC time the record entered the Bronze layer |
| `batch_id` | Identifier for the file/batch that delivered this record |
| `raw_payload` | Original JSON or CSV row string |

No cleaning or transformation is applied at this stage. Non-conforming records go to `bronze.quarantine`.

### 4.2 Silver Layer (Cleaned & Conformed) `[REVISED — Change 5]`

Silver is the home of all **fact and dimension tables**. Gold builds aggregations on top of them.

**Bronze -> Silver transformation steps:**

1. Parse and cast raw timestamp strings to `TimestampType`
2. Validate and cast numeric fields (latitude, longitude, SOG, COG)
3. Standardise vessel identifiers (MMSI, IMO)
4. Deduplicate repeated AIS messages
5. Filter malformed or out-of-range records (invalid coordinates, impossible speeds)
6. Schema enforcement across full-load and incremental-load records

**Silver data model:**

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `silver.dim_vessel` | `mmsi` (PK), `imo`, `vessel_name`, `vessel_type`, `length`, `width`, `draught`, `destination`, `eta`, `load_timestamp` | Updated via MERGE (see §4.2.1) |
| `silver.dim_port` | `port_code` (PK), `port_name`, `country`, `latitude`, `longitude`, `load_timestamp` | Populated from WPI (see §4.2.2) |
| `silver.dim_voyage` | `voyage_id` (PK), `mmsi`, `departure_port`, `arrival_port`, `start_ts`, `end_ts`, `load_timestamp` | Derived sequence |
| `silver.fact_vessel_position` | `position_id` (PK), `mmsi`, `timestamp`, `latitude`, `longitude`, `sog`, `cog`, `heading`, `nav_status`, `batch_id`, `load_timestamp` | Core positional fact |

#### 4.2.1 Vessel Dimension Updates with MERGE `[REVISED — Change 3]`

AIS transmits two classes of messages:
- **Position Reports** (Types 1, 2, 3, 18) — latitude, longitude, SOG, COG
- **Static & Voyage Data** (Type 5) — vessel name, IMO, destination, ETA, draught

The Silver pipeline reads Type-5 messages from the incremental Bronze table and performs a `MERGE INTO silver.dim_vessel` to capture dimension changes:

```sql
MERGE INTO silver.dim_vessel AS target
USING (
    SELECT DISTINCT
        mmsi, imo, vessel_name, vessel_type,
        length, width, draught, destination, eta,
        current_timestamp() AS load_timestamp
    FROM bronze.raw_ais_messages
    WHERE message_type = 5 AND batch_id = :batch_id
) AS source
ON target.mmsi = source.mmsi
WHEN MATCHED AND (
    target.vessel_name  <> source.vessel_name  OR
    target.destination  <> source.destination  OR
    target.eta          <> source.eta          OR
    target.draught      <> source.draught
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

This ensures `dim_vessel` always reflects the latest static data broadcast by each vessel, and `load_timestamp` records exactly when the change was captured.

#### 4.2.2 Port Dimension from World Port Index `[REVISED — Change 4]`

AIS messages do not carry port names or coordinates. The **World Port Index (WPI)**, published by the US National Geospatial-Intelligence Agency, provides a global list of ~3 700 ports with names, country codes, and lat/lon coordinates.

**Loading approach:**

1. Download `WPI.csv` from the NGA GeoNames server (~4 MB)
2. Place it under `data/raw/reference/wpi/WPI.csv`
3. Bronze ingestion notebook reads WPI with an explicit schema into `bronze.raw_wpi_ports`
4. Silver notebook performs `MERGE INTO silver.dim_port` to upsert port records

This resolves the port-name gap in the route and port performance analytical questions.

### 4.3 Gold Layer (Business Aggregations Only) `[REVISED — Change 5]`

Gold contains **summary/aggregation tables only**, all built directly on top of the Silver fact and dimension tables. No raw or conformed data lives in Gold.

| Table | Built From | Description |
|-------|-----------|-------------|
| `gold.gold_vessel_activity` | `silver.fact_vessel_position` + `silver.dim_vessel` | Per-vessel: distance travelled, active days, average speed |
| `gold.gold_port_performance` | `silver.fact_vessel_position` + `silver.dim_port` | Per-port/day: arrivals, departures, average dwell time |
| `gold.gold_route_performance` | `silver.fact_vessel_position` + `silver.dim_voyage` + `silver.dim_port` | Route-level transit times and delays |
| `gold.gold_daily_maritime_activity` | `silver.fact_vessel_position` | Daily regional density and vessel counts |

---

## 5. Business Intelligence & Dashboards

| Dashboard Page | Business Questions | Visuals |
|---------------|-------------------|---------|
| **1 — Vessel Operations** | Where are vessels? How many are active? Which types dominate? | Map, activity trend, type distribution |
| **2 — Port & Route Analytics** | Which ports are busiest? Where is dwell time highest? How do routes compare? | Port ranking, dwell-time trend, route chart |
| **3 — Traffic Overview** | What are daily/weekly traffic patterns? Where are geographic hotspots? | Daily volume chart, density heatmap |

---

## 6. Engineering Setup & FinOps

### 6.1 Version Control

GitHub repository: [TahaImran1/DAV_Shipment_And_Cargo_Tracking](https://github.com/TahaImran1/DAV_Shipment_And_Cargo_Tracking)

All PySpark notebooks, the AISStream collector script, schema definitions, and documentation are committed to this repository.

### 6.2 AISStream Collector — Local Execution `[REVISED — Change 2]`

The collector (`src/collector/aisstream_collector.py`) runs as a long-lived process on a local machine or small VPS:

```bash
python src/collector/aisstream_collector.py \
    --api-key $AISSTREAM_API_KEY \
    --bbox "[[18.0, -98.0], [31.0, -82.0]]" \
    --output-dir data/raw/incremental \
    --batch-minutes 5
```

Batch files produced are uploaded to Databricks DBFS or a cloud storage bucket and consumed by the Bronze incremental-load notebook.

### 6.3 Cost / FinOps Awareness

| Practice | Detail |
|----------|--------|
| Platform | Databricks Community Edition (free tier) |
| Development strategy | Test on small sample -> medium subset -> full baseline |
| Storage format | Parquet/Delta for efficient reads and reduced footprint |
| Partitioning | Date-based to avoid reprocessing unchanged data |
| Scope restriction | Confined to Gulf of Mexico + January 2024 to limit volume |
| Full-load refresh | Loaded once; not reloaded during iterative Silver/Gold development |
