# Maritime Data Pipeline - Roadmap & Architecture

## 1. Phase 1 → Phase 2 Implementation Roadmap

```mermaid
flowchart LR
    A["Step 1: Download Data\nNOAA Zone10 Jan 2024 (~210 MB)\nWPI reference (~4 MB)"] --> B["Step 2: Databricks Setup\nbronze, silver, gold, maritime_ops schemas"]
    B --> C["Step 3: Bronze Ingestion\nExplicit StructType schemas\nQuarantine on drift"]
    C --> D["Step 4: Silver Transformation\nMERGE dim_vessel (Type-5)\nMERGE dim_port (WPI)\nfact_vessel_position MERGE"]
    D --> E["Step 5: Gold Aggregations\nBuilt on Silver fact+dims\nSummary tables only"]
    E --> F["Step 6: Audit Logger\nmaritime_ops.pipeline_execution_logs\nper-layer, per-batch metrics"]

    style A fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1
    style B fill:#f3e8ff,stroke:#7e22ce,stroke-width:2px,color:#581c87
    style C fill:#ffedd5,stroke:#c2410c,stroke-width:2px,color:#9a3412
    style D fill:#f1f5f9,stroke:#475569,stroke-width:2px,color:#1e293b
    style E fill:#fef9c3,stroke:#ca8a04,stroke-width:2px,color:#854d0e
    style F fill:#fee2e2,stroke:#b91c1c,stroke-width:2px,color:#7f1d1d
```

---

## 2. Phase 2 Requirements Summary

Phase 2 transitions the project from a basic script to an enterprise-grade Lakehouse pipeline. The following hard requirements must all be met:

### 2.1 Infrastructure & Environment
- Platform: **Databricks Community Edition** (or Azure with student credits)
- All notebooks/scripts committed continuously to GitHub
- Four Databricks schemas: `bronze`, `silver`, `gold`, `maritime_ops`

### 2.2 Schema Enforcement (NO inferSchema)
- All DataFrames read with **explicit `StructType` / `StructField` schemas**
- `inferSchema=True` is **forbidden**
- Casting from string to correct types happens in Silver (timestamps, doubles, integers)
- Every table in Bronze and Silver must have a `load_timestamp` column

### 2.3 Idempotency via MERGE INTO
- All Silver writes use **`MERGE INTO`** (upsert) — never plain `overwrite` or `append`
- Running the pipeline twice on the same raw data must produce zero duplicates
- `dim_vessel` MERGE key: `mmsi`; `dim_port` MERGE key: `port_code`; `fact_vessel_position` MERGE key: `(mmsi, timestamp)`

### 2.4 Parameterised Backfills
- No hardcoded `"today"` file paths
- Every notebook/function accepts a **date or batch_id parameter**
- Re-running for any historical date must re-process correctly without side effects

### 2.5 Schema Drift Handling
Two strategies to implement:
1. **`mergeSchema=True`** on Delta writes for additive column additions
2. **Quarantine** non-conforming records (wrong type, missing required field) to `bronze.quarantine` rather than crashing the batch

### 2.6 Audit Logging — `maritime_ops.pipeline_execution_logs`
Every pipeline run writes one row per layer processed:

| Column | Type | Example |
|--------|------|---------|
| `log_id` | string (UUID) | `"a1b2c3..."` |
| `layer` | string | `"Raw-to-Bronze"` / `"Bronze-to-Silver"` |
| `parameter` | string | `"2024-01-01"` / `"batch_09-30.json"` |
| `start_time` | timestamp | `2024-01-02 08:00:00` |
| `end_time` | timestamp | `2024-01-02 08:04:32` |
| `status` | string | `"Success"` / `"Failure"` |
| `rows_inserted` | long | `142850` |
| `rows_updated` | long | `320` |
| `error_message` | string | `null` / `"AnalysisException..."` |

---

## 3. End-to-End Maritime Medallion Architecture (Revised)

```mermaid
flowchart TD
    %% 1. Ingestion Sources
    subgraph SOURCING ["1. Data Sources and Ingestion"]
        WPI["World Port Index (WPI)\nReference Port Data (~4 MB)"]
        NOAA["NOAA MarineCadastre\nHistorical AIS Jan 2024\nGulf of Mexico (~210 MB)"]
        COLLECTOR["AISStream Collector\nRuns LOCALLY on your machine\nbatch JSON files -> upload to DBFS"]
    end

    %% 2. Platform Setup
    subgraph PLATFORM ["2. Databricks Schemas"]
        SCHEMAS["bronze | silver | gold | maritime_ops"]
    end

    %% 3. Bronze Layer
    subgraph BRONZE ["3. Bronze Layer (Raw + Quarantine)"]
        INGEST["Bronze PySpark Ingestion\nExplicit StructType schemas\nParameterised by date/batch_id"]
        RAW_AIS[("bronze.raw_ais_messages\nDelta Table")]
        RAW_WPI[("bronze.raw_wpi_ports\nDelta Table")]
        QUARANTINE[/"bronze.quarantine\nSchema Drift & Corrupt Records"/]
    end

    %% Observability
    subgraph OPS ["Pipeline Observability"]
        AUDIT[("maritime_ops.pipeline_execution_logs\nBatch ID, Row Counts, Timestamps, Status")]
    end

    %% 4. Silver Layer — STAR SCHEMA LIVES HERE
    subgraph SILVER ["4. Silver Layer (Star Schema — Fact & Dims)"]
        TRANSFORM["Cleaning, Casting & Deduplication\nCoordinate & Speed Validation"]
        DIM_VESSEL[("silver.dim_vessel\nMMSI (PK), IMO, Name, Type\nDraught, Destination, ETA\nUpdated via MERGE on Type-5 msgs")]
        DIM_PORT[("silver.dim_port\nPort Code (PK), Name, Country\nLat/Lon — populated from WPI")]
        DIM_VOYAGE[("silver.dim_voyage\nVoyage ID (PK), MMSI\nDeparture/Arrival Port, Timestamps")]
        FACT_POS[("silver.fact_vessel_position\nCore positional fact\nMMSI, Timestamp, Lat, Lon, SOG\nMERGE key: (mmsi, timestamp)")]
    end

    %% 5. Gold Layer — AGGREGATIONS ONLY
    subgraph GOLD ["5. Gold Layer (Aggregations Only — built on Silver)"]
        GOLD_VESSEL[("gold.gold_vessel_activity\nDistance, Active Days, Avg Speed")]
        GOLD_PORT[("gold.gold_port_performance\nArrivals, Departures, Dwell Times")]
        GOLD_ROUTE[("gold.gold_route_performance\nTransit Times, Route Delays")]
        GOLD_DENSITY[("gold.gold_daily_maritime_activity\nRegional Density & Hotspots")]
    end

    %% 6. Power BI Layer
    subgraph BI ["6. Power BI Reporting"]
        PBI_OPS["Page 1: Vessel Operations\nFleet Locations & Live Tracking"]
        PBI_ANALYTICS["Page 2: Port & Route Analytics\nDwell Bottlenecks & Transit Delays"]
        PBI_HEATMAP["Page 3: Traffic Overview\nGeographic Activity Heatmaps"]
    end

    %% Connections
    WPI --> INGEST
    NOAA --> INGEST
    COLLECTOR --> INGEST
    SCHEMAS -.- INGEST

    INGEST -->|"Valid AIS Points"| RAW_AIS
    INGEST -->|"Valid Port Records"| RAW_WPI
    INGEST -->|"Schema Drift / Corrupt"| QUARANTINE
    INGEST -.-|"Audit Metrics"| AUDIT

    RAW_AIS --> TRANSFORM
    RAW_WPI --> TRANSFORM
    TRANSFORM -.-|"Audit Metrics"| AUDIT

    TRANSFORM -->|"MERGE on mmsi"| DIM_VESSEL
    TRANSFORM -->|"MERGE on port_code"| DIM_PORT
    TRANSFORM --> DIM_VOYAGE
    TRANSFORM -->|"MERGE on (mmsi, ts)"| FACT_POS

    DIM_VESSEL --> GOLD_VESSEL
    FACT_POS --> GOLD_VESSEL
    DIM_PORT --> GOLD_PORT
    FACT_POS --> GOLD_PORT
    DIM_VOYAGE --> GOLD_ROUTE
    FACT_POS --> GOLD_ROUTE
    DIM_PORT --> GOLD_ROUTE
    FACT_POS --> GOLD_DENSITY

    GOLD_VESSEL --> PBI_OPS
    GOLD_PORT --> PBI_ANALYTICS
    GOLD_ROUTE --> PBI_ANALYTICS
    GOLD_DENSITY --> PBI_HEATMAP

    %% Styles
    classDef sourceStyle fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;
    classDef platformStyle fill:#f3e8ff,stroke:#7e22ce,stroke-width:2px,color:#581c87;
    classDef bronzeStyle fill:#ffedd5,stroke:#c2410c,stroke-width:2px,color:#9a3412;
    classDef silverStyle fill:#f1f5f9,stroke:#475569,stroke-width:2px,color:#1e293b;
    classDef goldStyle fill:#fef9c3,stroke:#ca8a04,stroke-width:2px,color:#854d0e;
    classDef opsStyle fill:#fee2e2,stroke:#b91c1c,stroke-width:2px,color:#7f1d1d;
    classDef biStyle fill:#fdf4ff,stroke:#a21caf,stroke-width:2px,color:#701a75;

    class WPI,NOAA,COLLECTOR sourceStyle;
    class SCHEMAS platformStyle;
    class INGEST,RAW_AIS,RAW_WPI bronzeStyle;
    class QUARANTINE,AUDIT opsStyle;
    class TRANSFORM,DIM_VESSEL,DIM_PORT,DIM_VOYAGE,FACT_POS silverStyle;
    class GOLD_VESSEL,GOLD_PORT,GOLD_ROUTE,GOLD_DENSITY goldStyle;
    class PBI_OPS,PBI_ANALYTICS,PBI_HEATMAP biStyle;
```

---

## 4. Phase 2 Notebook Structure (to build)

```
notebooks/
├── 00_setup_schemas.py              # Create bronze/silver/gold/maritime_ops schemas
├── 01_bronze_noaa_full_load.py      # Parameterised: accepts --date YYYY-MM
├── 02_bronze_aisstream_incremental.py  # Parameterised: accepts --batch-file path
├── 03_bronze_wpi_reference.py       # One-time WPI load (idempotent MERGE)
├── 04_silver_dim_vessel.py          # MERGE on mmsi using Type-1/5 messages
├── 05_silver_dim_port.py            # MERGE on port_code from WPI Bronze
├── 06_silver_fact_position.py       # MERGE on (mmsi, timestamp) from AIS Bronze
├── 07_silver_dim_voyage.py          # Derive voyage sequences from fact_position
├── 08_gold_aggregations.py          # All four Gold summary tables
└── 99_audit_logger.py               # Shared logging utility (imported by all above)
```

## 5. Phase 2 Submission Checklist

- [ ] All notebooks committed to GitHub with continuous commits
- [ ] `README.md` updated with Bronze and Silver data models (column names, types, PKs)
- [ ] Execution guide in `README.md` explaining backfill vs. incremental parameter usage
- [ ] `inferSchema=True` not used anywhere — all schemas explicit via `StructType`
- [ ] Every table has `load_timestamp`
- [ ] All Silver writes use `MERGE INTO` (no raw appends/overwrites for fact/dims)
- [ ] `maritime_ops.pipeline_execution_logs` populated on every run
- [ ] Schema drift sends bad records to `bronze.quarantine` instead of crashing
- [ ] AISStream collector runs locally and produces batch JSON files
- [ ] Gulf of Mexico bounding box used consistently across NOAA and AISStream
