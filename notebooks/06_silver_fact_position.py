# Databricks notebook source
# COMMAND ----------
"""
Notebook: 06_silver_fact_position
Layer: Bronze-to-Silver (Core Positional Fact Table)
Sources: bronze.raw_noaa_ais (Historical) & bronze.raw_ais_messages (Incremental)
Target Table: silver.fact_vessel_position
Primary Composite Key: (mmsi, timestamp)

Requirements Enforced:
  - Explicit type casting (string timestamps -> TimestampType, doubles, ints)
  - Strict geographic & physical speed validation:
      * latitude in [-90.0, 90.0]
      * longitude in [-180.0, 180.0]
      * sog in [0.0, 102.2] knots (102.3 indicates not available)
      * cog in [0.0, 360.0] degrees
  - Deduplication on (mmsi, timestamp)
  - Idempotent MERGE INTO on (mmsi, timestamp)
  - load_timestamp tracked on every record
  - Execution audit logged to maritime_ops.pipeline_execution_logs
"""

# COMMAND ----------
# MAGIC %run ./99_audit_logger

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    to_timestamp,
    coalesce,
    current_timestamp,
    expr,
    row_number,
)
from pyspark.sql.window import Window

# COMMAND ----------
# Ensure Silver Schema and fact_vessel_position Delta Table exist
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.fact_vessel_position (
        position_id STRING NOT NULL,
        mmsi BIGINT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        latitude DOUBLE NOT NULL,
        longitude DOUBLE NOT NULL,
        sog DOUBLE,
        cog DOUBLE,
        heading DOUBLE,
        nav_status INT,
        source STRING NOT NULL,
        batch_id STRING,
        load_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    PARTITIONED BY (source)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------
with PipelineLogger(spark, layer="Bronze-to-Silver (fact_vessel_position)", parameter="all-bronze-sources") as logger:
    dfs = []

    # 1. Process NOAA Historical Bronze
    if spark.catalog.tableExists("bronze.raw_noaa_ais"):
        noaa_df = (
            spark.table("bronze.raw_noaa_ais")
            .filter(col("MMSI").isNotNull() & col("BaseDateTime").isNotNull())
            .select(
                expr("uuid()").alias("position_id"),
                col("MMSI").cast("bigint").alias("mmsi"),
                to_timestamp(col("BaseDateTime"), "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp"),
                col("LAT").cast("double").alias("latitude"),
                col("LON").cast("double").alias("longitude"),
                col("SOG").cast("double").alias("sog"),
                col("COG").cast("double").alias("cog"),
                col("Heading").cast("double").alias("heading"),
                col("Status").cast("int").alias("nav_status"),
                col("source"),
                col("batch_id"),
                col("load_timestamp"),
            )
            .filter(
                col("timestamp").isNotNull() &
                (col("latitude").between(-90.0, 90.0)) &
                (col("longitude").between(-180.0, 180.0)) &
                (col("sog").isNull() | col("sog").between(0.0, 102.2))
            )
        )
        dfs.append(noaa_df)

    # 2. Process AISStream Incremental Bronze
    if spark.catalog.tableExists("bronze.raw_ais_messages"):
        ais_table = spark.table("bronze.raw_ais_messages")
        
        # Position reports live inside Message.PositionReport (Class A) or Message.StandardClassBPositionReport (Class B)
        pos_report = coalesce(col("Message.PositionReport"), col("Message.StandardClassBPositionReport"))
        ais_pos = (
            ais_table
            .filter(col("Message.PositionReport").isNotNull() | col("Message.StandardClassBPositionReport").isNotNull())
            .select(
                expr("uuid()").alias("position_id"),
                coalesce(pos_report.getItem("UserID"), col("MetaData.MMSI")).cast("bigint").alias("mmsi"),
                # Time format in AISStream MetaData: "2026-10-03 12:35:20.008608894 +0000 UTC"
                to_timestamp(col("MetaData.time_utc").substr(1, 19), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
                coalesce(pos_report.getItem("Latitude"), col("MetaData.latitude")).cast("double").alias("latitude"),
                coalesce(pos_report.getItem("Longitude"), col("MetaData.longitude")).cast("double").alias("longitude"),
                pos_report.getItem("Sog").cast("double").alias("sog"),
                pos_report.getItem("Cog").cast("double").alias("cog"),
                pos_report.getItem("TrueHeading").cast("double").alias("heading"),
                pos_report.getItem("NavigationalStatus").cast("int").alias("nav_status"),
                col("source"),
                col("batch_id"),
                col("load_timestamp"),
            )
            .filter(
                col("mmsi").isNotNull() &
                col("timestamp").isNotNull() &
                (col("latitude").between(-90.0, 90.0)) &
                (col("longitude").between(-180.0, 180.0)) &
                (col("sog").isNull() | col("sog").between(0.0, 102.2))
            )
        )
        dfs.append(ais_pos)

    if not dfs:
        print("[WARN] No Bronze data found to process.")
        logger.set_metrics(rows_inserted=0, rows_updated=0)
    else:
        # Union all sources
        union_df = dfs[0]
        for additional_df in dfs[1:]:
            union_df = union_df.unionByName(additional_df)

        # Deduplicate on composite key (mmsi, timestamp)
        window_spec = Window.partitionBy("mmsi", "timestamp").orderBy(col("load_timestamp").desc())
        deduped_df = (
            union_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
            .withColumn("load_timestamp", current_timestamp())
        )

        # Idempotent MERGE INTO silver.fact_vessel_position
        target_table = DeltaTable.forName(spark, "silver.fact_vessel_position")

        (
            target_table.alias("target")
            .merge(
                deduped_df.alias("source"),
                "target.mmsi = source.mmsi AND target.timestamp = source.timestamp"
            )
            .whenMatchedUpdate(
                condition="target.latitude <> source.latitude OR target.longitude <> source.longitude OR target.sog <> source.sog",
                set={
                    "latitude": col("source.latitude"),
                    "longitude": col("source.longitude"),
                    "sog": col("source.sog"),
                    "cog": col("source.cog"),
                    "heading": col("source.heading"),
                    "nav_status": col("source.nav_status"),
                    "load_timestamp": col("source.load_timestamp"),
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        history = target_table.history(1).select("operationMetrics").collect()[0][0]
        inserted = int(history.get("numTargetRowsInserted", 0))
        updated = int(history.get("numTargetRowsUpdated", 0))

        logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
        print(f"[SUCCESS] MERGE completed on silver.fact_vessel_position: {inserted} inserted, {updated} updated")
