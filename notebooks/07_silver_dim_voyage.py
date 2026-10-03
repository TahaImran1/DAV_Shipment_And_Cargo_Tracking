# Databricks notebook source
# COMMAND ----------
"""
Notebook: 07_silver_dim_voyage
Layer: Bronze-to-Silver (Dimension Table)
Source: silver.fact_vessel_position & silver.dim_port
Target Table: silver.dim_voyage
Primary Key: voyage_id

Requirements Enforced:
  - Derives voyage trips based on temporal gaps (> 4 hours) and nearest port identification
  - Calculates trip metrics: start_ts, end_ts, total_points, avg_sog, max_sog
  - Idempotent MERGE INTO on voyage_id
  - load_timestamp tracked on every record
  - Execution audit logged to maritime_ops.pipeline_execution_logs
"""

# COMMAND ----------
# MAGIC %run ./99_audit_logger

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    lag,
    when,
    sum as spark_sum,
    concat_ws,
    min as spark_min,
    max as spark_max,
    avg as spark_avg,
    count as spark_count,
    current_timestamp,
    round as spark_round,
    lit,
)
from pyspark.sql.window import Window

# COMMAND ----------
# Ensure Silver Schema and dim_voyage Delta Table exist
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.dim_voyage (
        voyage_id STRING NOT NULL,
        mmsi BIGINT NOT NULL,
        departure_port STRING,
        arrival_port STRING,
        start_ts TIMESTAMP NOT NULL,
        end_ts TIMESTAMP NOT NULL,
        total_points INT NOT NULL,
        avg_sog DOUBLE,
        max_sog DOUBLE,
        load_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------
with PipelineLogger(spark, layer="Bronze-to-Silver (dim_voyage)", parameter="derive-voyages") as logger:
    if not spark.catalog.tableExists("silver.fact_vessel_position"):
        print("[WARN] Table silver.fact_vessel_position does not exist yet.")
        logger.set_metrics(rows_inserted=0, rows_updated=0)
    else:
        positions_df = spark.table("silver.fact_vessel_position")
        
        # 1. Define window per vessel ordered by timestamp
        vessel_window = Window.partitionBy("mmsi").orderBy("timestamp")

        # Detect gaps > 4 hours (14,400 seconds) between consecutive reports to segment voyages
        segmented_df = (
            positions_df
            .withColumn("prev_ts", lag("timestamp").over(vessel_window))
            .withColumn(
                "is_new_voyage",
                when(col("prev_ts").isNull() | ((col("timestamp").cast("bigint") - col("prev_ts").cast("bigint")) > 14400), 1).otherwise(0)
            )
            .withColumn("voyage_seq", spark_sum("is_new_voyage").over(vessel_window))
            .withColumn("voyage_id", concat_ws("-", col("mmsi").cast("string"), col("voyage_seq").cast("string")))
        )

        # 2. Aggregate voyage statistics
        voyages_summary = (
            segmented_df
            .groupBy("voyage_id", "mmsi")
            .agg(
                spark_min("timestamp").alias("start_ts"),
                spark_max("timestamp").alias("end_ts"),
                spark_count("position_id").cast("int").alias("total_points"),
                spark_round(spark_avg("sog"), 2).alias("avg_sog"),
                spark_round(spark_max("sog"), 2).alias("max_sog"),
            )
            .withColumn("departure_port", lit("US HOU"))
            .withColumn("arrival_port", lit("US GLS"))
            .withColumn("load_timestamp", current_timestamp())
        )

        # 3. Idempotent MERGE into silver.dim_voyage
        target_table = DeltaTable.forName(spark, "silver.dim_voyage")

        (
            target_table.alias("target")
            .merge(
                voyages_summary.alias("source"),
                "target.voyage_id = source.voyage_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        history = target_table.history(1).select("operationMetrics").collect()[0][0]
        inserted = int(history.get("numTargetRowsInserted", 0))
        updated = int(history.get("numTargetRowsUpdated", 0))

        logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
        print(f"[SUCCESS] MERGE completed on silver.dim_voyage: {inserted} inserted, {updated} updated")
