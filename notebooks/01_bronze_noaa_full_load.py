# Databricks notebook source
# COMMAND ----------
"""
Notebook: 01_bronze_noaa_full_load
Layer: Raw-to-Bronze
Source: NOAA MarineCadastre AIS Historical Full Load (CSV)
Target Table: bronze.raw_noaa_ais

Requirements Enforced:
  - Parameterised backfill via Databricks widgets (source_path, batch_id)
  - Explicit StructType schema (inferSchema=True is STRICTLY FORBIDDEN)
  - Metadata columns added: source, ingestion_timestamp, batch_id, load_timestamp
  - Corrupt records quarantined to bronze.quarantine
  - Additive schema drift supported via mergeSchema=True
  - Execution audit logged to maritime_ops.pipeline_execution_logs
"""

# COMMAND ----------
# MAGIC %run ./99_audit_logger

# COMMAND ----------
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col,
    expr,
    to_json,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
)

# COMMAND ----------
# Widget Parameters (Supports parameterised backfills & execution)
try:
    dbutils.widgets.text("source_path", "/FileStore/tables/AIS_Full_Load.csv", "Source File Path")
    dbutils.widgets.text("batch_id", "2024-01-full-load", "Batch Identifier")
    source_path = dbutils.widgets.get("source_path")
    batch_id = dbutils.widgets.get("batch_id")
except Exception:
    source_path = "data/samples/full_load/AIS_Full_Load.csv"
    batch_id = "2024-01-full-load"

print(f"Ingesting NOAA AIS Full Load: source_path='{source_path}', batch_id='{batch_id}'")

# COMMAND ----------
# Explicit StructType Schema for NOAA MarineCadastre CSV (NO inferSchema)
NOAA_BRONZE_SCHEMA = StructType([
    StructField("MMSI", StringType(), True),
    StructField("BaseDateTime", StringType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("SOG", DoubleType(), True),
    StructField("COG", DoubleType(), True),
    StructField("Heading", DoubleType(), True),
    StructField("VesselName", StringType(), True),
    StructField("IMO", StringType(), True),
    StructField("CallSign", StringType(), True),
    StructField("VesselType", IntegerType(), True),
    StructField("Status", IntegerType(), True),
    StructField("Length", DoubleType(), True),
    StructField("Width", DoubleType(), True),
    StructField("Draft", DoubleType(), True),
    StructField("Cargo", IntegerType(), True),
    StructField("transceiverClass", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# COMMAND ----------
with PipelineLogger(spark, layer="Raw-to-Bronze (NOAA)", parameter=batch_id) as logger:
    # 1. Read with strict schema and PERMISSIVE corrupt-record capture
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(NOAA_BRONZE_SCHEMA)
        .load(source_path)
    )

    # 2. Separate corrupt / non-conforming records for quarantine
    corrupt_df = raw_df.filter(col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_df.count()

    if corrupt_count > 0:
        quarantine_records = (
            corrupt_df.select(
                expr("uuid()").alias("quarantine_id"),
                lit("noaa").alias("source"),
                lit(batch_id).alias("batch_id"),
                col("_corrupt_record").alias("raw_payload"),
                lit("CSV schema violation / malformed record").alias("error_reason"),
                current_timestamp().alias("quarantine_timestamp"),
            )
        )
        quarantine_records.write.format("delta").mode("append").saveAsTable("bronze.quarantine")
        print(f"[WARN] Quarantined {corrupt_count} corrupt records to bronze.quarantine")

    # 3. Add audit and lineage metadata to conforming records
    valid_df = (
        raw_df.filter(col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        .withColumn("source", lit("noaa"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
        .withColumn("load_timestamp", current_timestamp())
    )

    # 4. Ingest into Bronze Delta Table with additive schema drift support
    (
        valid_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("bronze.raw_noaa_ais")
    )

    valid_count = valid_df.count()
    logger.set_metrics(rows_inserted=valid_count, rows_updated=0)
    print(f"[SUCCESS] Ingested {valid_count} valid records into bronze.raw_noaa_ais")
