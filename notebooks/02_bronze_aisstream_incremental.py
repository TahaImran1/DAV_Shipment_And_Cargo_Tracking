# Databricks notebook source
# COMMAND ----------
"""
Notebook: 02_bronze_aisstream_incremental
Layer: Raw-to-Bronze
Source: AISStream Live WebSocket Incremental Feed (JSON / JSON Lines)
Target Table: bronze.raw_ais_messages

Requirements Enforced:
  - Parameterised ingestion via Databricks widgets (batch_file, batch_id)
  - Explicit StructType schema (inferSchema=True is STRICTLY FORBIDDEN)
  - Supports PositionReport (Types 1-3), ShipStaticData (Type 5), StaticDataReport (Type 24)
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
    BooleanType,
)

# COMMAND ----------
# Widget Parameters
try:
    dbutils.widgets.text("batch_file", "/FileStore/tables/incremental_load/", "Source Batch File or Directory")
    dbutils.widgets.text("batch_id", "incremental-auto", "Batch Identifier")
    batch_file = dbutils.widgets.get("batch_file")
    batch_id = dbutils.widgets.get("batch_id")
except Exception:
    batch_file = "data/samples/incremental_load/"
    batch_id = "incremental-auto"

print(f"Ingesting AISStream Incremental Batch: batch_file='{batch_file}', batch_id='{batch_id}'")

# COMMAND ----------
# Dimension Struct (Ship Dimensions: Distance from GPS antenna to Bow, Stern, Port, Starboard)
DIMENSION_SCHEMA = StructType([
    StructField("A", IntegerType(), True),
    StructField("B", IntegerType(), True),
    StructField("C", IntegerType(), True),
    StructField("D", IntegerType(), True),
])

# ETA Struct
ETA_SCHEMA = StructType([
    StructField("Month", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("Hour", IntegerType(), True),
    StructField("Minute", IntegerType(), True),
])

# PositionReport Struct (Types 1, 2, 3)
POSITION_REPORT_SCHEMA = StructType([
    StructField("MessageID", IntegerType(), True),
    StructField("RepeatIndicator", IntegerType(), True),
    StructField("UserID", LongType(), True),
    StructField("Valid", BooleanType(), True),
    StructField("NavigationalStatus", IntegerType(), True),
    StructField("RateOfTurn", DoubleType(), True),
    StructField("Sog", DoubleType(), True),
    StructField("PositionAccuracy", BooleanType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Cog", DoubleType(), True),
    StructField("TrueHeading", DoubleType(), True),
    StructField("Timestamp", IntegerType(), True),
    StructField("SpecialManoeuvreIndicator", IntegerType(), True),
    StructField("Spare", IntegerType(), True),
    StructField("Raim", BooleanType(), True),
    StructField("CommunicationState", LongType(), True),
])

# ShipStaticData Struct (Type 5)
SHIP_STATIC_DATA_SCHEMA = StructType([
    StructField("MessageID", IntegerType(), True),
    StructField("RepeatIndicator", IntegerType(), True),
    StructField("UserID", LongType(), True),
    StructField("Valid", BooleanType(), True),
    StructField("AisVersion", IntegerType(), True),
    StructField("ImoNumber", LongType(), True),
    StructField("CallSign", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Type", IntegerType(), True),
    StructField("Dimension", DIMENSION_SCHEMA, True),
    StructField("FixType", IntegerType(), True),
    StructField("Eta", ETA_SCHEMA, True),
    StructField("MaximumStaticDraught", DoubleType(), True),
    StructField("Destination", StringType(), True),
    StructField("Dte", IntegerType(), True),
    StructField("Spare", BooleanType(), True),
])

# StaticDataReport Struct (Type 24)
STATIC_DATA_REPORT_SCHEMA = StructType([
    StructField("MessageID", IntegerType(), True),
    StructField("RepeatIndicator", IntegerType(), True),
    StructField("UserID", LongType(), True),
    StructField("Valid", BooleanType(), True),
    StructField("PartNumber", IntegerType(), True),
])

MESSAGE_BODY_SCHEMA = StructType([
    StructField("PositionReport", POSITION_REPORT_SCHEMA, True),
    StructField("ShipStaticData", SHIP_STATIC_DATA_SCHEMA, True),
    StructField("StaticDataReport", STATIC_DATA_REPORT_SCHEMA, True),
])

METADATA_SCHEMA = StructType([
    StructField("MMSI", LongType(), True),
    StructField("MMSI_String", LongType(), True),
    StructField("ShipName", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("time_utc", StringType(), True),
])

# Full Top-Level AISStream JSON Schema (Strict StructType - NO inferSchema)
AISSTREAM_BRONZE_SCHEMA = StructType([
    StructField("MetaData", METADATA_SCHEMA, True),
    StructField("MessageType", StringType(), True),
    StructField("Message", MESSAGE_BODY_SCHEMA, True),
    StructField("_collected_at", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# COMMAND ----------
with PipelineLogger(spark, layer="Raw-to-Bronze (AISStream)", parameter=batch_id) as logger:
    # 1. Read JSON batches with explicit schema and corrupt-record capture
    raw_df = (
        spark.read.format("json")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(AISSTREAM_BRONZE_SCHEMA)
        .load(batch_file)
    )

    # 2. Quarantine any corrupt or unparseable JSON records
    corrupt_df = raw_df.filter(col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_df.count()

    if corrupt_count > 0:
        quarantine_records = (
            corrupt_df.select(
                expr("uuid()").alias("quarantine_id"),
                lit("aisstream").alias("source"),
                lit(batch_id).alias("batch_id"),
                col("_corrupt_record").alias("raw_payload"),
                lit("JSON decode or schema mismatch error").alias("error_reason"),
                current_timestamp().alias("quarantine_timestamp"),
            )
        )
        quarantine_records.write.format("delta").mode("append").saveAsTable("bronze.quarantine")
        print(f"[WARN] Quarantined {corrupt_count} records to bronze.quarantine")

    # 3. Add standard metadata columns
    valid_df = (
        raw_df.filter(col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        .withColumn("source", lit("aisstream"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
        .withColumn("load_timestamp", current_timestamp())
    )

    # 4. Ingest into Bronze Delta Table
    (
        valid_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("bronze.raw_ais_messages")
    )

    valid_count = valid_df.count()
    logger.set_metrics(rows_inserted=valid_count, rows_updated=0)
    print(f"[SUCCESS] Ingested {valid_count} records into bronze.raw_ais_messages")
