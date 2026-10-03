# Databricks notebook source
# COMMAND ----------
"""
Notebook: 03_bronze_wpi_reference
Layer: Raw-to-Bronze
Source: National Geospatial-Intelligence Agency (NGA) World Port Index (Pub 150)
Target Table: bronze.raw_wpi_ports

Requirements Enforced:
  - Parameterised path via widgets (source_path, batch_id)
  - Explicit StructType schema (inferSchema=True is STRICTLY FORBIDDEN)
  - Adds standard metadata: source, ingestion_timestamp, batch_id, load_timestamp
  - Quarantine on schema drift or corrupt records
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
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# COMMAND ----------
# Widget Parameters
try:
    dbutils.widgets.text("source_path", "/FileStore/tables/WPI.csv", "Source WPI Path")
    dbutils.widgets.text("batch_id", "wpi-reference-v1", "Batch Identifier")
    source_path = dbutils.widgets.get("source_path")
    batch_id = dbutils.widgets.get("batch_id")
except Exception:
    source_path = "data/samples/wpi/WPI.csv"
    batch_id = "wpi-reference-v1"

print(f"Ingesting World Port Index (WPI): source_path='{source_path}', batch_id='{batch_id}'")

# COMMAND ----------
# Explicit StructType Schema for WPI (Strictly defined)
WPI_BRONZE_SCHEMA = StructType([
    StructField("portNumber", IntegerType(), True),
    StructField("portName", StringType(), True),
    StructField("regionNumber", IntegerType(), True),
    StructField("regionName", StringType(), True),
    StructField("countryCode", StringType(), True),
    StructField("countryName", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("publicationNumber", StringType(), True),
    StructField("chartNumber", StringType(), True),
    StructField("navArea", StringType(), True),
    StructField("harborSize", StringType(), True),
    StructField("harborType", StringType(), True),
    StructField("shelter", StringType(), True),
    StructField("erTide", StringType(), True),
    StructField("erSwell", StringType(), True),
    StructField("erIce", StringType(), True),
    StructField("erOther", StringType(), True),
    StructField("overheadLimits", StringType(), True),
    StructField("chDepth", StringType(), True),
    StructField("anDepth", StringType(), True),
    StructField("cpDepth", StringType(), True),
    StructField("otDepth", StringType(), True),
    StructField("tide", StringType(), True),
    StructField("maxVesselLength", StringType(), True),
    StructField("maxVesselBeam", StringType(), True),
    StructField("maxVesselDraft", StringType(), True),
    StructField("goodHoldingGround", StringType(), True),
    StructField("turningArea", StringType(), True),
    StructField("firstPortOfEntry", StringType(), True),
    StructField("usRep", StringType(), True),
    StructField("ptCompulsory", StringType(), True),
    StructField("ptAvailable", StringType(), True),
    StructField("ptLocalAssist", StringType(), True),
    StructField("ptAdvisable", StringType(), True),
    StructField("tugsSalvage", StringType(), True),
    StructField("tugsAssist", StringType(), True),
    StructField("qtPratique", StringType(), True),
    StructField("qtOther", StringType(), True),
    StructField("cmTelephone", StringType(), True),
    StructField("cmTelegraph", StringType(), True),
    StructField("cmRadio", StringType(), True),
    StructField("cmRadioTel", StringType(), True),
    StructField("cmAir", StringType(), True),
    StructField("cmRail", StringType(), True),
    StructField("loWharves", StringType(), True),
    StructField("loAnchor", StringType(), True),
    StructField("loMedMoor", StringType(), True),
    StructField("loBeachMoor", StringType(), True),
    StructField("loIceMoor", StringType(), True),
    StructField("medFacilities", StringType(), True),
    StructField("garbageDisposal", StringType(), True),
    StructField("degauss", StringType(), True),
    StructField("dirtyBallast", StringType(), True),
    StructField("crFixed", StringType(), True),
    StructField("crMobile", StringType(), True),
    StructField("crFloating", StringType(), True),
    StructField("lifts100", StringType(), True),
    StructField("lifts50", StringType(), True),
    StructField("lifts25", StringType(), True),
    StructField("lifts0", StringType(), True),
    StructField("srLongshore", StringType(), True),
    StructField("srElectrical", StringType(), True),
    StructField("srSteam", StringType(), True),
    StructField("srNavigEquip", StringType(), True),
    StructField("srElectRepair", StringType(), True),
    StructField("suProvisions", StringType(), True),
    StructField("suWater", StringType(), True),
    StructField("suFuel", StringType(), True),
    StructField("suDiesel", StringType(), True),
    StructField("suDeck", StringType(), True),
    StructField("suEngine", StringType(), True),
    StructField("repairCode", StringType(), True),
    StructField("drydock", StringType(), True),
    StructField("railway", StringType(), True),
    StructField("qtSanitation", StringType(), True),
    StructField("suAviationFuel", StringType(), True),
    StructField("harborUse", StringType(), True),
    StructField("ukcMgmtSystem", StringType(), True),
    StructField("portSecurity", StringType(), True),
    StructField("etaMessage", StringType(), True),
    StructField("searchAndRescue", StringType(), True),
    StructField("tss", StringType(), True),
    StructField("vts", StringType(), True),
    StructField("cht", StringType(), True),
    StructField("globalId", StringType(), True),
    StructField("loRoro", StringType(), True),
    StructField("loSolidBulk", StringType(), True),
    StructField("loContainer", StringType(), True),
    StructField("loBreakBulk", StringType(), True),
    StructField("loOilTerm", StringType(), True),
    StructField("loLongTerm", StringType(), True),
    StructField("loOther", StringType(), True),
    StructField("loDangCargo", StringType(), True),
    StructField("loLiquidBulk", StringType(), True),
    StructField("srIceBreaking", StringType(), True),
    StructField("srDiving", StringType(), True),
    StructField("cranesContainer", StringType(), True),
    StructField("unloCode", StringType(), True),
    StructField("dnc", StringType(), True),
    StructField("s121WaterBody", StringType(), True),
    StructField("s57Enc", StringType(), True),
    StructField("s101Enc", StringType(), True),
    StructField("dodWaterBody", StringType(), True),
    StructField("alternateName", StringType(), True),
    StructField("entranceWidth", StringType(), True),
    StructField("lngTerminalDepth", StringType(), True),
    StructField("offMaxVesselLength", StringType(), True),
    StructField("offMaxVesselBeam", StringType(), True),
    StructField("offMaxVesselDraft", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# COMMAND ----------
with PipelineLogger(spark, layer="Raw-to-Bronze (WPI)", parameter=batch_id) as logger:
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(WPI_BRONZE_SCHEMA)
        .load(source_path)
    )

    corrupt_df = raw_df.filter(col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_df.count()

    if corrupt_count > 0:
        quarantine_records = (
            corrupt_df.select(
                expr("uuid()").alias("quarantine_id"),
                lit("wpi").alias("source"),
                lit(batch_id).alias("batch_id"),
                col("_corrupt_record").alias("raw_payload"),
                lit("WPI CSV malformed line").alias("error_reason"),
                current_timestamp().alias("quarantine_timestamp"),
            )
        )
        quarantine_records.write.format("delta").mode("append").saveAsTable("bronze.quarantine")
        print(f"[WARN] Quarantined {corrupt_count} records to bronze.quarantine")

    valid_df = (
        raw_df.filter(col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        .withColumn("source", lit("wpi"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
        .withColumn("load_timestamp", current_timestamp())
    )

    (
        valid_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("bronze.raw_wpi_ports")
    )

    valid_count = valid_df.count()
    logger.set_metrics(rows_inserted=valid_count, rows_updated=0)
    print(f"[SUCCESS] Ingested {valid_count} ports into bronze.raw_wpi_ports")
