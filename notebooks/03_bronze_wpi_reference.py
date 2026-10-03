try:
    import os, sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass
from audit_logger import PipelineLogger

from pyspark.sql.functions import current_timestamp, lit, col, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

try:
    dbutils
except NameError:
    dbutils = None

if dbutils is not None:
    try:
        dbutils.widgets.text("source_path", "/Volumes/workspace/bronze/raw_data/WPI.csv", "Source WPI Path")
        dbutils.widgets.text("batch_id", "wpi-reference-v1", "Batch Identifier")
        source_path = dbutils.widgets.get("source_path")
        batch_id = dbutils.widgets.get("batch_id")
    except Exception:
        source_path = "/Volumes/workspace/bronze/raw_data/WPI.csv"
        batch_id = "wpi-reference-v1"
else:
    source_path = "/Volumes/workspace/bronze/raw_data/WPI.csv"
    batch_id = "wpi-reference-v1"

schema = StructType([
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
    StructField("srServices", StringType(), True),
    StructField("srProvisions", StringType(), True),
    StructField("srWater", StringType(), True),
    StructField("srFuel", StringType(), True),
    StructField("srDiesel", StringType(), True),
    StructField("srDeck", StringType(), True),
    StructField("srEngine", StringType(), True),
    StructField("repairCode", StringType(), True),
    StructField("drydock", StringType(), True),
    StructField("railway", StringType(), True),
    StructField("loEta", StringType(), True),
    StructField("loCable", StringType(), True),
    StructField("loIce", StringType(), True),
    StructField("loRollOn", StringType(), True),
    StructField("loContainer", StringType(), True),
    StructField("loBulk", StringType(), True),
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

with PipelineLogger(spark, layer="Raw-to-Bronze (WPI)", parameter=batch_id) as logger:
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
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
                lit("Malformed WPI CSV record").alias("error_reason"),
                current_timestamp().alias("quarantine_timestamp"),
            )
        )
        quarantine_records.write.format("delta").mode("append").saveAsTable("bronze.quarantine")
        print(f"Quarantined {corrupt_count} corrupt records")

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
    print(f"Ingested {valid_count} records into bronze.raw_wpi_ports")
