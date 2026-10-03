# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./99_audit_logger

# COMMAND ----------
import re
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    udf,
    when,
    trim,
    upper,
    coalesce,
    concat,
    lit,
    current_timestamp,
)
from pyspark.sql.types import DoubleType

# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.dim_port (
        port_code STRING NOT NULL,
        port_number INT NOT NULL,
        port_name STRING NOT NULL,
        country STRING,
        country_code STRING,
        latitude DOUBLE NOT NULL,
        longitude DOUBLE NOT NULL,
        harbor_size STRING,
        harbor_type STRING,
        load_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------
@udf(returnType=DoubleType())
def dms_to_decimal(dms_str):
    if not dms_str:
        return None
    try:
        clean = dms_str.strip().replace('"', '').replace("'", "")
        m = re.match(r"(\d+)[°\s]+(\d+)?(?:[\'\s]+(\d+))?\s*([NSEWnsew])?", clean)
        if not m:
            return float(clean)
        deg = float(m.group(1))
        minute = float(m.group(2) or 0)
        sec = float(m.group(3) or 0)
        direction = (m.group(4) or 'N').upper()
        
        dec = deg + (minute / 60.0) + (sec / 3600.0)
        if direction in ['S', 'W']:
            dec = -dec
        return round(dec, 6)
    except Exception:
        return None

# COMMAND ----------
with PipelineLogger(spark, layer="Bronze-to-Silver (dim_port)", parameter="wpi-reference") as logger:
    if not spark.catalog.tableExists("bronze.raw_wpi_ports"):
        raise RuntimeError("Table bronze.raw_wpi_ports not found. Run 03_bronze_wpi_reference first.")

    raw_wpi = spark.table("bronze.raw_wpi_ports")

    cleaned_ports = (
        raw_wpi
        .filter(col("portNumber").isNotNull())
        .withColumn("lat_dec", dms_to_decimal(col("latitude")))
        .withColumn("lon_dec", dms_to_decimal(col("longitude")))
        .filter(col("lat_dec").isNotNull() & col("lon_dec").isNotNull())
        .select(
            when(col("unloCode").isNotNull() & (trim(col("unloCode")) != ""), trim(upper(col("unloCode"))))
            .otherwise(concat(lit("WPI_"), col("portNumber").cast("string")))
            .alias("port_code"),
            col("portNumber").cast("int").alias("port_number"),
            trim(upper(col("portName"))).alias("port_name"),
            trim(upper(col("countryName"))).alias("country"),
            trim(upper(col("countryCode"))).alias("country_code"),
            col("lat_dec").alias("latitude"),
            col("lon_dec").alias("longitude"),
            col("harborSize").alias("harbor_size"),
            col("harborType").alias("harbor_type"),
            current_timestamp().alias("load_timestamp"),
        )
        .dropDuplicates(["port_code"])
    )

    target_table = DeltaTable.forName(spark, "silver.dim_port")

    (
        target_table.alias("target")
        .merge(
            cleaned_ports.alias("source"),
            "target.port_code = source.port_code"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    history = target_table.history(1).select("operationMetrics").collect()[0][0]
    inserted = int(history.get("numTargetRowsInserted", 0))
    updated = int(history.get("numTargetRowsUpdated", 0))

    logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
    print(f"MERGE completed on silver.dim_port: {inserted} inserted, {updated} updated")
