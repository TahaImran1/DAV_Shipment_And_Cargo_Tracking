import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from audit_logger import PipelineLogger

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    lit,
    coalesce,
    trim,
    upper,
    when,
    current_timestamp,
    row_number,
    concat_ws,
    lpad,
)
from pyspark.sql.window import Window

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.dim_vessel (
        mmsi BIGINT NOT NULL,
        imo BIGINT,
        vessel_name STRING,
        call_sign STRING,
        vessel_type INT,
        length DOUBLE,
        width DOUBLE,
        draught DOUBLE,
        destination STRING,
        eta STRING,
        load_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

with PipelineLogger(spark, layer="Bronze-to-Silver (dim_vessel)", parameter="merge-all-sources") as logger:
    noaa_vessels_df = None
    if spark.catalog.tableExists("bronze.raw_noaa_ais"):
        noaa_vessels_df = (
            spark.table("bronze.raw_noaa_ais")
            .filter(col("MMSI").isNotNull() & (col("MMSI") != "") & (col("MMSI") != "0"))
            .select(
                col("MMSI").cast("bigint").alias("mmsi"),
                when(col("IMO").isNotNull() & (col("IMO") != "") & (col("IMO") != "0"), col("IMO").cast("bigint")).otherwise(None).alias("imo"),
                trim(upper(col("VesselName"))).alias("vessel_name"),
                trim(upper(col("CallSign"))).alias("call_sign"),
                col("VesselType").cast("int").alias("vessel_type"),
                col("Length").cast("double").alias("length"),
                col("Width").cast("double").alias("width"),
                col("Draft").cast("double").alias("draught"),
                lit(None).cast("string").alias("destination"),
                lit(None).cast("string").alias("eta"),
                col("load_timestamp"),
            )
        )

    ais_vessels_df = None
    if spark.catalog.tableExists("bronze.raw_ais_messages"):
        ais_table = spark.table("bronze.raw_ais_messages")
        
        static_df = (
            ais_table
            .filter(col("Message.ShipStaticData").isNotNull())
            .select(
                coalesce(col("Message.ShipStaticData.UserID"), col("MetaData.MMSI")).cast("bigint").alias("mmsi"),
                when(col("Message.ShipStaticData.ImoNumber") > 0, col("Message.ShipStaticData.ImoNumber")).otherwise(None).alias("imo"),
                trim(upper(coalesce(col("Message.ShipStaticData.Name"), col("MetaData.ShipName")))).alias("vessel_name"),
                trim(upper(col("Message.ShipStaticData.CallSign"))).alias("call_sign"),
                col("Message.ShipStaticData.Type").cast("int").alias("vessel_type"),
                (col("Message.ShipStaticData.Dimension.A") + col("Message.ShipStaticData.Dimension.B")).cast("double").alias("length"),
                (col("Message.ShipStaticData.Dimension.C") + col("Message.ShipStaticData.Dimension.D")).cast("double").alias("width"),
                col("Message.ShipStaticData.MaximumStaticDraught").cast("double").alias("draught"),
                trim(upper(col("Message.ShipStaticData.Destination"))).alias("destination"),
                concat_ws("-",
                    lpad(col("Message.ShipStaticData.Eta.Month").cast("string"), 2, "0"),
                    lpad(col("Message.ShipStaticData.Eta.Day").cast("string"), 2, "0"),
                    concat_ws(":",
                        lpad(col("Message.ShipStaticData.Eta.Hour").cast("string"), 2, "0"),
                        lpad(col("Message.ShipStaticData.Eta.Minute").cast("string"), 2, "0")
                    )
                ).alias("eta"),
                col("load_timestamp"),
            )
            .filter(col("mmsi").isNotNull() & (col("mmsi") > 0))
        )

        meta_df = (
            ais_table
            .filter(col("MetaData.MMSI").isNotNull() & (col("MetaData.MMSI") > 0))
            .select(
                col("MetaData.MMSI").cast("bigint").alias("mmsi"),
                lit(None).cast("bigint").alias("imo"),
                trim(upper(col("MetaData.ShipName"))).alias("vessel_name"),
                lit(None).cast("string").alias("call_sign"),
                lit(None).cast("int").alias("vessel_type"),
                lit(None).cast("double").alias("length"),
                lit(None).cast("double").alias("width"),
                lit(None).cast("double").alias("draught"),
                lit(None).cast("string").alias("destination"),
                lit(None).cast("string").alias("eta"),
                col("load_timestamp"),
            )
        )
        
        ais_vessels_df = static_df.unionByName(meta_df)

    if noaa_vessels_df is not None and ais_vessels_df is not None:
        combined_df = noaa_vessels_df.unionByName(ais_vessels_df)
    elif noaa_vessels_df is not None:
        combined_df = noaa_vessels_df
    elif ais_vessels_df is not None:
        combined_df = ais_vessels_df
    else:
        combined_df = spark.createDataFrame([], schema=spark.table("silver.dim_vessel").schema)

    window_spec = Window.partitionBy("mmsi").orderBy(col("load_timestamp").desc())
    deduped_vessels = (
        combined_df
        .filter(col("mmsi").isNotNull())
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("load_timestamp", current_timestamp())
    )

    target_table = DeltaTable.forName(spark, "silver.dim_vessel")

    target_table.alias("target").merge(
        deduped_vessels.alias("source"),
        "target.mmsi = source.mmsi"
    ).whenMatchedUpdate(
        condition="""
            target.vessel_name <=> source.vessel_name = false OR
            target.destination <=> source.destination = false OR
            target.draught <=> source.draught = false OR
            target.eta <=> source.eta = false OR
            target.imo IS NULL AND source.imo IS NOT NULL
        """,
        set={
            "imo": coalesce(col("source.imo"), col("target.imo")),
            "vessel_name": coalesce(col("source.vessel_name"), col("target.vessel_name")),
            "call_sign": coalesce(col("source.call_sign"), col("target.call_sign")),
            "vessel_type": coalesce(col("source.vessel_type"), col("target.vessel_type")),
            "length": coalesce(col("source.length"), col("target.length")),
            "width": coalesce(col("source.width"), col("target.width")),
            "draught": coalesce(col("source.draught"), col("target.draught")),
            "destination": coalesce(col("source.destination"), col("target.destination")),
            "eta": coalesce(col("source.eta"), col("target.eta")),
            "load_timestamp": col("source.load_timestamp"),
        }
    ).whenNotMatchedInsertAll().execute()

    history = target_table.history(1).select("operationMetrics").collect()[0][0]
    inserted = int(history.get("numTargetRowsInserted", 0))
    updated = int(history.get("numTargetRowsUpdated", 0))

    logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
    print(f"MERGE completed on silver.dim_vessel: {inserted} inserted, {updated} updated")
