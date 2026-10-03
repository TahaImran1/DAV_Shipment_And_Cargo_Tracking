try:
    import os, sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass
from audit_logger import PipelineLogger

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

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

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

with PipelineLogger(spark, layer="Bronze-to-Silver (dim_voyage)", parameter="derive-voyages") as logger:
    if not spark.catalog.tableExists("silver.fact_vessel_position"):
        print("Table silver.fact_vessel_position does not exist yet.")
        logger.set_metrics(rows_inserted=0, rows_updated=0)
    else:
        positions_df = spark.table("silver.fact_vessel_position")
        
        vessel_window = Window.partitionBy("mmsi").orderBy("timestamp")

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
        print(f"MERGE completed on silver.dim_voyage: {inserted} inserted, {updated} updated")
