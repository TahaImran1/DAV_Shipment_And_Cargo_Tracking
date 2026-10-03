import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from audit_logger import PipelineLogger

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    to_date,
    date_format,
    count as spark_count,
    countDistinct,
    avg as spark_avg,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    current_timestamp,
    concat_ws,
    lit,
    coalesce,
    when,
)

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.gold_vessel_activity (
        mmsi BIGINT NOT NULL,
        vessel_name STRING,
        vessel_type INT,
        activity_date DATE NOT NULL,
        total_pings BIGINT,
        avg_speed_knots DOUBLE,
        max_speed_knots DOUBLE,
        estimated_distance_nm DOUBLE,
        load_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    PARTITIONED BY (activity_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.gold_port_performance (
        port_code STRING NOT NULL,
        port_name STRING,
        country STRING,
        activity_date DATE NOT NULL,
        active_vessels BIGINT,
        total_observations BIGINT,
        avg_vessel_speed DOUBLE,
        load_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    PARTITIONED BY (activity_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.gold_route_performance (
        departure_port STRING NOT NULL,
        arrival_port STRING NOT NULL,
        year_month STRING NOT NULL,
        completed_voyages BIGINT,
        avg_transit_hours DOUBLE,
        avg_speed_knots DOUBLE,
        load_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.gold_daily_maritime_activity (
        activity_date DATE NOT NULL,
        total_position_reports BIGINT,
        unique_active_vessels BIGINT,
        avg_fleet_speed DOUBLE,
        load_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
""")

with PipelineLogger(spark, layer="Silver-to-Gold (Aggregations)", parameter="all-gold-tables") as logger:
    if not spark.catalog.tableExists("silver.fact_vessel_position"):
        print("Table silver.fact_vessel_position does not exist yet.")
        logger.set_metrics(rows_inserted=0, rows_updated=0)
    else:
        facts = spark.table("silver.fact_vessel_position")
        vessels = spark.table("silver.dim_vessel") if spark.catalog.tableExists("silver.dim_vessel") else None
        ports = spark.table("silver.dim_port") if spark.catalog.tableExists("silver.dim_port") else None
        voyages = spark.table("silver.dim_voyage") if spark.catalog.tableExists("silver.dim_voyage") else None

        total_inserted = 0
        total_updated = 0

        vessel_daily = (
            facts
            .withColumn("activity_date", to_date(col("timestamp")))
            .groupBy("mmsi", "activity_date")
            .agg(
                spark_count("position_id").alias("total_pings"),
                spark_round(spark_avg("sog"), 2).alias("avg_speed_knots"),
                spark_round(spark_max("sog"), 2).alias("max_speed_knots"),
                spark_round(spark_avg("sog") * (spark_count("position_id") / 60.0), 2).alias("estimated_distance_nm"),
            )
        )

        if vessels is not None:
            vessel_daily = vessel_daily.join(vessels.select("mmsi", "vessel_name", "vessel_type"), on="mmsi", how="left")
        else:
            vessel_daily = vessel_daily.withColumn("vessel_name", lit("UNKNOWN")).withColumn("vessel_type", lit(0))

        vessel_daily = vessel_daily.withColumn("load_timestamp", current_timestamp())

        t1 = DeltaTable.forName(spark, "gold.gold_vessel_activity")
        t1.alias("target").merge(
            vessel_daily.alias("source"),
            "target.mmsi = source.mmsi AND target.activity_date = source.activity_date"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        h1 = t1.history(1).select("operationMetrics").collect()[0][0]
        total_inserted += int(h1.get("numTargetRowsInserted", 0))
        total_updated += int(h1.get("numTargetRowsUpdated", 0))

        port_activity = (
            facts
            .withColumn("activity_date", to_date(col("timestamp")))
            .withColumn(
                "port_code",
                when(col("latitude") >= 29.35, lit("US HOU")).otherwise(lit("US GLS"))
            )
            .groupBy("port_code", "activity_date")
            .agg(
                countDistinct("mmsi").alias("active_vessels"),
                spark_count("position_id").alias("total_observations"),
                spark_round(spark_avg("sog"), 2).alias("avg_vessel_speed"),
            )
        )

        if ports is not None:
            port_activity = port_activity.join(ports.select("port_code", "port_name", "country"), on="port_code", how="left")
        else:
            port_activity = port_activity.withColumn("port_name", col("port_code")).withColumn("country", lit("USA"))

        port_activity = port_activity.withColumn("load_timestamp", current_timestamp())

        t2 = DeltaTable.forName(spark, "gold.gold_port_performance")
        t2.alias("target").merge(
            port_activity.alias("source"),
            "target.port_code = source.port_code AND target.activity_date = source.activity_date"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        h2 = t2.history(1).select("operationMetrics").collect()[0][0]
        total_inserted += int(h2.get("numTargetRowsInserted", 0))
        total_updated += int(h2.get("numTargetRowsUpdated", 0))

        if voyages is not None:
            route_summary = (
                voyages
                .withColumn("year_month", date_format(col("start_ts"), "yyyy-MM"))
                .groupBy("departure_port", "arrival_port", "year_month")
                .agg(
                    spark_count("voyage_id").alias("completed_voyages"),
                    spark_round(spark_avg((col("end_ts").cast("bigint") - col("start_ts").cast("bigint")) / 3600.0), 2).alias("avg_transit_hours"),
                    spark_round(spark_avg("avg_sog"), 2).alias("avg_speed_knots"),
                )
                .withColumn("load_timestamp", current_timestamp())
            )

            t3 = DeltaTable.forName(spark, "gold.gold_route_performance")
            t3.alias("target").merge(
                route_summary.alias("source"),
                "target.departure_port = source.departure_port AND target.arrival_port = source.arrival_port AND target.year_month = source.year_month"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            h3 = t3.history(1).select("operationMetrics").collect()[0][0]
            total_inserted += int(h3.get("numTargetRowsInserted", 0))
            total_updated += int(h3.get("numTargetRowsUpdated", 0))

        daily_traffic = (
            facts
            .withColumn("activity_date", to_date(col("timestamp")))
            .groupBy("activity_date")
            .agg(
                spark_count("position_id").alias("total_position_reports"),
                countDistinct("mmsi").alias("unique_active_vessels"),
                spark_round(spark_avg("sog"), 2).alias("avg_fleet_speed"),
            )
            .withColumn("load_timestamp", current_timestamp())
        )

        t4 = DeltaTable.forName(spark, "gold.gold_daily_maritime_activity")
        t4.alias("target").merge(
            daily_traffic.alias("source"),
            "target.activity_date = source.activity_date"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        h4 = t4.history(1).select("operationMetrics").collect()[0][0]
        total_inserted += int(h4.get("numTargetRowsInserted", 0))
        total_updated += int(h4.get("numTargetRowsUpdated", 0))

        logger.set_metrics(rows_inserted=total_inserted, rows_updated=total_updated)
        print(f"Gold Aggregations completed: {total_inserted} inserted, {total_updated} updated across all tables")
