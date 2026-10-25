import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql.functions import (
    col,
    lit,
    coalesce,
    lag,
    when,
    count as _count,
    sum as _sum,
    avg as _avg,
    max as _max,
    round as _round,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    least,
    abs as _abs,
    broadcast,
    row_number,
    to_date,
    date_format,
    year,
    quarter,
    month,
    dayofmonth,
    dayofweek,
    sequence,
    explode,
    expr,
    unix_timestamp,
    current_timestamp,
)
from pyspark.sql.window import Window
from audit_logger import PipelineLogger

PORT_RADIUS_KM = 15
MAX_GAP_HOURS = 2
MOVING_SOG = 0.5
DESIGN_SPEED_KNOTS = 14
CO2_PER_TONNE_FUEL = 3.114


def haversine_km(lat1, lon1, lat2, lon2):
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * 6371 * asin(sqrt(least(lit(1.0), a)))


def save_table(df, table_name):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    return spark.table(table_name).count()


with PipelineLogger(spark, layer="gold", parameter="dimensional_modeling") as logger:
    date_range = spark.sql("SELECT sequence(to_date('2024-01-01'), to_date('2026-12-31'), interval 1 day) as dates")
    dim_date = (
        date_range.select(explode("dates").alias("full_date"))
        .withColumn("date_key", date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", year("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("month_name", date_format("full_date", "MMMM"))
        .withColumn("day", dayofmonth("full_date"))
        .withColumn("day_of_week", dayofweek("full_date"))
        .withColumn("day_name", date_format("full_date", "EEEE"))
        .withColumn("is_weekend", col("day_of_week").isin([1, 7]))
        .withColumn("load_timestamp", current_timestamp())
    )

    silver_vessels = spark.table("workspace.silver.dim_vessel")
    vt = col("vessel_type")
    vessel_category_expr = (
        when(vt.between(80, 89), lit("Tanker"))
        .when(vt.between(70, 79), lit("Cargo"))
        .when(vt.between(60, 69), lit("Passenger"))
        .when(vt.between(30, 39), lit("Fishing / Towing"))
        .when(vt.between(40, 49), lit("High Speed Craft"))
        .when(vt.between(50, 59), lit("Special Craft"))
        .otherwise(lit("Other / Unspecified"))
    )
    size_category_expr = (
        when(col("length") >= 300, lit("Ultra Large (>=300m)"))
        .when(col("length").between(200, 299.99), lit("Large (200-299m)"))
        .when(col("length").between(100, 199.99), lit("Medium (100-199m)"))
        .when(col("length") < 100, lit("Small (<100m)"))
        .otherwise(lit("Unspecified"))
    )
    efficiency_factor_expr = (
        when(vt.between(80, 89), lit(1.4))
        .when(vt.between(70, 79), lit(1.2))
        .when(vt.between(60, 69), lit(1.5))
        .when(vt.between(30, 39), lit(0.3))
        .otherwise(lit(0.6))
    )

    dim_vessel = (
        silver_vessels.select(
            col("mmsi"),
            col("vessel_name"),
            col("imo"),
            col("call_sign"),
            col("vessel_type").alias("vessel_type_code"),
            vessel_category_expr.alias("vessel_category"),
            col("length"),
            col("width"),
            col("draft"),
            size_category_expr.alias("size_category"),
            efficiency_factor_expr.alias("efficiency_factor"),
            current_timestamp().alias("load_timestamp"),
        )
    )

    silver_ports = spark.table("workspace.silver.dim_port")
    harbor_size_expr = (
        when(col("harbor_size") == "L", lit("Large Harbor"))
        .when(col("harbor_size") == "M", lit("Medium Harbor"))
        .when(col("harbor_size") == "S", lit("Small Harbor"))
        .when(col("harbor_size") == "V", lit("Very Small Harbor"))
        .otherwise(lit("Unspecified / Coastal Facility"))
    )

    dim_port = (
        silver_ports.select(
            col("port_code"),
            col("port_number"),
            col("port_name"),
            col("country"),
            col("country_code"),
            col("latitude"),
            col("longitude"),
            col("harbor_size"),
            col("harbor_type"),
            harbor_size_expr.alias("harbor_size_desc"),
            current_timestamp().alias("load_timestamp"),
        )
    )

    pos = spark.table("workspace.silver.fact_vessel_position")

    ports_ref = dim_port.filter(
        col("latitude").isNotNull() & col("longitude").isNotNull()
    ).select(
        col("port_code"),
        col("port_name"),
        col("latitude").alias("p_lat"),
        col("longitude").alias("p_lon")
    )

    stationary = (
        pos.filter(col("sog") < 1.0)
        .withColumn("date", to_date("timestamp"))
        .groupBy("mmsi", "date")
        .agg(
            _avg("latitude").alias("lat"),
            _avg("longitude").alias("lon"),
            _round(_avg("sog"), 2).alias("avg_sog")
        )
    )

    near_box = (_abs(col("lat") - col("p_lat")) < 0.5) & (_abs(col("lon") - col("p_lon")) < 0.5)

    congestion = (
        stationary.join(broadcast(ports_ref), near_box)
        .withColumn("dist_km", haversine_km(col("lat"), col("lon"), col("p_lat"), col("p_lon")))
        .filter(col("dist_km") <= PORT_RADIUS_KM)
        .withColumn("rn", row_number().over(Window.partitionBy("mmsi", "date").orderBy("dist_km")))
        .filter("rn = 1")
        .groupBy("port_code", "port_name", "date")
        .agg(
            _count("*").alias("vessels_in_port"),
            _round(_avg("avg_sog"), 2).alias("avg_stationary_sog")
        )
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
        .withColumnRenamed("date", "congestion_date")
        .withColumn("load_timestamp", current_timestamp())
    )

    w = Window.partitionBy("mmsi").orderBy("timestamp")
    legs = (
        pos.withColumn("prev_lat", lag("latitude").over(w))
        .withColumn("prev_lon", lag("longitude").over(w))
        .withColumn("prev_ts", lag("timestamp").over(w))
        .withColumn("hours", (unix_timestamp("timestamp") - unix_timestamp("prev_ts")) / 3600.0)
        .filter((col("hours") > 0.0) & (col("hours") <= MAX_GAP_HOURS))
        .withColumn("distance_km", haversine_km(col("prev_lat"), col("prev_lon"), col("latitude"), col("longitude")))
        .withColumn("date", to_date("timestamp"))
    )

    vessels_spec = dim_vessel.select("mmsi", "vessel_type_code", "length", "efficiency_factor")
    length_m = coalesce(col("length"), lit(100.0))
    tonnes_per_hour = col("efficiency_factor") * (length_m / 100.0) ** 2
    speed_ratio = least(col("sog"), lit(30.0)) / DESIGN_SPEED_KNOTS
    fuel_tonnes = tonnes_per_hour * (speed_ratio ** 3) * col("hours")

    legs_enriched = (
        legs.join(broadcast(vessels_spec), "mmsi", "left")
        .withColumn("fuel_tonnes", fuel_tonnes)
        .withColumn("moving_hour", when(col("sog") >= MOVING_SOG, col("hours")).otherwise(0.0))
    )

    fact_activity = (
        legs_enriched.groupBy("mmsi", "vessel_type_code", "date")
        .agg(
            _round(_sum("distance_km"), 2).alias("total_distance_km"),
            _round(_avg("sog"), 2).alias("avg_speed_knots"),
            _round(_max("sog"), 2).alias("max_speed_knots"),
            _round(_sum("moving_hour"), 2).alias("operating_hours"),
            _count("*").alias("underway_pings_count"),
            _round(_sum("fuel_tonnes"), 3).alias("fuel_burn_proxy_tonnes")
        )
        .withColumn("co2_proxy_tonnes", _round(col("fuel_burn_proxy_tonnes") * CO2_PER_TONNE_FUEL, 3))
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
        .withColumnRenamed("date", "activity_date")
        .withColumnRenamed("vessel_type_code", "vessel_type")
        .withColumn("load_timestamp", current_timestamp())
    )

    silver_voyages = spark.table("workspace.silver.dim_voyage")
    fact_voyages = (
        silver_voyages.select(
            col("voyage_id"),
            col("mmsi"),
            col("voyage_seq"),
            date_format("departure_time", "yyyyMMdd").cast("int").alias("departure_date_key"),
            col("departure_time"),
            date_format("arrival_time", "yyyyMMdd").cast("int").alias("arrival_date_key"),
            col("arrival_time"),
            _round((unix_timestamp("arrival_time") - unix_timestamp("departure_time")) / 3600.0, 2).alias("duration_hours"),
            col("arrival_port_code"),
            col("position_count"),
            lit(0.0).alias("transit_distance_km"),
            lit(0.0).alias("avg_speed_knots"),
            current_timestamp().alias("load_timestamp")
        )
    )

    audit_logs = spark.table("workspace.maritime_ops.pipeline_execution_logs")
    fact_pipeline = (
        audit_logs.select(
            col("log_id"),
            col("layer"),
            col("parameter"),
            date_format("start_time", "yyyyMMdd").cast("int").alias("start_date_key"),
            col("start_time"),
            col("end_time"),
            _round((unix_timestamp("end_time") - unix_timestamp("start_time")), 2).alias("duration_seconds"),
            col("status"),
            col("rows_inserted"),
            col("rows_updated"),
            col("error_message"),
            current_timestamp().alias("load_timestamp")
        )
    )

    compat_congestion = congestion.select(
        col("port_code"),
        col("port_name"),
        col("congestion_date").alias("date"),
        col("vessels_in_port")
    )
    compat_activity = fact_activity.select(
        col("mmsi"),
        col("activity_date").alias("date"),
        col("total_distance_km"),
        col("avg_speed_knots"),
        col("operating_hours")
    )
    compat_emissions = fact_activity.select(
        col("mmsi"),
        col("vessel_type"),
        col("activity_date").alias("date"),
        col("fuel_burn_proxy_tonnes"),
        col("operating_hours"),
        col("co2_proxy_tonnes")
    )

    cnt_date = save_table(dim_date, "workspace.gold.dim_date")
    cnt_vessel = save_table(dim_vessel, "workspace.gold.dim_vessel")
    cnt_port = save_table(dim_port, "workspace.gold.dim_port")
    cnt_act = save_table(fact_activity, "workspace.gold.fact_daily_vessel_activity")
    cnt_cong = save_table(congestion, "workspace.gold.fact_port_congestion_daily")
    cnt_voy = save_table(fact_voyages, "workspace.gold.fact_voyage_performance")
    cnt_ops = save_table(fact_pipeline, "workspace.gold.fact_pipeline_performance")

    save_table(compat_congestion, "workspace.gold.port_congestion_daily")
    save_table(compat_activity, "workspace.gold.daily_vessel_activity")
    save_table(compat_emissions, "workspace.gold.vessel_emissions_proxy")

    try:
        spark.sql("OPTIMIZE workspace.gold.fact_daily_vessel_activity ZORDER BY (mmsi, date_key)")
        spark.sql("OPTIMIZE workspace.gold.fact_port_congestion_daily ZORDER BY (port_code, date_key)")
        spark.sql("OPTIMIZE workspace.gold.fact_voyage_performance ZORDER BY (mmsi, arrival_port_code)")
        spark.sql("OPTIMIZE workspace.gold.dim_vessel ZORDER BY (mmsi)")
        spark.sql("OPTIMIZE workspace.gold.dim_port ZORDER BY (port_code)")
        spark.sql("OPTIMIZE workspace.gold.dim_date ZORDER BY (date_key)")
    except Exception as e:
        print(f"Optimize status: {e}")

    total_gold_rows = cnt_date + cnt_vessel + cnt_port + cnt_act + cnt_cong + cnt_voy + cnt_ops
    logger.set_metrics(rows_inserted=total_gold_rows)

    print(f"Gold star schema built successfully with {total_gold_rows} total rows.")
    print(f"  - dim_date: {cnt_date} rows")
    print(f"  - dim_vessel: {cnt_vessel} rows")
    print(f"  - dim_port: {cnt_port} rows")
    print(f"  - fact_daily_vessel_activity: {cnt_act} rows")
    print(f"  - fact_port_congestion_daily: {cnt_cong} rows")
    print(f"  - fact_voyage_performance: {cnt_voy} rows")
    print(f"  - fact_pipeline_performance: {cnt_ops} rows")

    spark.sql("""
        SELECT 'dim_date' AS table_name, COUNT(*) AS total_records FROM workspace.gold.dim_date
        UNION ALL
        SELECT 'dim_vessel' AS table_name, COUNT(*) AS total_records FROM workspace.gold.dim_vessel
        UNION ALL
        SELECT 'dim_port' AS table_name, COUNT(*) AS total_records FROM workspace.gold.dim_port
        UNION ALL
        SELECT 'fact_daily_vessel_activity' AS table_name, COUNT(*) AS total_records FROM workspace.gold.fact_daily_vessel_activity
        UNION ALL
        SELECT 'fact_port_congestion_daily' AS table_name, COUNT(*) AS total_records FROM workspace.gold.fact_port_congestion_daily
        UNION ALL
        SELECT 'fact_voyage_performance' AS table_name, COUNT(*) AS total_records FROM workspace.gold.fact_voyage_performance
        UNION ALL
        SELECT 'fact_pipeline_performance' AS table_name, COUNT(*) AS total_records FROM workspace.gold.fact_pipeline_performance
    """).show()

    print("=== Spark Execution Plan & DAG Breakdown for Gold Fact Aggregation ===")
    fact_activity.explain(mode="formatted")

