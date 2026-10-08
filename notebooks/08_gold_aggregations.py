import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from audit_logger import PipelineLogger

PORT_RADIUS_KM = 15
MAX_GAP_HOURS = 2          # ignore gaps between pings longer than this
MOVING_SOG = 0.5           # knots
DESIGN_SPEED_KNOTS = 14
CO2_PER_TONNE_FUEL = 3.114


def haversine_km(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * 6371 * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def save(df, table):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    return spark.table(table).count()


with PipelineLogger(spark, layer="gold", parameter="aggregations") as logger:
    pos = spark.table("workspace.silver.fact_vessel_position")

    # ---- port congestion ----
    ports = spark.table("workspace.silver.dim_port").filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).select("port_code", "port_name", F.col("latitude").alias("p_lat"), F.col("longitude").alias("p_lon"))

    # one point per slow-moving vessel per day keeps the port join small
    stationary = (
        pos.filter(F.col("sog") < 1)
        .withColumn("date", F.to_date("timestamp"))
        .groupBy("mmsi", "date")
        .agg(F.avg("latitude").alias("lat"), F.avg("longitude").alias("lon"))
    )

    near_box = (F.abs(F.col("lat") - F.col("p_lat")) < 0.5) & (F.abs(F.col("lon") - F.col("p_lon")) < 0.5)

    congestion = (
        stationary.join(F.broadcast(ports), near_box)
        .withColumn("dist_km", haversine_km(F.col("lat"), F.col("lon"), F.col("p_lat"), F.col("p_lon")))
        .filter(F.col("dist_km") <= PORT_RADIUS_KM)
        .withColumn("rn", F.row_number().over(Window.partitionBy("mmsi", "date").orderBy("dist_km")))
        .filter("rn = 1")
        .groupBy("port_code", "port_name", "date")
        .agg(F.count("*").alias("vessels_in_port"))
    )

    # ---- per-leg metrics (consecutive pings of the same vessel) ----
    w = Window.partitionBy("mmsi").orderBy("timestamp")
    legs = (
        pos.withColumn("prev_lat", F.lag("latitude").over(w))
        .withColumn("prev_lon", F.lag("longitude").over(w))
        .withColumn("prev_ts", F.lag("timestamp").over(w))
        .withColumn("hours", (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_ts")) / 3600)
        .filter((F.col("hours") > 0) & (F.col("hours") <= MAX_GAP_HOURS))
        .withColumn("distance_km", haversine_km(F.col("prev_lat"), F.col("prev_lon"), F.col("latitude"), F.col("longitude")))
        .withColumn("date", F.to_date("timestamp"))
    )

    moving_hours = F.sum(F.when(F.col("sog") >= MOVING_SOG, F.col("hours")).otherwise(0))

    activity = legs.groupBy("mmsi", "date").agg(
        F.round(F.sum("distance_km"), 2).alias("total_distance_km"),
        F.round(F.avg("sog"), 2).alias("avg_speed_knots"),
        F.round(moving_hours, 2).alias("operating_hours"),
    )

    # ---- emissions proxy: rough fuel burn using the cube law on speed ----
    vessels = spark.table("workspace.silver.dim_vessel").select("mmsi", "vessel_type", "length")

    vt = F.col("vessel_type")
    type_factor = (
        F.when(vt.between(80, 89), 1.4)    # tankers
        .when(vt.between(70, 79), 1.2)     # cargo
        .when(vt.between(60, 69), 1.5)     # passenger
        .when(vt.between(30, 39), 0.3)     # fishing / towing / etc.
        .otherwise(0.6)
    )
    length_m = F.coalesce(F.col("length"), F.lit(100.0))
    tonnes_per_hour = type_factor * (length_m / 100) ** 2
    speed_ratio = F.least(F.col("sog"), F.lit(30.0)) / DESIGN_SPEED_KNOTS
    fuel_tonnes = tonnes_per_hour * speed_ratio ** 3 * F.col("hours")

    emissions = (
        legs.join(vessels, "mmsi", "left")
        .withColumn("fuel_tonnes", fuel_tonnes)
        .groupBy("mmsi", "vessel_type", "date")
        .agg(
            F.round(F.sum("fuel_tonnes"), 3).alias("fuel_burn_proxy_tonnes"),
            F.round(moving_hours, 2).alias("operating_hours"),
        )
        .withColumn("co2_proxy_tonnes", F.round(F.col("fuel_burn_proxy_tonnes") * CO2_PER_TONNE_FUEL, 3))
    )

    c_count = save(congestion, "workspace.gold.port_congestion_daily")
    a_count = save(activity, "workspace.gold.daily_vessel_activity")
    e_count = save(emissions, "workspace.gold.vessel_emissions_proxy")
    total = c_count + a_count + e_count

    logger.set_metrics(rows_inserted=total)
    print("Gold tables refreshed:")
    print(f"  - port_congestion_daily: {c_count} rows")
    print(f"  - daily_vessel_activity: {a_count} rows")
    print(f"  - vessel_emissions_proxy: {e_count} rows")
