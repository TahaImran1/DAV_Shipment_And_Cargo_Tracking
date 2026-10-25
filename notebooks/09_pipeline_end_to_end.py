import os
import sys
from datetime import datetime, timezone

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
    trim,
    regexp_replace,
    to_timestamp,
    substring,
    length,
    first,
    count as _count,
    sum as _sum,
    avg as _avg,
    max as _max,
    min as _min,
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
    struct,
    concat_ws,
    to_date,
    date_format,
    year,
    quarter,
    month,
    dayofmonth,
    dayofweek,
    explode,
    expr,
    unix_timestamp,
    current_timestamp,
    get_json_object,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from audit_logger import PipelineLogger

try:
    dbutils.widgets.text("source_path", "/Volumes/workspace/bronze/raw_data/ais_daily_20261003.json", "Source Path")
    dbutils.widgets.text("batch_id", datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"), "Batch ID")
    SOURCE = dbutils.widgets.get("source_path")
    batch_id = dbutils.widgets.get("batch_id")
except Exception:
    SOURCE = "/Volumes/workspace/bronze/raw_data/ais_daily_20261003.json"
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

BRONZE_TARGET = "workspace.bronze.raw_aisstream_incremental"
QUARANTINE_TARGET = "workspace.bronze.quarantine"
SILVER_VESSEL = "workspace.silver.dim_vessel"
SILVER_FACT = "workspace.silver.fact_vessel_position"
SILVER_VOYAGE = "workspace.silver.dim_voyage"
SILVER_PORT = "workspace.silver.dim_port"

PORT_RADIUS_KM = 15
MAX_GAP_HOURS = 2
MOVING_SOG = 0.5
DESIGN_SPEED_KNOTS = 14
CO2_PER_TONNE_FUEL = 3.114
STOPPED_STATUSES = [1, 5]
STOPPED_SOG = 0.5


def j(path):
    return get_json_object(col("value"), path)


def position(field):
    return coalesce(
        j(f"$.Message.PositionReport.{field}"),
        j(f"$.Message.StandardClassBPositionReport.{field}"),
    )


def static(field):
    return j(f"$.Message.ShipStaticData.{field}")


def haversine_km(lat1, lon1, lat2, lon2):
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * 6371 * asin(sqrt(least(lit(1.0), a)))


def merge_into(df, target, keys):
    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)

    target_table = DeltaTable.forName(spark, target)
    join_condition = " AND ".join(f"target.{k} = source.{k}" for k in keys)

    (
        target_table.alias("target")
        .merge(df.alias("source"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    m = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["operationMetrics"]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


def save_table(df, table_name):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    return spark.table(table_name).count()


with PipelineLogger(spark, layer="end_to_end_orchestration", parameter=batch_id) as total_logger:

    with PipelineLogger(spark, layer="bronze", parameter="incremental_step") as b_logger:
        lines = spark.read.text(SOURCE)
        parsed = lines.select(
            j("$.MessageType").alias("message_type"),
            j("$.MetaData.MMSI").cast("long").alias("mmsi"),
            j("$.MetaData.ShipName").alias("ship_name"),
            j("$.MetaData.latitude").cast("double").alias("latitude"),
            j("$.MetaData.longitude").cast("double").alias("longitude"),
            j("$.MetaData.time_utc").alias("time_utc"),
            position("Sog").cast("double").alias("sog"),
            position("Cog").cast("double").alias("cog"),
            position("TrueHeading").cast("int").alias("true_heading"),
            position("NavigationalStatus").cast("int").alias("nav_status"),
            static("ImoNumber").alias("imo"),
            static("CallSign").alias("call_sign"),
            static("Type").cast("int").alias("ship_type"),
            (static("Dimension.A").cast("double") + static("Dimension.B").cast("double")).alias("length"),
            (static("Dimension.C").cast("double") + static("Dimension.D").cast("double")).alias("width"),
            static("MaximumStaticDraught").cast("double").alias("draft"),
            col("value").alias("raw_json"),
        )

        bad = parsed.filter(col("mmsi").isNull())
        if not bad.isEmpty():
            bad.select(
                expr("uuid()").alias("quarantine_id"),
                lit("aisstream").alias("source"),
                lit(batch_id).alias("batch_id"),
                col("raw_json").alias("raw_payload"),
                lit("corrupt JSON or missing MMSI").alias("error_reason"),
                current_timestamp().alias("quarantine_timestamp"),
            ).write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(QUARANTINE_TARGET)

        good = (
            parsed.filter(col("mmsi").isNotNull())
            .withColumn("source", lit("aisstream"))
            .withColumn("batch_id", lit(batch_id))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("load_timestamp", current_timestamp())
        )

        good.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(BRONZE_TARGET)

        b_rows = spark.table(BRONZE_TARGET).filter(col("batch_id") == batch_id).count()
        b_logger.set_metrics(rows_inserted=b_rows)
        print(f"Step 1: Bronze Ingestion finished ({b_rows} records)")

    with PipelineLogger(spark, layer="silver", parameter="vessel_merge_step") as v_logger:
        noaa = spark.table("workspace.bronze.raw_noaa_ais").select(
            col("MMSI").cast("long").alias("mmsi"),
            to_timestamp("BaseDateTime").alias("ts"),
            when(trim(col("VesselName")) != "", trim(col("VesselName"))).alias("vessel_name"),
            when(trim(regexp_replace(col("IMO"), "^IMO", "")) != "", trim(regexp_replace(col("IMO"), "^IMO", ""))).alias("imo"),
            when(trim(col("CallSign")) != "", trim(col("CallSign"))).alias("call_sign"),
            col("VesselType").cast("int").alias("vessel_type"),
            when(col("Length").cast("double") > 0, col("Length").cast("double")).alias("length"),
            when(col("Width").cast("double") > 0, col("Width").cast("double")).alias("width"),
            when(col("Draft").cast("double") > 0, col("Draft").cast("double")).alias("draft"),
        )
        live = spark.table("workspace.bronze.raw_aisstream_incremental").select(
            col("mmsi"),
            to_timestamp(substring("time_utc", 1, 19)).alias("ts"),
            when(trim(col("ship_name")) != "", trim(col("ship_name"))).alias("vessel_name"),
            when(trim(col("imo")) != "", trim(col("imo"))).alias("imo"),
            when(trim(col("call_sign")) != "", trim(col("call_sign"))).alias("call_sign"),
            col("ship_type").alias("vessel_type"),
            when(col("length").cast("double") > 0, col("length").cast("double")).alias("length"),
            when(col("width").cast("double") > 0, col("width").cast("double")).alias("width"),
            when(col("draft").cast("double") > 0, col("draft").cast("double")).alias("draft"),
        )
        vessels = noaa.unionByName(live).filter(length(col("mmsi").cast("string")) == 9)
        cols_to_first = ["vessel_name", "imo", "call_sign", "vessel_type", "length", "width", "draft"]
        w_v = (
            Window.partitionBy("mmsi")
            .orderBy(col("ts").desc_nulls_last())
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        dim_v_df = (
            vessels.select("mmsi", *[first(c, ignorenulls=True).over(w_v).alias(c) for c in cols_to_first])
            .dropDuplicates(["mmsi"])
            .withColumn("load_timestamp", current_timestamp())
        )
        v_ins, v_upd = merge_into(dim_v_df, SILVER_VESSEL, ["mmsi"])
        v_logger.set_metrics(rows_inserted=v_ins, rows_updated=v_upd)
        print(f"Step 2a: Silver dim_vessel merged ({v_ins} inserted, {v_upd} updated)")

    with PipelineLogger(spark, layer="silver", parameter="position_merge_step") as p_logger:
        noaa_p = spark.table("workspace.bronze.raw_noaa_ais").select(
            col("MMSI").cast("long").alias("mmsi"),
            to_timestamp("BaseDateTime").alias("timestamp"),
            col("LAT").cast("double").alias("latitude"),
            col("LON").cast("double").alias("longitude"),
            col("SOG").cast("double").alias("sog"),
            col("COG").cast("double").alias("cog"),
            col("Heading").cast("double").alias("heading"),
            col("Status").cast("int").alias("status"),
            lit("noaa").alias("source"),
        )
        live_p = spark.table("workspace.bronze.raw_aisstream_incremental").select(
            col("mmsi"),
            to_timestamp(substring("time_utc", 1, 19)).alias("timestamp"),
            col("latitude"),
            col("longitude"),
            col("sog"),
            col("cog"),
            col("true_heading").cast("double").alias("heading"),
            col("nav_status").alias("status"),
            lit("aisstream").alias("source"),
        )
        positions = (
            noaa_p.unionByName(live_p)
            .filter(
                col("mmsi").isNotNull()
                & col("timestamp").isNotNull()
                & col("latitude").between(-90, 90)
                & col("longitude").between(-180, 180)
                & (col("sog") >= 0)
            )
            .dropDuplicates(["mmsi", "timestamp"])
            .withColumn("load_timestamp", current_timestamp())
        )
        p_ins, p_upd = merge_into(positions, SILVER_FACT, ["mmsi", "timestamp"])
        p_logger.set_metrics(rows_inserted=p_ins, rows_updated=p_upd)
        print(f"Step 2b: Silver fact_vessel_position merged ({p_ins} inserted, {p_upd} updated)")

    with PipelineLogger(spark, layer="silver", parameter="voyage_derive_step") as vy_logger:
        pos_vy = spark.table(SILVER_FACT).select("mmsi", "timestamp", "latitude", "longitude", "sog", "status")
        w_vy = Window.partitionBy("mmsi").orderBy("timestamp")
        stopped_by_status = coalesce(col("status").isin(STOPPED_STATUSES), lit(False))
        pos_vy = pos_vy.withColumn("is_stopped", stopped_by_status | (col("sog") < STOPPED_SOG))
        prev_stopped = coalesce(lag("is_stopped").over(w_vy), lit(True))
        pos_vy = pos_vy.withColumn("departed", (~col("is_stopped") & prev_stopped).cast("int"))
        pos_vy = pos_vy.withColumn("voyage_seq", _sum("departed").over(w_vy))

        voyages_grouped = (
            pos_vy.filter("voyage_seq > 0")
            .groupBy("mmsi", "voyage_seq")
            .agg(
                _min(when(~col("is_stopped"), col("timestamp"))).alias("departure_time"),
                _min(when(col("is_stopped"), struct("timestamp", "latitude", "longitude"))).alias("arrival"),
                _count("*").alias("position_count"),
            )
            .select(
                "mmsi",
                "voyage_seq",
                "departure_time",
                col("arrival.timestamp").alias("arrival_time"),
                col("arrival.latitude").alias("arrival_lat"),
                col("arrival.longitude").alias("arrival_lon"),
                "position_count",
            )
        )

        ports_lookup = spark.table(SILVER_PORT).filter(
            col("latitude").isNotNull() & col("longitude").isNotNull()
        ).select("port_code", col("latitude").alias("p_lat"), col("longitude").alias("p_lon"))

        near_box_vy = (_abs(col("arrival_lat") - col("p_lat")) < 0.5) & (_abs(col("arrival_lon") - col("p_lon")) < 0.5)

        nearest_port = (
            voyages_grouped.filter(col("arrival_time").isNotNull())
            .join(broadcast(ports_lookup), near_box_vy)
            .withColumn("dist_km", haversine_km(col("arrival_lat"), col("arrival_lon"), col("p_lat"), col("p_lon")))
            .filter(col("dist_km") <= 20.0)
            .withColumn("rn", row_number().over(Window.partitionBy("mmsi", "voyage_seq").orderBy("dist_km")))
            .filter("rn = 1")
            .select("mmsi", "voyage_seq", col("port_code").alias("arrival_port_code"))
        )

        voyage_final = (
            voyages_grouped.join(nearest_port, ["mmsi", "voyage_seq"], "left")
            .withColumn("voyage_id", concat_ws("_", "mmsi", unix_timestamp("departure_time")))
            .withColumn("load_timestamp", current_timestamp())
        )
        voyage_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_VOYAGE)
        vy_rows = spark.table(SILVER_VOYAGE).count()
        vy_logger.set_metrics(rows_inserted=vy_rows)
        print(f"Step 2c: Silver dim_voyage refreshed ({vy_rows} voyages)")

    with PipelineLogger(spark, layer="gold", parameter="star_schema_step") as g_logger:
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

        silver_vessels = spark.table(SILVER_VESSEL)
        vt_col = col("vessel_type")
        v_cat_expr = (
            when(vt_col.between(80, 89), lit("Tanker"))
            .when(vt_col.between(70, 79), lit("Cargo"))
            .when(vt_col.between(60, 69), lit("Passenger"))
            .when(vt_col.between(30, 39), lit("Fishing / Towing"))
            .when(vt_col.between(40, 49), lit("High Speed Craft"))
            .when(vt_col.between(50, 59), lit("Special Craft"))
            .otherwise(lit("Other / Unspecified"))
        )
        s_cat_expr = (
            when(col("length") >= 300, lit("Ultra Large (>=300m)"))
            .when(col("length").between(200, 299.99), lit("Large (200-299m)"))
            .when(col("length").between(100, 199.99), lit("Medium (100-199m)"))
            .when(col("length") < 100, lit("Small (<100m)"))
            .otherwise(lit("Unspecified"))
        )
        eff_expr = (
            when(vt_col.between(80, 89), lit(1.4))
            .when(vt_col.between(70, 79), lit(1.2))
            .when(vt_col.between(60, 69), lit(1.5))
            .when(vt_col.between(30, 39), lit(0.3))
            .otherwise(lit(0.6))
        )
        dim_vessel_g = silver_vessels.select(
            col("mmsi"),
            col("vessel_name"),
            col("imo"),
            col("call_sign"),
            col("vessel_type").alias("vessel_type_code"),
            v_cat_expr.alias("vessel_category"),
            col("length"),
            col("width"),
            col("draft"),
            s_cat_expr.alias("size_category"),
            eff_expr.alias("efficiency_factor"),
            current_timestamp().alias("load_timestamp"),
        )

        silver_ports = spark.table(SILVER_PORT)
        hb_expr = (
            when(col("harbor_size") == "L", lit("Large Harbor"))
            .when(col("harbor_size") == "M", lit("Medium Harbor"))
            .when(col("harbor_size") == "S", lit("Small Harbor"))
            .when(col("harbor_size") == "V", lit("Very Small Harbor"))
            .otherwise(lit("Unspecified / Coastal Facility"))
        )
        dim_port_g = silver_ports.select(
            col("port_code"),
            col("port_number"),
            col("port_name"),
            col("country"),
            col("country_code"),
            col("latitude"),
            col("longitude"),
            col("harbor_size"),
            col("harbor_type"),
            hb_expr.alias("harbor_size_desc"),
            current_timestamp().alias("load_timestamp"),
        )

        pos_g = spark.table(SILVER_FACT)
        ports_g_ref = dim_port_g.filter(
            col("latitude").isNotNull() & col("longitude").isNotNull()
        ).select(
            col("port_code"),
            col("port_name"),
            col("latitude").alias("p_lat"),
            col("longitude").alias("p_lon")
        )
        stationary_g = (
            pos_g.filter(col("sog") < 1.0)
            .withColumn("date", to_date("timestamp"))
            .groupBy("mmsi", "date")
            .agg(
                _avg("latitude").alias("lat"),
                _avg("longitude").alias("lon"),
                _round(_avg("sog"), 2).alias("avg_sog")
            )
        )
        near_box_g = (_abs(col("lat") - col("p_lat")) < 0.5) & (_abs(col("lon") - col("p_lon")) < 0.5)
        congestion_g = (
            stationary_g.join(broadcast(ports_g_ref), near_box_g)
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

        w_legs = Window.partitionBy("mmsi").orderBy("timestamp")
        legs_g = (
            pos_g.withColumn("prev_lat", lag("latitude").over(w_legs))
            .withColumn("prev_lon", lag("longitude").over(w_legs))
            .withColumn("prev_ts", lag("timestamp").over(w_legs))
            .withColumn("hours", (unix_timestamp("timestamp") - unix_timestamp("prev_ts")) / 3600.0)
            .filter((col("hours") > 0.0) & (col("hours") <= MAX_GAP_HOURS))
            .withColumn("distance_km", haversine_km(col("prev_lat"), col("prev_lon"), col("latitude"), col("longitude")))
            .withColumn("date", to_date("timestamp"))
        )

        v_spec = dim_vessel_g.select("mmsi", "vessel_type_code", "length", "efficiency_factor")
        length_val = coalesce(col("length"), lit(100.0))
        t_per_hour = col("efficiency_factor") * (length_val / 100.0) ** 2
        sp_ratio = least(col("sog"), lit(30.0)) / DESIGN_SPEED_KNOTS
        fuel_calc = t_per_hour * (sp_ratio ** 3) * col("hours")

        legs_enr = (
            legs_g.join(broadcast(v_spec), "mmsi", "left")
            .withColumn("fuel_tonnes", fuel_calc)
            .withColumn("moving_hour", when(col("sog") >= MOVING_SOG, col("hours")).otherwise(0.0))
        )

        fact_activity_g = (
            legs_enr.groupBy("mmsi", "vessel_type_code", "date")
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

        fact_voyage_g = (
            spark.table(SILVER_VOYAGE).select(
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

        fact_pipe_g = (
            spark.table("workspace.maritime_ops.pipeline_execution_logs").select(
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

        save_table(dim_date, "workspace.gold.dim_date")
        save_table(dim_vessel_g, "workspace.gold.dim_vessel")
        save_table(dim_port_g, "workspace.gold.dim_port")
        save_table(fact_activity_g, "workspace.gold.fact_daily_vessel_activity")
        save_table(congestion_g, "workspace.gold.fact_port_congestion_daily")
        save_table(fact_voyage_g, "workspace.gold.fact_voyage_performance")
        save_table(fact_pipe_g, "workspace.gold.fact_pipeline_performance")

        save_table(
            congestion_g.select(col("port_code"), col("port_name"), col("congestion_date").alias("date"), col("vessels_in_port")),
            "workspace.gold.port_congestion_daily"
        )
        save_table(
            fact_activity_g.select(col("mmsi"), col("activity_date").alias("date"), col("total_distance_km"), col("avg_speed_knots"), col("operating_hours")),
            "workspace.gold.daily_vessel_activity"
        )
        save_table(
            fact_activity_g.select(col("mmsi"), col("vessel_type"), col("activity_date").alias("date"), col("fuel_burn_proxy_tonnes"), col("operating_hours"), col("co2_proxy_tonnes")),
            "workspace.gold.vessel_emissions_proxy"
        )

        try:
            spark.sql("OPTIMIZE workspace.gold.fact_daily_vessel_activity ZORDER BY (mmsi, date_key)")
            spark.sql("OPTIMIZE workspace.gold.fact_port_congestion_daily ZORDER BY (port_code, date_key)")
            spark.sql("OPTIMIZE workspace.gold.fact_voyage_performance ZORDER BY (mmsi, arrival_port_code)")
            spark.sql("OPTIMIZE workspace.gold.dim_vessel ZORDER BY (mmsi)")
            spark.sql("OPTIMIZE workspace.gold.dim_port ZORDER BY (port_code)")
            spark.sql("OPTIMIZE workspace.gold.dim_date ZORDER BY (date_key)")
        except Exception as e:
            print(f"Optimize status: {e}")

        total_g_rows = (
            spark.table("workspace.gold.dim_date").count()
            + spark.table("workspace.gold.dim_vessel").count()
            + spark.table("workspace.gold.dim_port").count()
            + spark.table("workspace.gold.fact_daily_vessel_activity").count()
            + spark.table("workspace.gold.fact_port_congestion_daily").count()
            + spark.table("workspace.gold.fact_voyage_performance").count()
            + spark.table("workspace.gold.fact_pipeline_performance").count()
        )
        g_logger.set_metrics(rows_inserted=total_g_rows)
        print(f"Step 3: Gold Star Schema refreshed ({total_g_rows} rows)")

    total_logger.set_metrics(rows_inserted=b_rows + total_g_rows, rows_updated=v_upd + p_upd)
    print(f"End-to-end pipeline run {batch_id} completed successfully.")
