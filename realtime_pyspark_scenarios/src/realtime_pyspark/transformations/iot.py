from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


iot_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity_pct", DoubleType(), True),
        StructField("vibration_mm_s", DoubleType(), True),
        StructField("battery_pct", DoubleType(), True),
    ]
)


def parse_iot_events(bronze_df: DataFrame) -> DataFrame:
    parsed_df = bronze_df.withColumn("payload", F.from_json("message_value", iot_schema))
    return parsed_df.select(
        "source_name",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "message_key",
        "message_value",
        "ingest_time",
        F.col("payload.event_id").alias("event_id"),
        F.col("payload.event_time").alias("event_time"),
        F.col("payload.device_id").alias("device_id"),
        F.col("payload.device_type").alias("device_type"),
        F.col("payload.site_id").alias("site_id"),
        F.col("payload.temperature_c").alias("temperature_c"),
        F.col("payload.humidity_pct").alias("humidity_pct"),
        F.col("payload.vibration_mm_s").alias("vibration_mm_s"),
        F.col("payload.battery_pct").alias("battery_pct"),
    )


def detect_iot_anomalies(df: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        df.filter(F.col("event_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("device_id").isNotNull())
        .withWatermark("event_time", watermark_delay)
        .dropDuplicates(["event_id"])
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn(
            "is_anomaly",
            (F.col("temperature_c") > 80.0)
            | (F.col("vibration_mm_s") > 12.0)
            | (F.col("battery_pct") < 10.0),
        )
        .withColumn(
            "anomaly_reason",
            F.when(F.col("temperature_c") > 80.0, F.lit("high_temperature"))
            .when(F.col("vibration_mm_s") > 12.0, F.lit("high_vibration"))
            .when(F.col("battery_pct") < 10.0, F.lit("low_battery"))
            .otherwise(F.lit("normal")),
        )
        .withColumn("processed_time", F.current_timestamp())
    )


def aggregate_iot_anomalies(df: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        df.withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window("event_time", "5 minutes").alias("event_window"),
            F.col("site_id"),
            F.col("device_type"),
        )
        .agg(
            F.count("*").alias("reading_count"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
            F.avg("temperature_c").alias("avg_temperature_c"),
            F.avg("vibration_mm_s").alias("avg_vibration_mm_s"),
            F.min("battery_pct").alias("min_battery_pct"),
            F.max("processed_time").alias("last_processed_time"),
        )
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "site_id",
            "device_type",
            "reading_count",
            "anomaly_count",
            "avg_temperature_c",
            "avg_vibration_mm_s",
            "min_battery_pct",
            "last_processed_time",
        )
    )
