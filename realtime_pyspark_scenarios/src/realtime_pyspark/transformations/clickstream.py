from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType


clickstream_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("is_bot", BooleanType(), True),
    ]
)


def parse_clickstream_events(bronze_df: DataFrame) -> DataFrame:
    parsed_df = bronze_df.withColumn("payload", F.from_json("message_value", clickstream_schema))
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
        F.col("payload.user_id").alias("user_id"),
        F.col("payload.session_id").alias("session_id"),
        F.col("payload.page_url").alias("page_url"),
        F.col("payload.referrer").alias("referrer"),
        F.col("payload.device_type").alias("device_type"),
        F.col("payload.campaign_id").alias("campaign_id"),
        F.col("payload.country").alias("country"),
        F.coalesce(F.col("payload.is_bot"), F.lit(False)).alias("is_bot"),
    )


def clean_clickstream_events(df: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        df.filter(F.col("event_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(~F.col("is_bot"))
        .withWatermark("event_time", watermark_delay)
        .dropDuplicates(["event_id"])
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("page_path", F.expr("parse_url(page_url, 'PATH')"))
        .withColumn("processed_time", F.current_timestamp())
    )


def aggregate_clickstream_metrics(df: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        df.withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window("event_time", "5 minutes").alias("event_window"),
            F.col("page_path"),
            F.col("campaign_id"),
            F.col("device_type"),
            F.col("country"),
        )
        .agg(
            F.count("*").alias("page_views"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.max("processed_time").alias("last_processed_time"),
        )
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "page_path",
            "campaign_id",
            "device_type",
            "country",
            "page_views",
            "unique_users",
            "unique_sessions",
            "last_processed_time",
        )
    )
