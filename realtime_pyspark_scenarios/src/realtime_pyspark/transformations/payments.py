from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


payment_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
    ]
)


def parse_payment_events(bronze_df: DataFrame) -> DataFrame:
    parsed_df = bronze_df.withColumn("payload", F.from_json("message_value", payment_schema))
    return parsed_df.select(
        "source_name",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "message_key",
        "message_value",
        "ingest_time",
        F.col("payload.transaction_id").alias("transaction_id"),
        F.col("payload.event_time").alias("event_time"),
        F.col("payload.customer_id").alias("customer_id"),
        F.col("payload.merchant_id").alias("merchant_id"),
        F.col("payload.card_id").alias("card_id"),
        F.col("payload.amount").alias("amount"),
        F.col("payload.currency").alias("currency"),
        F.col("payload.country").alias("country"),
        F.col("payload.channel").alias("channel"),
        F.col("payload.status").alias("status"),
    )


def score_payment_risk(df: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        df.filter(F.col("transaction_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .withWatermark("event_time", watermark_delay)
        .dropDuplicates(["transaction_id"])
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("amount", F.coalesce("amount", F.lit(0.0)))
        .withColumn(
            "risk_score",
            F.when(F.col("amount") >= 5000.0, F.lit(80))
            .when(F.col("amount") >= 1000.0, F.lit(50))
            .otherwise(F.lit(10))
            + F.when(F.col("channel") == "card_not_present", F.lit(15)).otherwise(F.lit(0))
            + F.when(F.col("status") == "declined", F.lit(20)).otherwise(F.lit(0)),
        )
        .withColumn("is_high_risk", F.col("risk_score") >= 70)
        .withColumn("processed_time", F.current_timestamp())
    )


def aggregate_payment_risk(df: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        df.withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window("event_time", "5 minutes").alias("event_window"),
            F.col("customer_id"),
            F.col("merchant_id"),
            F.col("country"),
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.max("risk_score").alias("max_risk_score"),
            F.sum(F.col("is_high_risk").cast("int")).alias("high_risk_count"),
            F.max("processed_time").alias("last_processed_time"),
        )
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "customer_id",
            "merchant_id",
            "country",
            "transaction_count",
            "total_amount",
            "avg_risk_score",
            "max_risk_score",
            "high_risk_count",
            "last_processed_time",
        )
    )
