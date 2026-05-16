from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from realtime_pyspark.common.config import KafkaSettings


def read_kafka_topic(
    spark: SparkSession,
    settings: KafkaSettings,
    topic: str,
    *,
    username_secret: str | None = None,
    password_secret: str | None = None,
) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("failOnDataLoss", str(settings.fail_on_data_loss).lower())
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
    )

    if username_secret and password_secret:
        reader = (
            reader.option("kafka.security.protocol", settings.security_protocol)
            .option("kafka.sasl.mechanism", settings.sasl_mechanism)
            .option(
                "kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{username_secret}" password="{password_secret}";',
            )
        )

    return reader.load()


def kafka_envelope(df: DataFrame, source_name: str) -> DataFrame:
    return df.select(
        F.lit(source_name).alias("source_name"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("message_value"),
        F.current_timestamp().alias("ingest_time"),
    )
