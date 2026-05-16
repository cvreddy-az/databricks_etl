from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession

from realtime_pyspark.common.config import load_config
from realtime_pyspark.common.kafka import kafka_envelope, read_kafka_topic
from realtime_pyspark.common.streaming import await_queries, configure_streaming_session
from realtime_pyspark.common.writers import write_stream_to_delta_table
from realtime_pyspark.transformations.iot import (
    aggregate_iot_anomalies,
    detect_iot_anomalies,
    parse_iot_events,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run realtime IoT telemetry monitoring.")
    parser.add_argument("--config", required=True, help="Path to YAML environment config.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    spark = SparkSession.builder.appName(f"{config.environment}-iot-telemetry").getOrCreate()
    configure_streaming_session(spark)

    raw_kafka_df = read_kafka_topic(spark, config.kafka, config.topics["iot_telemetry"])
    bronze_df = kafka_envelope(raw_kafka_df, "iot_telemetry")
    silver_df = detect_iot_anomalies(parse_iot_events(bronze_df), config.streaming.watermark_delay)
    gold_df = aggregate_iot_anomalies(silver_df, config.streaming.watermark_delay)

    queries = [
        write_stream_to_delta_table(
            bronze_df,
            table_name=config.table("iot_bronze"),
            checkpoint_path=config.checkpoint("iot_bronze"),
            query_name="iot_bronze",
            trigger_interval=config.streaming.trigger_interval,
        ),
        write_stream_to_delta_table(
            silver_df,
            table_name=config.table("iot_silver"),
            checkpoint_path=config.checkpoint("iot_silver"),
            query_name="iot_silver",
            trigger_interval=config.streaming.trigger_interval,
            partition_by=["event_date"],
        ),
        write_stream_to_delta_table(
            gold_df,
            table_name=config.table("iot_gold"),
            checkpoint_path=config.checkpoint("iot_gold"),
            query_name="iot_gold",
            trigger_interval=config.streaming.trigger_interval,
            output_mode="update",
        ),
    ]
    await_queries(queries)


if __name__ == "__main__":
    main()
