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
from realtime_pyspark.transformations.clickstream import (
    aggregate_clickstream_metrics,
    clean_clickstream_events,
    parse_clickstream_events,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run realtime clickstream analytics.")
    parser.add_argument("--config", required=True, help="Path to YAML environment config.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    spark = SparkSession.builder.appName(f"{config.environment}-clickstream-analytics").getOrCreate()
    configure_streaming_session(spark)

    raw_kafka_df = read_kafka_topic(spark, config.kafka, config.topics["clickstream"])
    bronze_df = kafka_envelope(raw_kafka_df, "clickstream")
    silver_df = clean_clickstream_events(
        parse_clickstream_events(bronze_df),
        config.streaming.watermark_delay,
    )
    gold_df = aggregate_clickstream_metrics(silver_df, config.streaming.watermark_delay)

    queries = [
        write_stream_to_delta_table(
            bronze_df,
            table_name=config.table("clickstream_bronze"),
            checkpoint_path=config.checkpoint("clickstream_bronze"),
            query_name="clickstream_bronze",
            trigger_interval=config.streaming.trigger_interval,
        ),
        write_stream_to_delta_table(
            silver_df,
            table_name=config.table("clickstream_silver"),
            checkpoint_path=config.checkpoint("clickstream_silver"),
            query_name="clickstream_silver",
            trigger_interval=config.streaming.trigger_interval,
            partition_by=["event_date"],
        ),
        write_stream_to_delta_table(
            gold_df,
            table_name=config.table("clickstream_gold"),
            checkpoint_path=config.checkpoint("clickstream_gold"),
            query_name="clickstream_gold",
            trigger_interval=config.streaming.trigger_interval,
            output_mode="update",
        ),
    ]
    await_queries(queries)


if __name__ == "__main__":
    main()
