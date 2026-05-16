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
from realtime_pyspark.transformations.payments import (
    aggregate_payment_risk,
    parse_payment_events,
    score_payment_risk,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run realtime payment fraud aggregation.")
    parser.add_argument("--config", required=True, help="Path to YAML environment config.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    spark = SparkSession.builder.appName(f"{config.environment}-payment-fraud").getOrCreate()
    configure_streaming_session(spark)

    raw_kafka_df = read_kafka_topic(spark, config.kafka, config.topics["payment_transactions"])
    bronze_df = kafka_envelope(raw_kafka_df, "payment_transactions")
    silver_df = score_payment_risk(parse_payment_events(bronze_df), config.streaming.watermark_delay)
    gold_df = aggregate_payment_risk(silver_df, config.streaming.watermark_delay)

    queries = [
        write_stream_to_delta_table(
            bronze_df,
            table_name=config.table("payments_bronze"),
            checkpoint_path=config.checkpoint("payments_bronze"),
            query_name="payments_bronze",
            trigger_interval=config.streaming.trigger_interval,
        ),
        write_stream_to_delta_table(
            silver_df,
            table_name=config.table("payments_silver"),
            checkpoint_path=config.checkpoint("payments_silver"),
            query_name="payments_silver",
            trigger_interval=config.streaming.trigger_interval,
            partition_by=["event_date"],
        ),
        write_stream_to_delta_table(
            gold_df,
            table_name=config.table("payments_gold"),
            checkpoint_path=config.checkpoint("payments_gold"),
            query_name="payments_gold",
            trigger_interval=config.streaming.trigger_interval,
            output_mode="update",
        ),
    ]
    await_queries(queries)


if __name__ == "__main__":
    main()
