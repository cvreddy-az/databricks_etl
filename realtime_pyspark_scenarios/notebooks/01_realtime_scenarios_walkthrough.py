# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Realtime PySpark Scenarios
# MAGIC
# MAGIC This notebook shows how to run the same modular code used by the production job entry points.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01. Parameters

# COMMAND ----------

dbutils.widgets.text("config_path", "realtime_pyspark_scenarios/conf/dev.yml")
dbutils.widgets.dropdown("scenario", "clickstream", ["clickstream", "iot_telemetry", "payment_transactions"])

config_path = dbutils.widgets.get("config_path")
scenario = dbutils.widgets.get("scenario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02. Load Shared Modules

# COMMAND ----------

import sys
from pathlib import Path

repo_root = Path.cwd()
scenario_src = repo_root / "realtime_pyspark_scenarios" / "src"
if not scenario_src.exists():
    scenario_src = repo_root.parent / "realtime_pyspark_scenarios" / "src"
sys.path.insert(0, str(scenario_src))

from realtime_pyspark.common.config import load_config
from realtime_pyspark.common.kafka import kafka_envelope, read_kafka_topic
from realtime_pyspark.common.streaming import configure_streaming_session
from realtime_pyspark.common.writers import write_stream_to_delta_table
from realtime_pyspark.transformations.clickstream import (
    aggregate_clickstream_metrics,
    clean_clickstream_events,
    parse_clickstream_events,
)
from realtime_pyspark.transformations.iot import (
    aggregate_iot_anomalies,
    detect_iot_anomalies,
    parse_iot_events,
)
from realtime_pyspark.transformations.payments import (
    aggregate_payment_risk,
    parse_payment_events,
    score_payment_risk,
)

config = load_config(config_path)
configure_streaming_session(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03. Select Realtime Source

# COMMAND ----------

topic_name = config.topics[scenario]
raw_kafka_df = read_kafka_topic(spark, config.kafka, topic_name)
bronze_df = kafka_envelope(raw_kafka_df, scenario)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04. Apply Scenario Transformations

# COMMAND ----------

if scenario == "clickstream":
    silver_df = clean_clickstream_events(
        parse_clickstream_events(bronze_df),
        config.streaming.watermark_delay,
    )
    gold_df = aggregate_clickstream_metrics(silver_df, config.streaming.watermark_delay)
    bronze_table_key = "clickstream_bronze"
    silver_table_key = "clickstream_silver"
    gold_table_key = "clickstream_gold"
elif scenario == "iot_telemetry":
    silver_df = detect_iot_anomalies(parse_iot_events(bronze_df), config.streaming.watermark_delay)
    gold_df = aggregate_iot_anomalies(silver_df, config.streaming.watermark_delay)
    bronze_table_key = "iot_bronze"
    silver_table_key = "iot_silver"
    gold_table_key = "iot_gold"
else:
    silver_df = score_payment_risk(parse_payment_events(bronze_df), config.streaming.watermark_delay)
    gold_df = aggregate_payment_risk(silver_df, config.streaming.watermark_delay)
    bronze_table_key = "payments_bronze"
    silver_table_key = "payments_silver"
    gold_table_key = "payments_gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05. Write To Unity Catalog Delta Tables

# COMMAND ----------

bronze_query = write_stream_to_delta_table(
    bronze_df,
    table_name=config.table(bronze_table_key),
    checkpoint_path=config.checkpoint(bronze_table_key),
    query_name=bronze_table_key,
    trigger_interval=config.streaming.trigger_interval,
)

silver_query = write_stream_to_delta_table(
    silver_df,
    table_name=config.table(silver_table_key),
    checkpoint_path=config.checkpoint(silver_table_key),
    query_name=silver_table_key,
    trigger_interval=config.streaming.trigger_interval,
    partition_by=["event_date"],
)

gold_query = write_stream_to_delta_table(
    gold_df,
    table_name=config.table(gold_table_key),
    checkpoint_path=config.checkpoint(gold_table_key),
    query_name=gold_table_key,
    trigger_interval=config.streaming.trigger_interval,
    output_mode="update",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06. Monitor Active Streams

# COMMAND ----------

display(
    spark.createDataFrame(
        [
            {
                "name": query.name,
                "id": str(query.id),
                "run_id": str(query.runId),
                "is_active": query.isActive,
                "message": query.status.get("message"),
            }
            for query in spark.streams.active
        ]
    )
)
