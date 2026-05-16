# Realtime PySpark Scenarios For Databricks

This folder contains reusable PySpark Structured Streaming code for realistic realtime data engineering scenarios in Databricks.

## Scenarios

1. **Clickstream Analytics**
   - Source: Kafka topic `clickstream.events`
   - Bronze: raw Kafka envelope
   - Silver: parsed, deduplicated page-view events
   - Gold: 5-minute page, campaign, and device aggregations

2. **IoT Telemetry Monitoring**
   - Source: Kafka topic `iot.telemetry`
   - Bronze: raw device telemetry
   - Silver: normalized sensor readings
   - Gold: anomaly counts and average readings by site and device type

3. **Payment Fraud Aggregation**
   - Source: Kafka topic `payments.transactions`
   - Bronze: raw transaction stream
   - Silver: cleaned payment transactions
   - Gold: realtime risk metrics by customer, merchant, and country

## Design

- `src/realtime_pyspark/common`: shared config, Kafka readers, Delta writers, and streaming utilities.
- `src/realtime_pyspark/transformations`: scenario-specific parsing and transformation logic.
- `jobs`: Databricks job entry points.
- `notebooks`: Databricks notebook source files with ordered markdown cells.
- `conf`: dev and prod YAML configs.

## Databricks Usage

1. Copy or sync this repo into Databricks Repos.
2. Update `conf/dev.yml` or `conf/prod.yml` with Kafka bootstrap servers, topics, Unity Catalog names, and checkpoint paths.
3. Configure Kafka credentials through Databricks secrets or cluster Spark configs.
4. Run one of the jobs:

```bash
spark-submit jobs/run_clickstream_analytics.py --config conf/dev.yml
spark-submit jobs/run_iot_telemetry_monitoring.py --config conf/dev.yml
spark-submit jobs/run_payment_fraud_aggregation.py --config conf/dev.yml
```

## Production Notes

- Use a separate checkpoint path per streaming query.
- Store credentials in Databricks secret scopes, not YAML files.
- Use Unity Catalog volumes for checkpoints and Auto Loader schema locations.
- Keep raw Kafka payloads in bronze for replay and audit.
- Apply event-time watermarks before deduplication and aggregation.
- Use `availableNow=True` only for controlled backfills; use processing-time triggers for live streams.
