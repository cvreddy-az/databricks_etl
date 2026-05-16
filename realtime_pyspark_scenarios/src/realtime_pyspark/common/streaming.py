from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery


def configure_streaming_session(spark: SparkSession) -> None:
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", spark.conf.get("spark.sql.shuffle.partitions", "200"))


def await_queries(queries: list[StreamingQuery]) -> None:
    try:
        queries[0].sparkSession.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.exception():
                raise query.exception()
