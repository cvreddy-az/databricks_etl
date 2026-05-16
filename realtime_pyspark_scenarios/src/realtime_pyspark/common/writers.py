from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_stream_to_delta_table(
    df: DataFrame,
    *,
    table_name: str,
    checkpoint_path: str,
    query_name: str,
    trigger_interval: str,
    output_mode: str = "append",
    partition_by: list[str] | None = None,
) -> StreamingQuery:
    writer = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .queryName(query_name)
        .outputMode(output_mode)
        .trigger(processingTime=trigger_interval)
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    return writer.toTable(table_name)


def write_console_for_debug(df: DataFrame, query_name: str) -> StreamingQuery:
    return (
        df.writeStream.format("console")
        .option("truncate", "false")
        .queryName(query_name)
        .outputMode("append")
        .start()
    )
