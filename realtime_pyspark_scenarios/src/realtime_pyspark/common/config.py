from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str
    starting_offsets: str
    fail_on_data_loss: bool
    max_offsets_per_trigger: int


@dataclass(frozen=True)
class StreamingSettings:
    watermark_delay: str
    trigger_interval: str


@dataclass(frozen=True)
class AppConfig:
    environment: str
    catalog: str
    schema: str
    checkpoint_root: str
    kafka: KafkaSettings
    streaming: StreamingSettings
    topics: dict[str, str]
    tables: dict[str, str]

    def table(self, key: str) -> str:
        return f"{self.catalog}.{self.schema}.{self.tables[key]}"

    def checkpoint(self, query_name: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{query_name}"


def load_config(path: str | Path) -> AppConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return parse_config(raw)


def parse_config(raw: dict[str, Any]) -> AppConfig:
    return AppConfig(
        environment=raw["environment"],
        catalog=raw["catalog"],
        schema=raw["schema"],
        checkpoint_root=raw["checkpoint_root"],
        kafka=KafkaSettings(**raw["kafka"]),
        streaming=StreamingSettings(**raw["streaming"]),
        topics=dict(raw["topics"]),
        tables=dict(raw["tables"]),
    )
