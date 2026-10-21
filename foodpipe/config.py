"""Load plain cluster settings and build the Kafka bootstrap address list."""

import os
from pathlib import Path


def _parse_env(path: Path) -> dict[str, str]:
    settings = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        settings[key] = value
    return settings


def load_env(name: str | None = None) -> dict[str, str]:
    """Read the selected config file relative to the repository root."""
    name = name or os.environ.get("FOODPIPE_ENV") or "cluster"
    path = Path(__file__).resolve().parents[1] / "config" / f"{name}.env"
    return _parse_env(path)


def bootstrap_servers(env: dict) -> str:
    """Return both broker addresses, rejecting placeholder broker IPs."""
    for key in ("VM2_IP", "VM3_IP"):
        if env[key] == "CHANGE_ME":
            raise ValueError(f"{key} is CHANGE_ME; configure the broker IP")
    return f"{env['VM2_IP']}:{env['VM2_KAFKA_PORT']},{env['VM3_IP']}:{env['VM3_KAFKA_PORT']}"
