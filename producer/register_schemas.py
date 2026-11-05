"""
Registers the Avro contracts in schemas/avro with the Schema Registry and requires
BACKWARD compatibility, so an incompatible change is refused at registration time.
Run once by the schema-init service; re-running with unchanged schemas is a no-op.
"""

import json
import os
import time
from pathlib import Path

import requests

REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", Path(__file__).resolve().parent.parent / "schemas" / "avro"))
HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def wait_for_registry(attempts: int = 30):
    for _ in range(attempts):
        try:
            requests.get(f"{REGISTRY}/subjects", timeout=5).raise_for_status()
            return
        except requests.RequestException:
            time.sleep(2)
    raise SystemExit(f"Schema Registry not reachable at {REGISTRY}")


def main():
    wait_for_registry()
    requests.put(f"{REGISTRY}/config", headers=HEADERS, json={"compatibility": "BACKWARD"}, timeout=10).raise_for_status()

    for path in sorted(SCHEMA_DIR.glob("*.avsc")):
        subject = path.stem  # e.g. transactions-value, the default topic-value naming
        schema = json.dumps(json.loads(path.read_text()))
        response = requests.post(
            f"{REGISTRY}/subjects/{subject}/versions", headers=HEADERS, json={"schema": schema}, timeout=10
        )
        response.raise_for_status()
        print(f"{subject}: schema id {response.json()['id']}")


if __name__ == "__main__":
    main()
