"""
Great Expectations validation for the Silver layer.

The suites are plain JSON in quality/expectations, so a rule can be reviewed and
changed without touching pipeline code. Each table is validated twice per run:

  raw    before cleaning. Failures are expected, and show how dirty the input was.
  clean  after cleaning. This is the gate: any failure blocks the write.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import great_expectations as gx

EXPECTATIONS_DIR = Path(__file__).parent / "expectations"


@dataclass
class ExpectationResult:
    expectation: str
    column: str
    success: bool
    unexpected_count: int


def load_suite(table: str) -> list[dict]:
    return json.loads((EXPECTATIONS_DIR / f"{table}.json").read_text())


def validate(df, table: str, stage: str) -> list[ExpectationResult]:
    """Run a table's suite against a Spark DataFrame and return one result per expectation."""
    context = gx.get_context()
    datasource = context.sources.add_or_update_spark(name=f"silver_{table}_{stage}")
    asset = datasource.add_dataframe_asset(name=table)
    validator = context.get_validator(
        batch_request=asset.build_batch_request(dataframe=df),
        create_expectation_suite_with_name=f"{table}_{stage}",
    )

    results = []
    for rule in load_suite(table):
        outcome = getattr(validator, rule["expectation_type"])(**rule["kwargs"])
        results.append(ExpectationResult(
            expectation=rule["expectation_type"],
            column=rule["kwargs"].get("column", ""),
            success=bool(outcome.success),
            unexpected_count=int(outcome.result.get("unexpected_count") or 0),
        ))
    return results
