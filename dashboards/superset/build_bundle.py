"""
Builds a Superset import bundle (the ZIP format `superset import-dashboards` reads)
for the StreamCart business dashboard. Assets stay reviewable as code, and the
database credentials come from the environment rather than being committed.

Usage: python build_bundle.py /tmp/streamcart_bundle.zip
"""

import os
import sys
import uuid
import zipfile
from datetime import datetime, timezone

import yaml

NAMESPACE = uuid.UUID("7f7a4a52-6c1e-4f0a-9d0f-5b0d2c6e1a10")
DB_NAME = "StreamCart Warehouse"


def stable_uuid(name: str) -> str:
    """Same name, same UUID: re-importing updates assets rather than duplicating them."""
    return str(uuid.uuid5(NAMESPACE, name))


DB_UUID = stable_uuid("database")

FUNNEL_SQL = """
SELECT 1 AS stage_order, 'Sessions' AS stage, sum(sessions_started) AS sessions FROM gold.agg_funnel_conversion
UNION ALL SELECT 2, 'Viewed product', sum(sessions_reached_product) FROM gold.agg_funnel_conversion
UNION ALL SELECT 3, 'Added to cart', sum(sessions_reached_cart) FROM gold.agg_funnel_conversion
UNION ALL SELECT 4, 'Reached checkout', sum(sessions_reached_checkout) FROM gold.agg_funnel_conversion
UNION ALL SELECT 5, 'Purchased', sum(sessions_converted) FROM gold.agg_funnel_conversion
"""

# name → (physical table or None, columns as {name: type}, datetime column, virtual SQL)
DATASETS = {
    "agg_daily_revenue": ("gold", {
        "event_date": "DATE", "region": "TEXT", "total_orders": "BIGINT", "unique_customers": "BIGINT",
        "gross_revenue_usd": "DOUBLE PRECISION", "net_revenue_usd": "DOUBLE PRECISION",
        "refund_amount_usd": "DOUBLE PRECISION", "avg_order_value_usd": "DOUBLE PRECISION",
    }, "event_date", None),
    "agg_funnel_conversion": ("gold", {
        "event_date": "DATE", "region": "TEXT", "sessions_started": "BIGINT", "sessions_converted": "BIGINT",
        "overall_conversion_rate_pct": "NUMERIC", "cart_abandonment_rate_pct": "NUMERIC",
    }, "event_date", None),
    "agg_customer_retention": ("gold", {
        "cohort_week": "DATE", "weeks_since_first_order": "INTEGER", "cohort_size": "BIGINT",
        "active_customers": "BIGINT", "retention_pct": "NUMERIC",
    }, None, None),
    "dim_products": ("gold", {
        "product_id": "TEXT", "category": "TEXT", "popularity_tier": "TEXT", "total_orders": "BIGINT",
        "total_units_sold": "BIGINT", "total_revenue_usd": "DOUBLE PRECISION",
    }, None, None),
    "funnel_stages": ("gold", {"stage_order": "INTEGER", "stage": "TEXT", "sessions": "BIGINT"}, None, FUNNEL_SQL),
}


def metric(column: str, aggregate: str, label: str) -> dict:
    return {
        "expressionType": "SIMPLE", "aggregate": aggregate, "label": label, "optionName": f"metric_{column}_{aggregate}",
        "column": {"column_name": column},
    }


def timeseries_params(dataset, x_axis, metrics, groupby=None, viz="echarts_timeseries_line", **extra):
    return {
        "viz_type": viz, "x_axis": x_axis, "time_grain_sqla": "P1D", "metrics": metrics,
        "groupby": groupby or [], "adhoc_filters": [], "row_limit": 10000,
        "show_legend": True, "rich_tooltip": True, "datasource": f"{dataset}__table", **extra,
    }


CHARTS = {
    "Total net revenue": ("agg_daily_revenue", {
        "viz_type": "big_number_total", "metric": metric("net_revenue_usd", "SUM", "Net revenue"),
        "adhoc_filters": [], "y_axis_format": "$,.0f", "subheader": "All time, USD",
    }),
    "Total orders": ("agg_daily_revenue", {
        "viz_type": "big_number_total", "metric": metric("total_orders", "SUM", "Orders"),
        "adhoc_filters": [], "y_axis_format": ",d", "subheader": "All time",
    }),
    "Revenue trend by region": ("agg_daily_revenue", timeseries_params(
        "agg_daily_revenue", "event_date", [metric("net_revenue_usd", "SUM", "Net revenue")], ["region"],
        y_axis_format="$,.0f", stack=None, area=False,
    )),
    "Average order value": ("agg_daily_revenue", timeseries_params(
        "agg_daily_revenue", "event_date", [metric("avg_order_value_usd", "AVG", "Average order value")],
        y_axis_format="$,.2f",
    )),
    "Customer retention cohorts (% of cohort ordering)": ("agg_customer_retention", {
        "viz_type": "pivot_table_v2", "groupbyRows": ["cohort_week"], "groupbyColumns": ["weeks_since_first_order"],
        "metrics": [metric("retention_pct", "MAX", "Retention %")], "adhoc_filters": [], "row_limit": 10000,
        "aggregateFunction": "Sum", "valueFormat": ",.1f", "date_format": "%Y-%m-%d", "metricsLayout": "COLUMNS",
        "rowOrder": "key_a_to_z", "colOrder": "key_a_to_z",
        "conditional_formatting": [{"colorScheme": "#1FA8C9", "column": "Retention %", "operator": "≥", "targetValue": 0}],
    }),
    "Conversion funnel": ("funnel_stages", {
        "viz_type": "funnel", "groupby": ["stage"], "metric": metric("sessions", "MAX", "Sessions"),
        "adhoc_filters": [], "row_limit": 10, "sort_by_metric": True, "show_labels": True, "show_legend": False,
        "label_type": "key_value_percent", "number_format": ",d",
    }),
    "Conversion rate by region": ("agg_funnel_conversion", timeseries_params(
        "agg_funnel_conversion", "event_date", [metric("overall_conversion_rate_pct", "AVG", "Conversion %")], ["region"],
        y_axis_format=",.1f",
    )),
    "Top 15 products by revenue": ("dim_products", {
        "viz_type": "table", "query_mode": "raw",
        "all_columns": ["product_id", "category", "popularity_tier", "total_orders", "total_units_sold", "total_revenue_usd"],
        "order_by_cols": ['["total_revenue_usd", false]'], "row_limit": 15, "adhoc_filters": [],
        "column_config": {"total_revenue_usd": {"d3NumberFormat": "$,.0f"}, "total_units_sold": {"d3NumberFormat": ",d"}},
        "include_search": False, "page_length": 0,
    }),
    "Revenue by category": ("dim_products", {
        "viz_type": "pie", "groupby": ["category"], "metric": metric("total_revenue_usd", "SUM", "Revenue"),
        "adhoc_filters": [], "row_limit": 10, "donut": True, "show_labels": True, "label_type": "key_percent",
        "show_legend": False, "number_format": "$,.0f",
    }),
}

# Dashboard layout: rows of (chart name, width out of 12, height)
LAYOUT = [
    [("Total net revenue", 3, 26), ("Total orders", 3, 26), ("Average order value", 6, 26)],
    [("Revenue trend by region", 12, 55)],
    [("Customer retention cohorts (% of cohort ordering)", 12, 50)],
    [("Conversion funnel", 5, 55), ("Conversion rate by region", 7, 55)],
    [("Top 15 products by revenue", 7, 60), ("Revenue by category", 5, 60)],
]


def database_yaml():
    uri = "postgresql+psycopg2://{u}:{p}@postgres:5432/{d}".format(
        u=os.environ["POSTGRES_USER"], p=os.environ["POSTGRES_PASSWORD"], d=os.environ["POSTGRES_DB"]
    )
    return {
        "database_name": DB_NAME, "sqlalchemy_uri": uri, "cache_timeout": None, "expose_in_sqllab": True,
        "allow_run_async": False, "allow_ctas": False, "allow_cvas": False, "allow_dml": False,
        "allow_file_upload": False, "extra": "{}", "uuid": DB_UUID, "version": "1.0.0",
    }


def dataset_yaml(name, schema, columns, dttm, sql):
    return {
        "table_name": name, "main_dttm_col": dttm, "description": None, "default_endpoint": None, "offset": 0,
        "cache_timeout": None, "schema": schema, "sql": sql, "params": None, "template_params": None,
        "filter_select_enabled": True, "fetch_values_predicate": None, "extra": None,
        "uuid": stable_uuid(f"dataset:{name}"), "metrics": [
            {"metric_name": "count", "verbose_name": "COUNT(*)", "metric_type": "count", "expression": "COUNT(*)",
             "description": None, "d3format": None, "extra": None, "warning_text": None},
        ], "columns": [
            {"column_name": col, "verbose_name": None, "is_dttm": col == dttm, "is_active": True, "type": kind,
             "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None,
             "advanced_data_type": None, "extra": None}
            for col, kind in columns.items()
        ], "version": "1.0.0", "database_uuid": DB_UUID,
    }


def chart_yaml(name, dataset, params):
    return {
        "slice_name": name, "description": None, "certified_by": None, "certification_details": None,
        "viz_type": params["viz_type"], "params": params, "query_context": None, "cache_timeout": None,
        "uuid": stable_uuid(f"chart:{name}"), "version": "1.0.0", "dataset_uuid": stable_uuid(f"dataset:{dataset}"),
    }


def dashboard_yaml():
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": []},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": "StreamCart: Business Intelligence"}},
    }
    for r, row in enumerate(LAYOUT):
        row_id = f"ROW-{r}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "id": row_id, "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"], "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for c, (name, width, height) in enumerate(row):
            chart_id = f"CHART-{r}-{c}"
            position[row_id]["children"].append(chart_id)
            position[chart_id] = {
                "id": chart_id, "type": "CHART", "parents": ["ROOT_ID", "GRID_ID", row_id], "children": [],
                "meta": {"uuid": stable_uuid(f"chart:{name}"), "sliceName": name, "width": width, "height": height},
            }
    return {
        "dashboard_title": "StreamCart: Business Intelligence", "description": None, "css": "", "slug": "streamcart-bi",
        "certified_by": None, "certification_details": None, "published": True,
        "uuid": stable_uuid("dashboard"), "position": position,
        "metadata": {"color_scheme": "supersetColors", "refresh_frequency": 0, "timed_refresh_immune_slices": [],
                     "expanded_slices": {}, "label_colors": {}, "default_filters": "{}"},
        "version": "1.0.0",
    }


def main(path):
    files = {
        "metadata.yaml": {"version": "1.0.0", "type": "Dashboard", "timestamp": datetime.now(timezone.utc).isoformat()},
        "databases/warehouse.yaml": database_yaml(),
        "dashboards/streamcart_bi.yaml": dashboard_yaml(),
    }
    for name, (schema, columns, dttm, sql) in DATASETS.items():
        files[f"datasets/warehouse/{name}.yaml"] = dataset_yaml(name, schema, columns, dttm, sql)
    for i, (name, (dataset, params)) in enumerate(CHARTS.items()):
        files[f"charts/chart_{i}.yaml"] = chart_yaml(name, dataset, params)

    with zipfile.ZipFile(path, "w") as bundle:
        for filename, content in files.items():
            bundle.writestr(f"streamcart_bundle/{filename}", yaml.safe_dump(content, sort_keys=False, allow_unicode=True))
    print(f"Wrote {len(files)} assets to {path}")


if __name__ == "__main__":
    main(sys.argv[1])
