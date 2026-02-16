# zynex

[![PyPI version](https://badge.fury.io/py/zynex.svg)](https://badge.fury.io/py/zynex)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

 **Fast, notebook-first data quality checks for Spark / Databricks**

zynex is a lightweight data-quality validation library for Apache Spark, designed specifically for Databricks notebooks.  
It provides quick, readable checks for common data issues without requiring schemas, configuration files, or heavy setup.

## What zynex does

zynex focuses on a small set of high-signal checks that catch the most common data issues in analytical pipelines:

- **Structural issues** — duplicate full rows
- **Data quality** — null ratios per column
- **Distribution problems** — extreme values and skewed data
- **Storage hygiene** — small-file detection for Delta tables (metadata only)

The goal is not exhaustive validation, but fast feedback you can trust while working in notebooks.

## Installation 
```bash
pip install zynex
```
## Quick Start
```python
from zynex import zx

zx("schema.table")
```

## API
### Primary entry point:
```python
zx(
    source,
    table_name=None,
    render=True,
    cache=False,
    modules=None,
    config=None,
)
```

# Input Modes
### 1 Validate a catalog table
```python
zx("schema.table")
```

### or with Unity Catalog:
```python
zx("catalog.schema.table")
```
### Behavior:
* Loads table via spark.table(...)
* Runs pre-flight metadata checks (if Delta)
* Runs full data scan

### 2️ Validate a Spark DataFrame
```python
zx(df)
```
### Behavior:
* Skips metadata preflight
* Runs full data scan only

### 3️ Validate DataFrame with table context
```python
zx(df, table_name="schema.table")
```
Behavior:
* Uses provided DataFrame
* Uses table metadata for preflight checks
* Avoids re-reading table

# Optional Arguments
### render

Default: ```True```

If ```False```, returns a ```ValidationReport``` object instead of printing.
```python
report = zx("schema.table", render=False)
```

### cache

Default: ```False```

If ```True```, DataFrame is persisted during validation.
```python
zx("schema.table", cache=True)
```
Recommended for large datasets.

### modules

Default: ```["core_quality"]```

You can explicitly select modules:
```python
zx("schema.table", modules=["core_quality"])
```

### config

Override rule configuration:
```python
zx(
    "schema.table",
    config={
        "extreme_values_threshold_stddev": 2.0
    }
)
```
Currently supported config keys:
* extreme_values_threshold_stddev (default: 3.0)
* cache (internal, set via argument)

# Output Structure

Zynex prints:
* Dataset summary (rows × columns)
* Rule results grouped by:
    * OK
    * WARNING
    * ERROR
    * NOT_APPLICABLE

Example:
```yml
ZYNEX REPORT
Dataset: 240 000 rows x 10 columns | 0 Errors | 3 Warnings

[WARNING] duplicate_rows
[WARNING] null_ratio
[WARNING] extreme_values
```

# Pre-Flight Behavior
When validating a table:
* Metadata checks run first (e.g., small_files)
* Results are printed immediately
* Validation continues regardless of warnings

Zynex does not block execution.

If fragmentation is detected:
* Recommendation is shown
* User decides whether to run OPTIMIZE

# Table Name Errors
If a table name is incorrect:
```python
zx("schema.wrong_name")
```
Zynex prints:
* Clear error message
* Suggested similar tables (if available)
* Hint to use SHOW TABLES

Validation stops early in this case.

# Design Philosophy

Zynex is:
* Spark-native
* Notebook-first
* Advisory (not policy enforcement)
* Lightweight and modular

It is not:
* A data governance framework
* A pipeline orchestrator
* A blocking validation gate

# Return Value

If render=```False```, returns:
```yml
ValidationReport
```
Containing:

* row count
* column count
* rule results
* metrics
* messages

## Inspecting the result programmatically
```python
print(report.rows)
print(report.columns)
```
## Iterate over rule results
```python
for r in report.results:
    print(r.name, r.status)
```
### Example:
```python
for r in report.results:
    if r.name == "duplicate_rows":
        print(r.metrics)
```
Example metrics for duplicate_rows:
```python
{
    "duplicate_count": 40000,
    "duplicate_ratio": 0.1667
}
```
Example metrics for null_ratio:
```python
{
    "total_nulls": 10764,
    "columns_with_nulls": {
        "amount": 7205,
        "region": 2428,
        "customer_id": 1131
    }
}
```
# Requirements

* Spark 3.x
* Databricks or compatible Spark environment
* Delta tables for metadata preflight
