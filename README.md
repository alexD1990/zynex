# dcheck

 **Fast, notebook-first data quality checks for Spark / Databricks**

dcheck is a lightweight data-quality validation library for Apache Spark, designed specifically for Databricks notebooks.  
It provides quick, readable checks for common data issues without requiring schemas, configuration files, or heavy setup.

## What dcheck does

dcheck focuses on a small set of high-signal checks that catch the most common data issues in analytical pipelines:

- **Structural issues** — duplicate full rows
- **Data quality** — null ratios per column
- **Distribution problems** — extreme values and skewed data
- **Storage hygiene** — small-file detection for Delta tables (metadata only)

The goal is not exhaustive validation, but fast feedback you can trust while working in notebooks.

## Installation (Databricks)

Install dcheck directly in your Databricks notebook:
> **Databricks note:** Install with `--no-deps` to avoid pip upgrading `pyspark` and breaking the runtime.

```bash
%pip install --no-deps dcheck
dbutils.library.restartPython()
```
Installing from GitHub repo:
```python
%pip install --no-deps --no-cache-dir git+https://github.com/alexD1990/DCheck.git@main
dbutils.library.restartPython()
```
Restarting the Python kernel is required after installation.

## Quick start

The simplest way to use dcheck is to validate a table by name:

```python
from dcheck.api import dc

dc("catalog.schema.table") 
```
## This will:

1. Run a pre-flight metadata check (small-file analysis, if a table name is provided)
```
[OK] small_files
• Rating       : NOT_APPLICABLE
• Num Files    : 5
• Total Size   : 0.19 MB
• Avg File Size: 0.04 MB
```
2. Run a full data scan
```
DCHECK REPORT
Dataset: 5 033 rows x 10 columns | 0 Errors | 3 Warnings

[WARNING] duplicate_rows
• 30 duplicate rows (0.60%)

[WARNING] null_ratio
• 5 649 null values across 3 columns

[WARNING] extreme_values
• Extreme deviations detected in 5 columns
```
3. Render a clear validation report directly in the notebook

## Usage patterns

dcheck supports three explicit validation modes, depending on how your data is loaded.

### 1. Validate a catalog table (recommended)

```python
dc("workspace.default.my_table")
```
This is the primary and recommended usage:
* Runs a pre-flight metadata check (small-file analysis)
* Then performs a full data scan
* Uses the table as the single source of truth

Use this whenever the data exists as a Delta table in the catalog.

### 2. Validate a Spark DataFrame only
```python
dc(df)
```
This mode:
* Skips all metadata checks
* Runs only data-level validation rules
* Never inspects storage layout or files

Use this when working with intermediate DataFrames or derived results.

### 3. Validate a DataFrame with table context
```python
dc(df, table_name="workspace.default.my_table")
```
This hybrid mode:
* Uses the provided DataFrame for data scanning
* Uses the table name for metadata checks
* Avoids re-reading the table from storage

This is useful when the DataFrame is derived from a table, but you still want storage-level diagnostics.

## Output philosophy

dcheck is designed to be used interactively in notebooks, where fast feedback matters more than exhaustive reporting.

The output follows a few simple principles:

- **Readable first** — results are optimized for humans, not machines
- **Explicit results** — no silent failures or hidden warnings
- **Safe by default** — metadata issues never block data scans
- **Low noise** — only high-signal issues are surfaced

The goal is to make data problems obvious while you are still working, not after a pipeline has failed.

## Architecture note

dcheck uses a modular internal architecture.
The open-source distribution ships with a single built-in module
(`core_quality`) that provides the checks described above.

This design allows additional modules to be added in the future
without changing the notebook API.
