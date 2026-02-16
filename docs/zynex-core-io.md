# Zynex Core — Public I/O Contract (Authoritative)

**Contract ID:** zynex-core-io  
**Version:** 0.1.0  
**Status:** Authoritative Source of Truth (Core is the authority)

This document defines the complete public Input/Output behavior of `zynex.zx()` (`zynex.api.check`).  
Anything not explicitly specified here is an internal detail and may change.

---

## 1) Public Entry Point

Zynex exposes exactly one public callable:

```python
zx(
    source: Union[str, pyspark.sql.DataFrame],
    table_name: Optional[str] = None,
    render: bool = True,
    cache: bool = False,
    modules: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Optional[ValidationReport]
```

**Aliases:**

- `zx = check`
- `dc = check`

---

## 2) Input Contract

### 2.1 source (required)

**Type:** `Union[str, pyspark.sql.DataFrame]`

#### Case A — source: str (Spark table reference)

`source` MUST be a Spark table identifier resolvable by `spark.table(source)`.

**Common forms:**

- `"catalog.schema.table"`
- `"schema.table"`
- `"table"`

**Behavior:**

Zynex loads the table using the active SparkSession:

```python
spark = SparkSession.getActiveSession()
```

then `df = spark.table(source)`

**If SparkSession is missing:**

- Zynex prints: `Error: No active SparkSession found.`
- Returns: `None`

**If table is not found:**

- Zynex prints an error + may print "Did you mean …" suggestions (only when `schema.table` form is used and `SHOW TABLES IN schema` succeeds).
- Returns: `None`

**Important:** "table not found" is a soft-fail (printed + None), not an exception.

#### Case B — source: DataFrame

`source` MUST be a valid `pyspark.sql.DataFrame`.

**Behavior:**

- Zynex validates the DataFrame directly (no table loading).
- `table_name` (if provided) becomes metadata and enables preflight.

#### Case C — anything else

Zynex raises:

```python
ValueError("Input must be a Spark DataFrame or a table name string.")
```

---

### 2.2 table_name (optional)

**Type:** `Optional[str]`

**Meaning:**

Metadata label only, used to enable preflight when validating a DataFrame.

**Rules:**

- If `source` is a string:
  - `table_name` is ignored (the real table name is `source`).
- If `source` is a DataFrame:
  - `table_name` is used as "real_table_name" and triggers preflight if not None.

---

### 2.3 render (optional, default=True)

**Type:** `bool`

This parameter changes the return behavior.

#### If render=True (default)

Zynex prints:

- dataset summary + rule output via `render_report(...)`
- plus preflight output (if preflight runs)

**Return value:** `None`

#### If render=False

Zynex prints nothing (no final report, and preflight printing is suppressed)

**Return value:** `ValidationReport`

**This is a hard contract:** `render=True` returns `None` in current core.

---

### 2.4 cache (optional, default=False)

**Type:** `bool`

**Meaning:**

Controls DataFrame caching inside orchestrator.

**Contract:**

`cache` is merged into config as:

```python
merged_config["cache"] = cache
```

It wins over any value in `config["cache"]` because it's forced in merge.

---

### 2.5 modules (optional)

**Type:** `Optional[List[str]]`  
**Default:** `["core_quality"]`

**Selection rules:**

- Orchestrator discovers available modules at runtime.
- Selected modules are filtered:
  - unknown / missing module names are silently dropped
  - no exception is raised
- If you pass only invalid module names, the run will effectively execute nothing meaningful (but still returns a report shape).

**Current OSS module name:**

- `core_quality`

---

### 2.6 config (optional)

**Type:** `Optional[Dict[str, Any]]`  
**Default:** `{}`

**Meaning:**

Used for orchestrator/runtime behavior and module thresholds.

#### Recognized keys (v0.1.0 core)

**Orchestrator caching:**

- `cache_df`: bool (preferred)
- `cache`: bool (supported; also set by `cache=` parameter)
- `cache_storage_level`: str (default `"MEMORY_AND_DISK"`, uses `pyspark.StorageLevel.<n>` if available)

**core_quality module thresholds:**

- `extreme_values_threshold_stddev`: float (default 3.0)
  - passed into `SkewnessRule(threshold_stddev=thr)` (rule name: `"extreme_values"`)

**Unknown keys:**

- Allowed (ignored by core unless some module uses them)
- No exception is raised for unknown keys

---

## 3) Runtime Behavior

### 3.1 Preflight

Preflight is only attempted if `real_table_name` is present:

- `source` is str → `real_table_name = source` → preflight runs
- `source` is DataFrame AND `table_name` provided → preflight runs
- DataFrame without `table_name` → no preflight

**What preflight does (core_quality):**

Runs `SmallFileRule` (metadata-only, uses `DESCRIBE DETAIL <table>`)

**Preflight failure:**

- does not stop the full run
- returns an error result instead

**Render interaction:**

- If `render=True`:
  - preflight prints a mini-report first
  - then prints "Proceeding with full data scan…"
- If `render=False`:
  - preflight printing is suppressed

---

## 4) Output Contract

### 4.1 Return type

Return depends on `render`:

- `render=True` → returns `None`
- `render=False` → returns `ValidationReport`

---

### 4.2 ValidationReport (returned when render=False)

**Source of truth:** `zynex.core.report.ValidationReport`

```python
@dataclass
class ValidationReport:
    rows: int
    columns: int
    column_names: List[str]
    results: List[RuleResult]
```

---

### 4.3 RuleResult

```python
@dataclass
class RuleResult:
    name: str
    status: str
    metrics: Dict[str, Any]
    message: str
```

---

### 4.4 Status domain (strict)

At the adapter boundary (`report_to_validation_report`), statuses are normalized to:

**Allowed values:**

- `"ok"`
- `"warning"`
- `"error"`
- `"skipped"`
- `"not_applicable"`

**If a module emits any other status:**

- it is normalized to `"error"`
- message is prefixed to make this visible

---

## 5) Side-Effect Contract (Core)

**Zynex core will:**

- Read data (Spark actions will occur)
- Optionally persist/unpersist the DataFrame in-session (best effort)

**Zynex core will NOT:**

- write tables
- mutate schemas
- alter catalog objects
- permanently persist data

---

## 6) Failure Contract (Core)

### Hard failures (exceptions)

- Invalid source type → `ValueError(...)`
- If `ExecutionContext` cannot compute rowcount because `df.count()` is missing → `TypeError(...)` (rare; non-Spark df)

### Soft failures (printed + returns None)

- No active SparkSession when source is string
- Table not found / cannot be loaded when source is string

**Validation findings:**

- NEVER throw exceptions (they become `RuleResult.status`)

---

## 7) Non-Guarantees (protects future refactors)

Core does NOT guarantee:

- rule execution order
- presence/naming of specific keys inside `metrics` (except what a specific rule chooses to emit)
- that unknown modules will be rejected (currently they are ignored)
- stability of internal orchestrator types (`zynex.common.types.Report`) — only `ValidationReport` is the stable return for `render=False`

---

## 8) Canonical Examples

```python
# 1) Notebook / interactive (prints, returns None)
zx("catalog.schema.table")

# 2) Programmatic usage (silent, returns ValidationReport)
report = zx("catalog.schema.table", render=False)
print(report.rows, report.columns)

# 3) DataFrame usage (no preflight unless table_name is provided)
report = zx(df, render=False)

# 4) DataFrame + preflight enabled
report = zx(df, table_name="catalog.schema.table", render=False)

# 5) Custom threshold
report = zx("catalog.schema.table", render=False, config={"extreme_values_threshold_stddev": 2.0})

# 6) Cache during run
report = zx("catalog.schema.table", render=False, cache=True)
```
