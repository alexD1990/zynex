# Zynex Core — Internals

**Version:** 0.1.0  
**Audience:** Zynex maintainers and module developers

---

## 1) Architecture Overview

```
zx() / check()          ← public entry point (api.py)
    │
    ├── Input resolution (str → DataFrame, validation)
    ├── Orchestrator (engine.py)
    │       ├── discover_modules() → registry.py
    │       ├── ExecutionContext (types.py)
    │       ├── preflight (per module, optional)
    │       ├── run (per module)
    │       └── ResultAggregator → Report
    │
    └── report_to_validation_report() ← adapters.py
            └── render_report() or return ValidationReport
```

---

## 2) Entry Point (`api.py`)

`check()` / `zx()` / `dc()` are identical aliases.

**Responsibilities:**
- Resolves `source` (string → `spark.table()`, or DataFrame passthrough)
- Handles soft failures (no SparkSession, table not found) — prints and returns `None`
- Builds `merged_config` (`cache` parameter always wins over `config["cache"]`)
- Defines `_on_preflight` callback for live UI feedback
- Calls `run_orchestrator()`, then `report_to_validation_report()`
- If `render=True`: calls `render_report()`, filters `small_files` from final report (already shown in preflight), returns `None`
- If `render=False`: returns `ValidationReport`

**Table-not-found logic (str source only):**
- For `schema.table` form: pre-checks via `SHOW TABLES IN schema` before attempting `spark.table()`
- Falls back to catching `TABLE_OR_VIEW_NOT_FOUND` exceptions
- Offers fuzzy name suggestions via `difflib.get_close_matches`

---

## 3) Orchestrator (`orchestrator/engine.py`)

**Responsibilities:**
- Calls `discover_modules()` to get available modules
- Filters selected modules against available (unknown names silently dropped)
- Creates `ExecutionContext` and calls `ensure_persisted()` once
- For each module: runs `preflight()` then `run()`
- Module crashes are caught — never propagate to caller
- Calls `unpersist_if_needed()` in `finally` block

**Rowcount:** computed once via `ctx.rows` (lazy, memoized). All modules reuse the same integer.

---

## 4) Module Registry (`orchestrator/registry.py`)

Two-step discovery:

1. **Built-ins:** `CoreModule` (hardcoded)
2. **Plugins:** discovered via Python entry points group `zynex.modules`

Plugin entry point format:
```toml
[project.entry-points."zynex.modules"]
gdpr = "my_package.module:GDPRModule"
```

**Built-ins always win** — a plugin cannot shadow `core_quality`.  
Plugin load failures are silently ignored (fail-open).

---

## 5) Module Interface (`common/interfaces.py`)

All modules must implement `DCheckModule`:

```python
class DCheckModule(ABC):
    @property
    def name(self) -> str: ...          # unique key, e.g. "core_quality"

    def preflight(self, ctx) -> Optional[CheckResult]: ...   # optional
    def run(self, ctx) -> List[CheckResult]: ...             # required
```

- `preflight` runs only when `table_name` is present
- `run` always runs (for selected modules)
- Both receive `ExecutionContext`

---

## 6) ExecutionContext (`common/types.py`)

Passed to every module. Contains:

| Attribute | Type | Description |
|---|---|---|
| `df` | DataFrame | The Spark DataFrame to validate |
| `table_name` | Optional[str] | Real table name if available |
| `config` | Dict | Merged config from caller |
| `rows` | int (property) | Lazy memoized rowcount |

**Caching:** controlled by `config["cache_df"]` or `config["cache"]`. Uses `pyspark.StorageLevel` (default `MEMORY_AND_DISK`). Silently skipped if persist fails (e.g. serverless).

---

## 7) core_quality Module (`modules/core_quality/module.py`)

**Preflight:**
- Runs `SmallFileRule` — metadata-only via `DESCRIBE DETAIL`

**Full run (3 rules):**

| Rule | Check | Spark jobs |
|---|---|---|
| `DuplicateRowRule` | Full-row duplicates via `dropDuplicates().count()` | 1 |
| `NullRatioRule` | Null counts per column | 1 (single aggregation) |
| `SkewnessRule` | Extreme values via Z-score on numeric columns | 1 (single aggregation) |

`SkewnessRule` threshold default: `3.0` stddev (configurable via `config["extreme_values_threshold_stddev"]`).  
Note: default in `SkewnessRule.__init__` is `5.0`, but `CoreModule.run()` passes `3.0` unless overridden.

---

## 8) Adapters (`orchestrator/adapters.py`)

`report_to_validation_report()` converts internal `Report` → public `ValidationReport`:

- Strips `rowcount` pseudo-check from output
- Normalizes `check_id` from `module.check` → `check`
- Enforces status domain: any status not in `{ok, warning, error, skipped, not_applicable}` → `"error"` with message prefix

---

## 9) Internal Types

| Type | Location | Purpose |
|---|---|---|
| `CheckResult` | `common/types.py` | Per-check result inside modules |
| `Report` | `common/types.py` | Module-grouped results (internal) |
| `RuleResult` | `core/report.py` | Legacy rule output (used by rules layer) |
| `ValidationReport` | `core/report.py` | Public output type |

`RuleResult` → `CheckResult` conversion happens in `CoreModule._to_check_result()`.

---

## 10) Render (`core/report.py`)

`render_report()` prints ANSI-colored CLI output. Not part of public I/O contract.

- `print_header=False` suppresses dataset summary (used for preflight mini-report)
- `verbose=False` collapses OK/skipped/not_applicable to single-line output
- Result ordering: `small_files` always printed first (preflight), then remaining rules