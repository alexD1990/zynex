# Zynex Core — I/O Contract

**Contract ID:** zynex-core-io  
**Version:** 0.1.0  
**Status:** Frozen — changes require version bump  
**Audience:** Consumers building on top of Zynex (runners, pipelines, integrations)

---

## 1) Public Entry Point

```python
from zynex import zx, check, dc   # zx = check = dc

check(
    source: Union[str, DataFrame],
    table_name: Optional[str] = None,
    render: bool = True,
    cache: bool = False,
    modules: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Optional[ValidationReport]
```

---

## 2) Input

### `source` (required)

| Type | Behavior |
|---|---|
| `str` | Spark table identifier — loaded via active SparkSession |
| `DataFrame` | Validated directly |
| anything else | Raises `ValueError("Input must be a Spark DataFrame or a table name string.")` |

Valid string forms: `"table"`, `"schema.table"`, `"catalog.schema.table"`

### `table_name` (optional)

Only meaningful when `source` is a DataFrame. Provides a table name for preflight.  
Ignored when `source` is a string (string itself is the table name).

### `render` (optional, default=`True`)

Controls return value and printing. See Output section.

### `cache` (optional, default=`False`)

Enables DataFrame caching during the run. Always overrides `config["cache"]`.

### `modules` (optional, default=`["core_quality"]`)

List of module names to run. Unknown names are silently dropped.  
Available OSS module: `"core_quality"`

### `config` (optional, default=`{}`)

| Key | Type | Default | Description |
|---|---|---|---|
| `cache_df` | bool | False | Enable DataFrame caching (preferred key) |
| `cache` | bool | False | Enable DataFrame caching (alias) |
| `cache_storage_level` | str | `"MEMORY_AND_DISK"` | Spark StorageLevel name |
| `extreme_values_threshold_stddev` | float | `3.0` | Z-score threshold for extreme value detection |

Unknown keys are ignored. No exception is raised.

---

## 3) Output

### Return value

| `render` | Return type |
|---|---|
| `True` (default) | `None` — report is printed to stdout |
| `False` | `ValidationReport` |

### `ValidationReport`

```python
@dataclass
class ValidationReport:
    rows: int
    columns: int
    column_names: List[str]
    results: List[RuleResult]
```

### `RuleResult`

```python
@dataclass
class RuleResult:
    name: str
    status: str
    metrics: Dict[str, Any]
    message: str
```

### Status domain (strict)

Allowed values: `"ok"` `"warning"` `"error"` `"skipped"` `"not_applicable"`

Any other status from a module is normalized to `"error"`.

---

## 4) Failure Behavior

### Hard failures (exceptions — caller must handle)

| Condition | Exception |
|---|---|
| `source` is not `str` or `DataFrame` | `ValueError` |

### Soft failures (returns `None`, prints error)

| Condition | Behavior |
|---|---|
| No active SparkSession | Prints error, returns `None` |
| Table not found | Prints error + suggestions, returns `None` |

### Validation findings

Rules **never** raise exceptions. All findings are captured as `RuleResult.status`.

---

## 5) Side Effects

Zynex **will:**
- Execute Spark actions (read data)
- Optionally cache/uncache the DataFrame within the session

Zynex **will not:**
- Write tables
- Mutate schemas
- Alter catalog objects
- Permanently persist data

---

## 6) Non-Guarantees

Zynex does **not** guarantee:
- Rule execution order
- Stability of `metrics` key names inside `RuleResult` (except as documented per rule)
- Rejection of unknown module names (they are silently dropped)
- Stability of internal types — only `ValidationReport` and `RuleResult` are stable

---

## 7) Canonical Examples

```python
# Interactive — prints report, returns None
zx("catalog.schema.table")

# Programmatic — silent, returns ValidationReport
report = zx("catalog.schema.table", render=False)

# DataFrame — no preflight
report = zx(df, render=False)

# DataFrame + preflight
report = zx(df, table_name="catalog.schema.table", render=False)

# Custom threshold
report = zx("catalog.schema.table", render=False,
            config={"extreme_values_threshold_stddev": 2.0})

# With caching
report = zx("catalog.schema.table", render=False, cache=True)
```