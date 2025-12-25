from dataclasses import dataclass, field
from typing import Any, Dict, List

@dataclass
class RuleResult:
    name: str
    status: str
    metrics: Dict[str, Any]
    message: str

@dataclass
class ValidationReport:
    rows: int
    columns: int
    column_names: List[str]
    results: List[RuleResult] = field(default_factory=list)

    def has_warnings(self) -> bool:
        return any((r.status or "").lower() == "warning" for r in self.results)

    def has_errors(self) -> bool:
        return any((r.status or "").lower() == "error" for r in self.results)

    def summary(self):
        return {
            "rows": self.rows,
            "columns": self.columns,
            "rules_run": len(self.results),
            "warnings": sum(r.status == "warning" for r in self.results),
            "errors": sum(r.status == "error" for r in self.results),
        }

def render_report(report: ValidationReport, show_summary: bool = True):
    print("=" * 60)
    print("DCHECK REPORT")
    print(f"Rows    : {report.rows}")
    print(f"Columns : {report.columns}")
    print(f"Rules   : {len(report.results)}")
    print("=" * 60)
    print()

    status_count = {"ok": 0, "warning": 0, "error": 0}
    total_cells = report.rows * report.columns

    def fmt(n, decimals=0):
        try:
            if isinstance(n, float):
                return f"{n:,.{decimals}f}".replace(",", " ")
            else:
                return f"{int(n):,}".replace(",", " ")
        except Exception:
            return n

    # Render preflight first (small_files), then the rest.
    results = list(report.results)
    preflight = [r for r in results if r.name == "small_files"]
    others = [r for r in results if r.name != "small_files"]
    ordered = preflight + others

    def print_preflight_banner_if_needed(r: RuleResult):
        metrics = r.metrics or {}
        rating = metrics.get("rating")

        if rating == "high_risk":
            print("PRE-FLIGHT NOTICE: Small file density is high risk")
            print(
                "A high number of files relative to dataset size typically increases planning overhead, "
                "task scheduling costs, and slows full-table scans."
            )
            rec = metrics.get("recommendation")
            if rec:
                print(f"Suggested action: {rec}")
            print()

    for result in ordered:
        status = (result.status or "").lower()
        if status not in status_count:
            status_count[status] = 0
        status_count[status] += 1

        print(f"[RULE] {result.name}")
        print(f"Status : {result.status}")
        print(f"Message: {result.message}")
        print()

        metrics = result.metrics or {}

        # --------------------------------------------------
        # DUPLICATE FULL ROWS — percent of dataset rows
        # --------------------------------------------------
        if result.name == "duplicate_rows":
            total_rows = report.rows
            dup = metrics.get("duplicate_rows", 0)
            uniq = metrics.get("unique_rows", 0)

            dup_pct = round((dup / total_rows) * 100, 3) if total_rows else 0
            uniq_pct = round((uniq / total_rows) * 100, 3) if total_rows else 0

            print("Total metrics:")
            print(f"  - unique_rows    : {fmt(uniq)} ({fmt(uniq_pct, 2)}%)")
            print(f"  - duplicate_rows : {fmt(dup)} ({fmt(dup_pct, 2)}%)")

        # --------------------------------------------------
        # NULL + IQR — total % of all cells; per-column % of rows
        # --------------------------------------------------
        elif result.name in ("null_ratio", "iqr_outliers"):
            total_key = "total_nulls" if result.name == "null_ratio" else "total_outliers"
            total_val = metrics.get(total_key, 0)
            pct = round((total_val / total_cells) * 100, 4) if total_cells else 0

            print("Total metrics:")
            print(f"  - {total_key} : {fmt(total_val)} ({fmt(pct, 2)}% of all values)")

            per_column = metrics.get("per_column")
            if isinstance(per_column, dict) and per_column:
                print()
                print("Per column:")
                for col, col_metrics in per_column.items():
                    val = col_metrics.get("nulls") or col_metrics.get("outliers", 0)
                    col_pct = round((val / report.rows) * 100, 4) if report.rows else 0
                    print(f"  - {col} : {fmt(val)} ({fmt(col_pct, 2)}% of rows)")

        # --------------------------------------------------
        # SMALL FILES — dataset-level only
        # --------------------------------------------------
        elif result.name == "small_files":
            print_preflight_banner_if_needed(result)
            print("Total metrics:")

            key_order = [
                "rating",
                "num_files",
                "total_size_gb",
                "avg_file_size_mb",
                "files_per_gb",
                "recommendation",
            ]
            for k in key_order:
                if k in metrics:
                    v = metrics[k]
                    if isinstance(v, (int, float)):
                        print(f"  - {k} : {fmt(v, 6)}")
                    else:
                        print(f"  - {k} : {v}")

            for key, value in metrics.items():
                if key in key_order or key == "per_column":
                    continue
                print(f"  - {key} : {fmt(value, 6)}")

        # --------------------------------------------------
        # FALLBACK — future rules
        # --------------------------------------------------
        else:
            print("Total metrics:")
            for key, value in metrics.items():
                if key != "per_column":
                    print(f"  - {key} : {fmt(value, 6)}")

        print()
        print("-" * 60)
        print()

    if show_summary:
        print("=" * 60)
        print(
            f"Summary: "
            f"ok={fmt(status_count['ok'])} | "
            f"warning={fmt(status_count['warning'])} | "
            f"error={fmt(status_count['error'])}"
        )
        print("=" * 60)
