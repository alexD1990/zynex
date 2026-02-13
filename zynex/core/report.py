from __future__ import annotations

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

    def summary(self) -> Dict[str, Any]:
        return {
            "rows": self.rows,
            "columns": self.columns,
            "rules_run": len(self.results),
            "warnings": sum((r.status or "").lower() == "warning" for r in self.results),
            "errors": sum((r.status or "").lower() == "error" for r in self.results),
        }

    def __repr__(self) -> str:
        s = self.summary()
        return f"<ValidationReport: {s['rows']} rows | {s['errors']} Errors | {s['warnings']} Warnings>"

    def __str__(self) -> str:
        return self.__repr__()


class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def render_report(report: ValidationReport, verbose: bool = False, print_header: bool = True) -> None:
    """
    Renders a CLI-friendly validation report with ANSI color coding and structured layout.
    """

    def print_box(rule_name: str, status: str, lines: List[str], width: int = 80) -> None:
        status_l = (status or "").lower()
        if status_l == "error":
            c = Colors.FAIL
            lbl = "[ERROR]"
        elif status_l == "warning":
            c = Colors.WARNING
            lbl = "[WARNING]"
        elif status_l == "skipped":
            c = Colors.CYAN
            lbl = "[SKIPPED]"
        elif status_l == "not_applicable":
            c = Colors.CYAN
            lbl = "[N/A]"
        else:
            c = Colors.GREEN
            lbl = "[OK]"

        header_text = f" {lbl} {rule_name} "
        dash_len = width - len(header_text) - 3
        if dash_len < 0:
            dash_len = 0

        print(f"{c}┌──{Colors.BOLD}{header_text}{Colors.ENDC}{c}{'─' * dash_len}┐{Colors.ENDC}")
        for line in lines:
            print(f"{c}│{Colors.ENDC}  {line}")
        print(f"{c}└{'─' * width}┘{Colors.ENDC}")
        print()

    def fmt(n: Any, decimals: int = 0) -> Any:
        if n is None:
            return "-"
        if isinstance(n, bool):
            return str(n)
        try:
            if isinstance(n, float):
                return f"{n:,.{decimals}f}".replace(",", " ")
            return f"{int(n):,}".replace(",", " ")
        except Exception:
            return n

    def _trim(s: str, n: int = 48) -> str:
        s = str(s)
        return s if len(s) <= n else s[: n - 1] + "…"

    # 1) SUMMARY HEADER
    if print_header:
        status_count = {"ok": 0, "warning": 0, "error": 0}
        for r in report.results:
            s = (r.status or "").lower()
            if s in status_count:
                status_count[s] += 1

        err_str = (
            f"{Colors.FAIL}{status_count['error']} Errors{Colors.ENDC}"
            if status_count["error"] > 0
            else "0 Errors"
        )
        warn_str = (
            f"{Colors.WARNING}{status_count['warning']} Warnings{Colors.ENDC}"
            if status_count["warning"] > 0
            else "0 Warnings"
        )
        ok_str = f"{Colors.GREEN}{status_count['ok']} OK{Colors.ENDC}"

        print()
        print(f"{Colors.BOLD}ZYNEX REPORT{Colors.ENDC}")
        print(f"Dataset: {fmt(report.rows)} rows x {report.columns} columns | {err_str} | {warn_str} | {ok_str}")
        print()

    # 2) ORDER RESULTS (preflight first)
    results = list(report.results)
    preflight = [r for r in results if r.name == "small_files"]
    others = [r for r in results if r.name != "small_files"]
    ordered = preflight + others

    for result in ordered:
        status = (result.status or "").lower()
        metrics = result.metrics or {}

        # Collapse OK results unless verbose
        if status == "ok" and not verbose:
            print(f"{Colors.GREEN}[OK] {result.name}{Colors.ENDC}")
            continue
        if status == "skipped" and not verbose:
            print(f"{Colors.CYAN}[SKIPPED] {result.name}{Colors.ENDC}")
            continue
        if status == "not_applicable" and not verbose:
            print(f"{Colors.CYAN}[N/A] {result.name}{Colors.ENDC}")
            continue

        lines: List[str] = []
        lines.append(f"{Colors.BOLD}Message:{Colors.ENDC} {result.message}")
        lines.append("")

        if result.name == "duplicate_rows":
            dup = metrics.get("duplicate_rows", 0)
            dup_pct = round((dup / report.rows) * 100, 3) if report.rows else 0
            lines.append(f"• Duplicates : {fmt(dup)} rows ({fmt(dup_pct, 2)}%)")

        elif result.name == "null_ratio":
            total_val = metrics.get("total_nulls", 0)
            denom = (report.rows * report.columns) if (report.rows and report.columns) else 0
            pct = round((total_val / denom) * 100, 2) if denom else 0
            lines.append(f"• Total Nulls : {fmt(total_val)} ({fmt(pct, 2)}%)")

            per_col = metrics.get("per_column", {})
            if per_col:
                lines.append("• Columns with nulls:")
                for c, v in per_col.items():
                    val = v.get("nulls", 0)
                    cpct = round((val / report.rows) * 100, 2) if report.rows else 0
                    lines.append(f"    - {c.ljust(15)} : {fmt(val)} ({cpct}%)")

        elif result.name == "extreme_values":
            flagged = metrics.get("flagged_columns", {})
            if flagged:
                thr = metrics.get("threshold_stddev", 3.0)
                lines.append(f"• Extreme deviations (>{thr} stddev):")
                for c, s in flagged.items():
                    sigma = s.get("max_sigma", 0)
                    lines.append(f"    - {Colors.BOLD}{c}{Colors.ENDC} ({sigma}x sigma)")
                    lines.append(f"      Range : {fmt(s.get('min'))} ... {fmt(s.get('max'))}")
                    lines.append(f"      Avg   : {fmt(s.get('avg'))} (std: {fmt(s.get('std'))})")
                    lines.append("")

        elif result.name == "pii_scan":
            flagged_cols = metrics.get("flagged_columns") or []
            rows_scanned = metrics.get("rows_scanned")
            cols_scanned = metrics.get("columns_scanned")
            flagged_count = metrics.get("flagged_count")

            lines.append(f"• Flagged columns : {', '.join(flagged_cols) if flagged_cols else '-'}")
            if flagged_count is not None:
                lines.append(f"• Flagged count   : {fmt(flagged_count)}")
            if rows_scanned is not None:
                lines.append(f"• Rows scanned    : {fmt(rows_scanned)}")
            if cols_scanned is not None:
                lines.append(f"• Cols scanned    : {fmt(cols_scanned)}")

            masked = metrics.get("masked_samples") or {}
            if masked:
                lines.append("")
                lines.append("• Findings (masked samples):")

                for col, findings in masked.items():
                    if not findings:
                        continue

                    parts: List[str] = []
                    for f in findings:
                        pii_type = f.get("pii_type", "?")
                        match_count = f.get("match_count", 0)
                        sample_list = f.get("sample") or []
                        sample_list = [_trim(x) for x in sample_list[:3]]
                        sample_txt = ", ".join(sample_list) if sample_list else "-"
                        parts.append(f"{pii_type} ({match_count}) samples: {sample_txt}")

                    lines.append(f"    - {Colors.BOLD}{col}{Colors.ENDC}: " + " | ".join(parts))

        elif result.name == "small_files":
            rating = (metrics.get("rating", "unknown") or "unknown").upper()
            target = metrics.get("target")
            reason = metrics.get("reason")

            num_files = metrics.get("num_files")
            total_size_gb = metrics.get("total_size_gb")
            avg_mb = metrics.get("avg_file_size_mb", 0)

            lines.append(f"• Rating       : {rating}")

            #  point 2: show context when metadata fails / target is wrong
            if target:
                lines.append(f"• Target       : {target}")
            if reason:
                lines.append(f"• Reason       : {str(reason).upper()}")

            #  point 1: avoid fake "0 MB" when not applicable and we have no real size metrics
            is_na = rating == "NOT_APPLICABLE"
            has_any_size_metric = (num_files is not None) or (total_size_gb is not None)

            if num_files is not None:
                lines.append(f"• Num Files    : {fmt(num_files)}")

            if total_size_gb is not None:
                try:
                    total_gb = float(total_size_gb)
                    if total_gb >= 1:
                        lines.append(f"• Total Size   : {fmt(total_gb, 2)} GB")
                    else:
                        lines.append(f"• Total Size   : {fmt(total_gb * 1024, 2)} MB")
                except Exception:
                    lines.append(f"• Total Size   : {total_size_gb}")

            if not (is_na and not has_any_size_metric):
                lines.append(f"• Avg File Size: {fmt(avg_mb, 2)} MB")

            rec = metrics.get("recommendation")
            if rec:
                lines.append("")
                lines.append(f"Recommendation: {rec}")

        else:
            for k, v in metrics.items():
                if k != "per_column":
                    lines.append(f"• {k}: {v}")

        print_box(result.name, status, lines)
