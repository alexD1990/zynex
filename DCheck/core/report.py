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

# ANSI color codes for terminal/Databricks output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def render_report(report: ValidationReport, verbose: bool = False):
    """
    Renders a CLI-friendly validation report with ANSI color coding and structured layout.
    """
    
    # --- HELPER: DRAW BOX (Style 2A - Status embedded in Border) ---
    def print_box(rule_name, status, lines, width=80):
        # Determine color and label based on status
        if status == "error":
            c = Colors.FAIL
            lbl = "[ERROR]"
        elif status == "warning":
            c = Colors.WARNING
            lbl = "[WARNING]"
        else:
            c = Colors.GREEN
            lbl = "[OK]"
        
        # Header construction: ┌── [STATUS] rule_name ──...──┐
        header_text = f" {lbl} {rule_name} "
        dash_len = width - len(header_text) - 3 # 3 accounts for '┌──'
        if dash_len < 0: dash_len = 0
        
        # Print top border
        print(f"{c}┌──{Colors.BOLD}{header_text}{Colors.ENDC}{c}{'─' * dash_len}┐{Colors.ENDC}")
        
        # Print content lines
        for line in lines:
            print(f"{c}│{Colors.ENDC}  {line}")

        # Print bottom border
        print(f"{c}└{'─' * (width)}┘{Colors.ENDC}")
        print()
    # ------------------------------------------------------

    # 1. SUMMARY HEADER
    status_count = {"ok": 0, "warning": 0, "error": 0}
    for r in report.results:
        s = (r.status or "").lower()
        if s in status_count: status_count[s] += 1
    
    def fmt(n, decimals=0):
        try:
            if isinstance(n, float):
                return f"{n:,.{decimals}f}".replace(",", " ")
            else:
                return f"{int(n):,}".replace(",", " ")
        except Exception:
            return n

    err_str = f"{Colors.FAIL}{status_count['error']} Errors{Colors.ENDC}" if status_count['error'] > 0 else "0 Errors"
    warn_str = f"{Colors.WARNING}{status_count['warning']} Warnings{Colors.ENDC}" if status_count['warning'] > 0 else "0 Warnings"
    ok_str = f"{Colors.GREEN}{status_count['ok']} OK{Colors.ENDC}"

    print()
    print(f"{Colors.BOLD}DCHECK REPORT{Colors.ENDC}")
    # UPDATED LINE: Added 'x {report.columns} columns'
    print(f"Dataset: {fmt(report.rows)} rows x {report.columns} columns | {err_str} | {warn_str} | {ok_str}")
    print()

    # Sort: Run pre-flight checks (small_files) first
    results = list(report.results)
    preflight = [r for r in results if r.name == "small_files"]
    others = [r for r in results if r.name != "small_files"]
    ordered = preflight + others

    for result in ordered:
        status = (result.status or "").lower()
        metrics = result.metrics or {}
        
        # UX: Collapse 'OK' results to a single line unless verbose mode is active
        if status == "ok" and not verbose:
            print(f"{Colors.GREEN}[OK] {result.name}{Colors.ENDC}")
            continue

        # Build content lines for the details box
        lines = []
        lines.append(f"{Colors.BOLD}Message:{Colors.ENDC} {result.message}")
        lines.append("") # Spacer line

        # --- RULE SPECIFIC FORMATTING ---

        # 1. DUPLICATE ROWS
        if result.name == "duplicate_rows":
            dup = metrics.get("duplicate_rows", 0)
            dup_pct = round((dup / report.rows) * 100, 3) if report.rows else 0
            lines.append(f"• Duplicates : {fmt(dup)} rows ({fmt(dup_pct, 2)}%)")

        # 2. NULL RATIO
        elif result.name == "null_ratio":
            total_val = metrics.get("total_nulls", 0)
            pct = round((total_val / (report.rows * report.columns)) * 100, 2)
            lines.append(f"• Total Nulls : {fmt(total_val)} ({fmt(pct, 2)}%)")
            
            per_col = metrics.get("per_column", {})
            if per_col:
                lines.append(f"• Columns with nulls:")
                for c, v in per_col.items():
                    val = v.get("nulls", 0)
                    cpct = round((val/report.rows)*100, 2)
                    lines.append(f"    - {c.ljust(15)} : {fmt(val)} ({cpct}%)")

        # 3. EXTREME VALUES (SKEWNESS)
        elif result.name == "extreme_values":
            flagged = metrics.get("flagged_columns", {})
            if flagged:
                lines.append(f"• Extreme deviations (>5 stddev):")
                for c, s in flagged.items():
                    sigma = s.get("max_sigma", 0)
                    lines.append(f"    - {Colors.BOLD}{c}{Colors.ENDC} ({sigma}x sigma)")
                    lines.append(f"      Range : {fmt(s.get('min'))} ... {fmt(s.get('max'))}")
                    lines.append(f"      Avg   : {fmt(s.get('avg'))} (std: {fmt(s.get('std'))})")
                    lines.append("") # Spacer

        # 4. SMALL FILES
        elif result.name == "small_files":
            rating = metrics.get("rating", "unknown").upper()
            lines.append(f"• Rating       : {rating}")
            lines.append(f"• Avg File Size: {fmt(metrics.get('avg_file_size_mb',0), 2)} MB")
            
            rec = metrics.get("recommendation")
            if rec and rating != "OPTIMAL":
                lines.append("")
                lines.append(f"Recommendation: {rec}")

        # 5. FALLBACK (Generic rules)
        else:
            for k, v in metrics.items():
                if k != "per_column": 
                    lines.append(f"• {k}: {v}")

        # Render the box
        print_box(result.name, status, lines)