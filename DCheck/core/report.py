from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class RuleResult:
    name: str
    status: str
    metrics: Dict[str, float]
    message: str

@dataclass
class ValidationReport:
    rows: int
    columns: int
    column_names: List[str]
    results: List[RuleResult] = field(default_factory=list)

    def summary(self):
        return {
            "rows": self.rows,
            "columns": self.columns,
            "rules_run": len(self.results),
            "warnings": sum(r.status == "warning" for r in self.results),
            "errors": sum(r.status == "error" for r in self.results),
        }
