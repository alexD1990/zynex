# zynex/common/types.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


@dataclass
class ExecutionContext:
    """
    Shared context passed to modules.

    Design goals:
    - Keep this import-light (no mandatory pyspark imports at import-time).
    - Provide lazy, memoized rowcount (Spark action) so we never count twice.
    - Optional DataFrame caching controlled by config for multi-rule workloads.

    Supported config keys:
      - cache_df: bool (preferred)
      - cache: bool (backward compatible)
      - cache_storage_level: str (e.g. 'MEMORY_AND_DISK')
    """
    df: Any
    table_name: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)

    _rows: Optional[int] = field(default=None, init=False, repr=False)
    _is_persisted: bool = field(default=False, init=False, repr=False)

    def _should_cache_df(self) -> bool:
        cfg = self.config or {}
        # Backward compatible alias: 'cache'
        return bool(cfg.get("cache_df", False) or cfg.get("cache", False))

    def ensure_persisted(self) -> None:
        """
        Persist the DataFrame once if caching is enabled.
        Uses dynamic import to avoid hard pyspark dependency at import-time.
        """
        if self._is_persisted or not self._should_cache_df():
            return

        persist = getattr(self.df, "persist", None)
        if persist is None:
            return

        storage_level_name = (self.config or {}).get(
            "cache_storage_level", "MEMORY_AND_DISK"
        )

        try:
            from pyspark import StorageLevel  # type: ignore
            storage_level = getattr(
                StorageLevel,
                storage_level_name,
                StorageLevel.MEMORY_AND_DISK,
            )
            persist(storage_level)
        except Exception:
            # If persist fails (e.g., serverless not supporting cache),
            # silently skip caching.
            return

        self._is_persisted = True

    def unpersist_if_needed(self) -> None:
        """
        Best-effort unpersist if we persisted earlier.
        """
        if not self._is_persisted:
            return
        unpersist = getattr(self.df, "unpersist", None)
        if unpersist is None:
            return
        try:
            unpersist()
        finally:
            self._is_persisted = False

    @property
    def rows(self) -> int:
        """
        Lazy + memoized rowcount. First call triggers one Spark job.
        Subsequent calls reuse the cached integer in-memory.
        """
        if self._rows is None:
            # Persist before first action if enabled
            self.ensure_persisted()
            count = getattr(self.df, "count", None)
            if count is None:
                raise TypeError("ExecutionContext.df does not support count()")
            self._rows = int(count())
        return self._rows


class CheckStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    ERROR = "error"
    SKIPPED = "skipped"


@dataclass
class CheckResult:
    """
    Standard result format across all modules.
    """
    check_id: str
    status: str
    message: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    module_name: str = ""


@dataclass
class Report:
    """
    Aggregated results grouped by module (not mixed).
    """
    rows: int
    columns: int
    column_names: List[str]
    results_by_module: Dict[str, List[CheckResult]] = field(default_factory=dict)

    def all_results_flat(self) -> List[CheckResult]:
        out: List[CheckResult] = []
        for _, rs in self.results_by_module.items():
            out.extend(rs)
        return out