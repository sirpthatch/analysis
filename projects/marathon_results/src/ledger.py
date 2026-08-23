"""Filter-provenance ledger.

`research.md` asks that every operation which drops rows record what it dropped and
why, so the surviving sample can be audited for bias. Every stage instantiates one
Ledger, calls `record` around each filter, and writes it beside its output.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


class Ledger:
    def __init__(self, stage: str):
        self.stage = stage
        self.rows: list[dict] = []

    def record(self, filt: str, rows_in: int, rows_out: int, reason: str = "") -> None:
        self.rows.append({
            "stage": self.stage,
            "filter": filt,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_dropped": rows_in - rows_out,
            "pct_dropped": round(100.0 * (rows_in - rows_out) / rows_in, 3) if rows_in else 0.0,
            "reason": reason,
        })

    def apply(self, df: pd.DataFrame, mask: pd.Series, filt: str, reason: str = "") -> pd.DataFrame:
        """Apply a boolean mask and record the drop in one step."""
        out = df[mask]
        self.record(filt, len(df), len(out), reason)
        return out

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.to_frame().to_csv(path, index=False)
        print(f"  ledger -> {path}")

    def summary(self) -> str:
        if not self.rows:
            return "(no filters recorded)"
        w = max(len(r["filter"]) for r in self.rows)
        lines = [f"{'filter':<{w}}  {'in':>12}  {'out':>12}  {'dropped':>12}  {'%':>7}"]
        for r in self.rows:
            lines.append(
                f"{r['filter']:<{w}}  {r['rows_in']:>12,}  {r['rows_out']:>12,}"
                f"  {r['rows_dropped']:>12,}  {r['pct_dropped']:>6.2f}%"
            )
        return "\n".join(lines)
