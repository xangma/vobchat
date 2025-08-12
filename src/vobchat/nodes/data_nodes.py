"""Data retrieval nodes: find_cubes_node.

Fetches statistical cube rows for the currently selected theme and units,
reusing cached rows when possible and emitting an interrupt so the UI can
render visualizations immediately. The implementation is careful to only
update specific fields and to pass large payloads through the interrupt.
"""

# data_nodes.py – lean data‑cube retrieval for LangGraph
# =======================================================
# Public node exported:
#     • **FindCubes_node** – fetches / caches statistical cubes for the
#       currently‑selected theme and geographic units.
#
#   – Reuses any already‑stored rows that match the current theme / year range.
#   – Fetches only the missing bits via `find_cubes_for_unit_theme()`.
#   – Applies optional `min_year` / `max_year` filters **once** in a helper.
#   – Emits an interrupt so the front‑end can visualise straight away.
#
# Requires helpers from the wider app: `_append_ai`, `interrupt`, `get_selected_units`,
# and `find_cubes_for_unit_theme`.
# =======================================================

from __future__ import annotations

import io
import logging
from typing import Dict, List, Union

import pandas as pd
# type: ignore – provided by langgraph
from langgraph.types import Command, interrupt

from vobchat.state_schema import lg_State, get_selected_units
from vobchat.tools import find_cubes_for_unit_theme
from .utils import _append_ai

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Helper – filter a cube DataFrame by year range (non‑destructive)
# -----------------------------------------------------------------------------


def _year_filter(df: pd.DataFrame, *, min_year: int | None, max_year: int | None) -> pd.DataFrame:
    """Return a copy of df filtered to a year range.

    Args:
        df: DataFrame with numeric ``Start`` and ``End`` columns.
        min_year: Minimum acceptable ``End`` (inclusive). If None, no lower bound.
        max_year: Maximum acceptable ``Start`` (inclusive). If None, no upper bound.

    Returns:
        pd.DataFrame: A filtered copy if possible; otherwise the original ``df``.
    """
    if df.empty or {"Start", "End"}.issubset(df.columns) is False:
        return df  # cannot filter – keep as‑is
    out = df.copy()
    out["Start"] = pd.to_numeric(out["Start"], errors="coerce")
    out["End"] = pd.to_numeric(out["End"], errors="coerce")
    if min_year is not None:
        out = out[out["End"] >= min_year]
    if max_year is not None:
        out = out[out["Start"] <= max_year]
    return out

# -----------------------------------------------------------------------------
# Node – FindCubes_node
# -----------------------------------------------------------------------------


def find_cubes_node(state: lg_State) -> Dict[str, Union[str, list, dict]]:
    """Retrieve data cubes for the current theme × units selection.

    Behaviour:
    1. Validate prerequisites (theme and ≥1 unit).
    2. Reuse any cubes already in ``state['selected_cubes']`` that match theme and year range.
    3. Fetch only the missing unit IDs from the backend.
    4. Merge, store back to state, and emit an interrupt so the UI can render.

    Returns:
        dict: Minimal state updates such as updated messages and, via interrupt,
        the cube payload for visualisation. On error/empty cases, returns just
        messages and optionally ``selected_cubes=None``.
    """

    msgs = state.get("messages", [])

    # 1️⃣  Preconditions ---------------------------------------------------------
    units: List[int] = sorted(set(get_selected_units(state)))
    if not units:
        _append_ai(state, "No areas selected – please add a place first.")
        return {"messages": msgs}

    theme_json = state.get("selected_theme")
    if not theme_json:
        _append_ai(state, "No theme selected – pick a theme first.")
        return {"messages": msgs}

    theme_df = pd.read_json(io.StringIO(theme_json), orient="records")
    theme_id: str = theme_df["ent_id"].iat[0]
    theme_label: str = theme_df["labl"].iat[0]

    min_year: int | None = state.get("min_year")
    max_year: int | None = state.get("max_year")

    # 2️⃣  Look at cached cubes --------------------------------------------------
    existing_json = state.get("selected_cubes")
    existing = pd.DataFrame()
    if existing_json:
        try:
            existing = pd.read_json(io.StringIO(
                existing_json), orient="records")
        except ValueError:
            existing = pd.DataFrame()

    if not existing.empty:
        # Ensure we actually have theme + unit columns; otherwise ignore cache
        theme_col = "Theme_ID" if "Theme_ID" in existing.columns else (
            "theme_id" if "theme_id" in existing.columns else None)
        if theme_col and "g_unit" in existing.columns:
            existing = existing[existing[theme_col] == theme_id]
            existing = _year_filter(
                existing, min_year=min_year, max_year=max_year)
            existing = existing[existing["g_unit"].isin(units)]
        else:
            existing = pd.DataFrame()

    have_units = set(existing["g_unit"].unique()
                     ) if "g_unit" in existing.columns else set()
    need_units = [u for u in units if u not in have_units]
    logger.info("FindCubes_node – need fresh cubes for units: %s", need_units)

    # 3️⃣  Fetch any missing units ----------------------------------------------
    fresh: List[pd.DataFrame] = []
    for u in need_units:
        try:
            raw = find_cubes_for_unit_theme(
                {"g_unit": str(u), "theme_id": theme_id})
            df = pd.read_json(io.StringIO(raw), orient="records")
            if "g_unit" not in df.columns:
                df["g_unit"] = u
            df = _year_filter(df, min_year=min_year, max_year=max_year)
            if not df.empty:
                df["g_unit"] = u
                fresh.append(df)
        except Exception as exc:  # noqa: BLE001 – external call can fail
            logger.warning("Cube fetch failed for unit %s: %s", u, exc)
            _append_ai(state, f"Trouble fetching data for one area (ID {u}).")

    if existing.empty and not fresh:
        _append_ai(
            state, f"No data found for theme ‘{theme_label}’ in the chosen areas.")
        return {"messages": state["messages"], "selected_cubes": None}

    # 4️⃣  Merge + store ----------------------------------------------------------
    combined = pd.concat([existing, *fresh],
                         ignore_index=True) if fresh else existing
    # Handle NaN values properly for JSON serialization
    cubes_json = combined.to_json(orient="records", force_ascii=False, default_handler=str)

    # 5️⃣  Interrupt for visualisation -------------------------------------------
    # Create the message but don't append yet (will be lost due to interrupt)
    # data_message = (
    #     f"Loaded {len(combined)} data rows for '{theme_label}' across {len(units)} area"
    #     f"{'s' if len(units) != 1 else ''}"
    #     + (f" (years {min_year or '…'}–{max_year or '…'})" if min_year or max_year else "")
    #     + "."
    # )

    interrupt({
        "cube_data_ready": True,
        "theme_label": theme_label,
        "cubes": cubes_json,
        "places": state.get("places", []),
        "selected_theme": theme_json,
        # "message": data_message,
        # "messages": serialize_messages(state.get("messages", []))
    })
