# """Data retrieval nodes: find_cubes_node."""
# from __future__ import annotations
# import io
# import pandas as pd
# from langgraph.types import Command
# from vobchat.state_schema import lg_State, get_selected_units
# from vobchat.tools import find_cubes_for_unit_theme
# from .utils import _append_ai
# import logging

# logger = logging.getLogger(__name__)

# def find_cubes_node(state: lg_State) -> dict | Command:
#     """
#     Retrieves the data‑cubes (statistical datasets) for the **currently selected theme**
#     (``state["selected_theme"]``) and every selected geographical unit
#     (from the `places` array - single source of truth).

#     Key steps
#     ----------
#     1. Merge the workflow‑selected and map‑selected units.
#     2. Parse theme information from ``state['selected_theme']``.
#     3. **Reuse already‑fetched cubes** in ``state['selected_cubes']`` where they satisfy the
#        current theme + year filters, and **only request cubes that are missing**.
#     4. Apply the optional ``min_year`` / ``max_year`` filters.
#     5. Combine the cubes, update ``state['selected_cubes']``, and emit an ``interrupt``
#        so the front‑end can visualise the data.
#     """
#     logger.info("Node: find_cubes_node entered.")
#     state["current_node"] = "find_cubes_node"
#     logger.debug({"current_state": state})

#     # ──────────────────────────────────────────────────────────────────────────
#     # 1. Early‑exit for NEW AddPlace / RemovePlace intents, but not stale ones
#     # ──────────────────────────────────────────────────────────────────────────
#     last_intent = state.get("last_intent_payload")
#     if last_intent and last_intent.get("intent") in {"AddPlace", "RemovePlace"}:
#         # Check if this is a stale intent for polygons already selected
#         intent_args = last_intent.get("arguments", {})
#         intent_polygon_id = intent_args.get("polygon_id")
#         current_selected_units = get_selected_units(state)

#         # If this AddPlace intent is for a polygon already selected, it's stale - clear it and continue
#         if (last_intent.get("intent") == "AddPlace" and
#             intent_polygon_id and intent_polygon_id in current_selected_units):
#             logger.info(
#                 f"find_cubes_node: Clearing stale AddPlace intent for already-selected polygon {intent_polygon_id}"
#             )
#             state["last_intent_payload"] = {}  # Clear after processing to prevent loops
#             # Continue processing cubes since this was a stale intent
#         else:
#             # This is a fresh intent for a new/different polygon - route to agent_node
#             logger.info(
#                 "find_cubes_node: last_intent_payload set to %s, returning to agent_node.",
#                 last_intent,
#             )
#             return Command(goto="agent_node")

#     # ──────────────────────────────────────────────────────────────────────────
#     # 2. Collect the full list of selected geographical‑unit IDs
#     # ──────────────────────────────────────────────────────────────────────────
#     # Use simplified state schema - get units from places array
#     workflow_units: list[int] = get_selected_units(state)

#     # CRITICAL: Always use workflow units as authoritative source for find_cubes_node
#     # This node is called after place/unit resolution is complete, so workflow_units
#     # contains the definitive selection state including any removals from the single source of truth
#     all_selected_unit_ids: list[int] = sorted(set(workflow_units))
#     logger.info(f"find_cubes_node: Using workflow units as authoritative: {all_selected_unit_ids}")

#     if not all_selected_unit_ids:
#         logger.warning("No units selected to find cubes for.")
#         _append_ai(state, "No areas selected to fetch data for.")
#         return {"messages": state.get("messages", [])}

#     # ──────────────────────────────────────────────────────────────────────────
#     # 3. Parse the selected theme information
#     # ──────────────────────────────────────────────────────────────────────────
#     selected_theme_json: str | None = state.get("selected_theme")
#     if not selected_theme_json:
#         logger.warning("No theme selected to find cubes for.")
#         _append_ai(state, "Please select a theme first.")
#         return {"messages": state.get("messages", [])}

#     try:
#         selected_theme_series = pd.read_json(io.StringIO(selected_theme_json))
#         if selected_theme_series.empty or "ent_id" not in selected_theme_series.columns:
#             raise ValueError("Selected theme data is invalid or missing 'ent_id'.")
#         theme_id: str = selected_theme_series["ent_id"][0]
#         theme_label: str = selected_theme_series["labl"][0]  # friendly name for the UI
#     except (ValueError, KeyError) as err:
#         logger.error("Error parsing selected theme JSON: %s", err, exc_info=True)
#         _append_ai(state, "Error reading the selected theme information.")
#         return {"messages": state.get("messages", [])}

#     # Optional year filters
#     min_year: int | None = state.get("min_year")
#     max_year: int | None = state.get("max_year")

#     # ──────────────────────────────────────────────────────────────────────────
#     # 4. Determine which units (if any) still need data
#     # ──────────────────────────────────────────────────────────────────────────
#     existing_cubes_json: str | None = state.get("selected_cubes")
#     existing_cubes_df = pd.DataFrame()
#     missing_unit_ids: list[int] = list(all_selected_unit_ids)  # start by assuming all missing

#     if existing_cubes_json:
#         try:
#             existing_cubes_df = pd.read_json(
#                 io.StringIO(existing_cubes_json), orient="records", dtype=False
#             )
#             # The stored cubes may include other themes or incomplete year ranges.
#             # Keep only rows matching the current theme.
#             if "g_unit" in existing_cubes_df.columns:
#                 existing_cubes_df = existing_cubes_df[existing_cubes_df["Theme_ID"] == theme_id]
#             else:
#                 existing_cubes_df = pd.DataFrame()  # Structure is unexpected – treat as empty
#         except ValueError:
#             # Bad JSON ⇒ ignore
#             logger.warning("selected_cubes contained invalid JSON – ignoring it.")
#             existing_cubes_df = pd.DataFrame()

#         # Apply the same year filtering logic to the existing data so the coverage test is fair.
#         def _apply_year_filter(df: pd.DataFrame) -> pd.DataFrame:
#             if "Start" not in df.columns or "End" not in df.columns:
#                 return df  # Cannot filter without year columns – assume okay
#             df = df.copy()
#             df["Start"] = pd.to_numeric(df["Start"], errors="coerce")
#             df["End"] = pd.to_numeric(df["End"], errors="coerce")
#             if min_year is not None:
#                 df = df[df["End"] >= min_year]
#             if max_year is not None:
#                 df = df[df["Start"] <= max_year]
#             return df

#         filtered_existing_df = _apply_year_filter(existing_cubes_df)

#         # For each selected unit, check if we have *any* rows after filtering.
#         missing_unit_ids = [
#             u
#             for u in all_selected_unit_ids
#             if filtered_existing_df.empty
#             or filtered_existing_df[filtered_existing_df["g_unit"] == u].empty
#         ]

#     logger.info(
#         "Units requiring a fresh fetch: %s (out of %s)",
#         missing_unit_ids,
#         all_selected_unit_ids,
#     )

#     # ──────────────────────────────────────────────────────────────────────────
#     # 5. Fetch cubes for any missing units
#     # ──────────────────────────────────────────────────────────────────────────
#     newly_fetched_dfs: list[pd.DataFrame] = []
#     for g_unit in missing_unit_ids:
#         try:
#             raw_json = find_cubes_for_unit_theme({"g_unit": str(g_unit), "theme_id": theme_id})
#             cubes_df = pd.read_json(io.StringIO(raw_json), orient="records")
#             if cubes_df.empty:
#                 logger.debug("No cubes found for unit %s, theme %s.", g_unit, theme_id)
#                 continue

#             # Year‑filter the newly fetched data
#             if "Start" in cubes_df.columns and "End" in cubes_df.columns:
#                 cubes_df["Start"] = pd.to_numeric(cubes_df["Start"], errors="coerce")
#                 cubes_df["End"] = pd.to_numeric(cubes_df["End"], errors="coerce")
#                 if min_year is not None:
#                     cubes_df = cubes_df[cubes_df["End"] >= min_year]
#                 if max_year is not None:
#                     cubes_df = cubes_df[cubes_df["Start"] <= max_year]

#             if cubes_df.empty:
#                 logger.debug(
#                     "No cubes remained for unit %s after year filtering (%s–%s).",
#                     g_unit,
#                     min_year,
#                     max_year,
#                 )
#                 continue

#             cubes_df["g_unit"] = g_unit  # tag with the unit ID
#             newly_fetched_dfs.append(cubes_df)
#             logger.debug(
#                 "Fetched %d cube rows for unit %s (theme %s).", len(cubes_df), g_unit, theme_id
#             )
#         except Exception as exc:  # noqa: BLE001
#             logger.error(
#                 "Error finding cubes for unit %s, theme %s: %s", g_unit, theme_id, exc, exc_info=True
#             )
#             _append_ai(state, f"Error fetching data for one of the areas (Unit ID: {g_unit}).")

#     # ──────────────────────────────────────────────────────────────────────────
#     # 6. Merge existing + newly‑fetched cubes and update state
#     # ──────────────────────────────────────────────────────────────────────────
#     combined_df_list: list[pd.DataFrame] = []
#     if not existing_cubes_df.empty:
#         # CRITICAL: Filter existing cubes to only include currently selected units
#         # This prevents cube data from previously removed units from persisting
#         existing_cubes_filtered = existing_cubes_df[existing_cubes_df["g_unit"].isin(all_selected_unit_ids)]
#         if not existing_cubes_filtered.empty:
#             combined_df_list.append(existing_cubes_filtered)
#             logger.info(
#                 "Filtered existing cubes: %d rows (from %d) for currently selected units %s",
#                 len(existing_cubes_filtered),
#                 len(existing_cubes_df),
#                 all_selected_unit_ids,
#             )
#         else:
#             logger.info("No existing cubes remain after filtering for current units")

#     combined_df_list.extend(newly_fetched_dfs)

#     if not combined_df_list:
#         logger.warning("No cube data found for any selected units.")
#         _append_ai(state, f"No data found for theme '{theme_label}' in the selected areas." + (" Try adjusting the year range." if (min_year or max_year) else ""))
#         return {
#             "messages": state.get("messages", []),
#             "selected_cubes": None
#         }

#     final_cubes_df = pd.concat(combined_df_list, ignore_index=True)
#     logger.info("Final combined cubes count: %d rows.", len(final_cubes_df))

#     # Generate a summary message
#     units_count = len(all_selected_unit_ids)
#     rows_count = len(final_cubes_df)
#     units_str = "area" if units_count == 1 else "areas"
#     rows_str = "data point" if rows_count == 1 else "data points"
#     year_range_str = ""
#     if min_year or max_year:
#         year_range_str = f" (years: {min_year or '…'} to {max_year or '…'})"

#     summary_msg = (
#         f"Found {rows_count} {rows_str} for theme '{theme_label}' "
#         f"across {units_count} {units_str}{year_range_str}."
#     )

#     _append_ai(state, summary_msg)

#     # Store the updated cube data
#     selected_cubes = final_cubes_df.to_json(orient="records")

#     # CRITICAL: Pass cube data through interrupt since state changes before interrupt are not saved
#     logger.info("find_cubes_node: Emitting interrupt with cube data ready for visualization.")
#     from langgraph.types import interrupt
#     interrupt(value={
#         "cube_data_ready": True,
#         "theme_label": theme_label,
#         "cubes": selected_cubes,  # Pass cube data through interrupt
#         "selected_cubes": selected_cubes,  # Pass selected cube data through interrupt
#         "show_visualization": True,  # Signal to show visualization
#         # Include other state data needed for visualization
#         "places": state.get("places", []),
#         "selected_theme": state.get("selected_theme")
#     })


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
from .utils import _append_ai, serialize_messages

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Helper – filter a cube DataFrame by year range (non‑destructive)
# -----------------------------------------------------------------------------


def _year_filter(df: pd.DataFrame, *, min_year: int | None, max_year: int | None) -> pd.DataFrame:
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
    """Retrieve data cubes for the current **theme × units** selection.

    Behaviour:
    1. Quick validation of prerequisites (theme + units).
    2. Reuse any cubes already in *state['selected_cubes']* that match the theme & year range.
    3. Fetch only the missing unit IDs.
    4. Merge, store back to state, send a summary message, and **interrupt** so the
       UI can render charts/maps immediately.
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
    cubes_json = combined.to_json(orient="records")

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
