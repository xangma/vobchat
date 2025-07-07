"""Data retrieval nodes: find_cubes_node."""
from __future__ import annotations
import io
import pandas as pd
from langchain_core.messages import AIMessage
from langgraph.types import Command
from vobchat.state_schema import lg_State, get_selected_units
from vobchat.tools import find_cubes_for_unit_theme
import logging

logger = logging.getLogger(__name__)

def find_cubes_node(state: lg_State) -> lg_State | Command:
    """
    Retrieves the data‑cubes (statistical datasets) for the **currently selected theme**
    (``state["selected_theme"]``) and every selected geographical unit
    (from the `places` array - single source of truth).

    Key steps
    ----------
    1. Merge the workflow‑selected and map‑selected units.
    2. Parse theme information from ``state['selected_theme']``.
    3. **Reuse already‑fetched cubes** in ``state['selected_cubes']`` where they satisfy the
       current theme + year filters, and **only request cubes that are missing**.
    4. Apply the optional ``min_year`` / ``max_year`` filters.
    5. Combine the cubes, update ``state['selected_cubes']``, and emit an ``interrupt``
       so the front‑end can visualise the data.
    """
    logger.info("Node: find_cubes_node entered.")
    state["current_node"] = "find_cubes_node"
    logger.debug({"current_state": state})

    # ──────────────────────────────────────────────────────────────────────────
    # 1. Early‑exit for NEW AddPlace / RemovePlace intents, but not stale ones
    # ──────────────────────────────────────────────────────────────────────────
    last_intent = state.get("last_intent_payload")
    if last_intent and last_intent.get("intent") in {"AddPlace", "RemovePlace"}:
        # Check if this is a stale intent for polygons already selected
        intent_args = last_intent.get("arguments", {})
        intent_polygon_id = intent_args.get("polygon_id")
        current_selected_units = get_selected_units(state)

        # If this AddPlace intent is for a polygon already selected, it's stale - clear it and continue
        if (last_intent.get("intent") == "AddPlace" and
            intent_polygon_id and intent_polygon_id in current_selected_units):
            logger.info(
                f"find_cubes_node: Clearing stale AddPlace intent for already-selected polygon {intent_polygon_id}"
            )
            state["last_intent_payload"] = {}
            # Continue processing cubes since this was a stale intent
        else:
            # This is a fresh intent for a new/different polygon - route to agent_node
            logger.info(
                "find_cubes_node: last_intent_payload set to %s, returning to agent_node.",
                last_intent,
            )
            return Command(goto="agent_node")

    # ──────────────────────────────────────────────────────────────────────────
    # 2. Collect the full list of selected geographical‑unit IDs
    # ──────────────────────────────────────────────────────────────────────────
    # Use simplified state schema - get units from places array
    workflow_units: list[int] = get_selected_units(state)

    # CRITICAL: Always use workflow units as authoritative source for find_cubes_node
    # This node is called after place/unit resolution is complete, so workflow_units
    # contains the definitive selection state including any removals from the single source of truth
    all_selected_unit_ids: list[int] = sorted(set(workflow_units))
    logger.info(f"find_cubes_node: Using workflow units as authoritative: {all_selected_unit_ids}")

    if not all_selected_unit_ids:
        logger.warning("No units selected to find cubes for.")
        state["messages"].append(AIMessage(content="No areas selected to fetch data for."))
        return state

    # ──────────────────────────────────────────────────────────────────────────
    # 3. Parse the selected theme information
    # ──────────────────────────────────────────────────────────────────────────
    selected_theme_json: str | None = state.get("selected_theme")
    if not selected_theme_json:
        logger.warning("No theme selected to find cubes for.")
        state["messages"].append(AIMessage(content="Please select a theme first."))
        return state

    try:
        selected_theme_series = pd.read_json(io.StringIO(selected_theme_json), typ="series")
        if selected_theme_series.empty or "ent_id" not in selected_theme_series.index:
            raise ValueError("Selected theme data is invalid or missing 'ent_id'.")
        theme_id: str = selected_theme_series["ent_id"]
        theme_label: str = selected_theme_series["labl"]  # friendly name for the UI
    except (ValueError, KeyError) as err:
        logger.error("Error parsing selected theme JSON: %s", err, exc_info=True)
        state["messages"].append(
            AIMessage(content="Error reading the selected theme information.")
        )
        return state

    # Optional year filters
    min_year: int | None = state.get("min_year")
    max_year: int | None = state.get("max_year")

    # ──────────────────────────────────────────────────────────────────────────
    # 4. Determine which units (if any) still need data
    # ──────────────────────────────────────────────────────────────────────────
    existing_cubes_json: str | None = state.get("selected_cubes")
    existing_cubes_df = pd.DataFrame()
    missing_unit_ids: list[int] = list(all_selected_unit_ids)  # start by assuming all missing

    if existing_cubes_json:
        try:
            existing_cubes_df = pd.read_json(
                io.StringIO(existing_cubes_json), orient="records", dtype=False
            )
            # The stored cubes may include other themes or incomplete year ranges.
            # Keep only rows matching the current theme.
            if "g_unit" in existing_cubes_df.columns:
                existing_cubes_df = existing_cubes_df[existing_cubes_df["Theme_ID"] == theme_id]
            else:
                existing_cubes_df = pd.DataFrame()  # Structure is unexpected – treat as empty
        except ValueError:
            # Bad JSON ⇒ ignore
            logger.warning("selected_cubes contained invalid JSON – ignoring it.")
            existing_cubes_df = pd.DataFrame()

        # Apply the same year filtering logic to the existing data so the coverage test is fair.
        def _apply_year_filter(df: pd.DataFrame) -> pd.DataFrame:
            if "Start" not in df.columns or "End" not in df.columns:
                return df  # Cannot filter without year columns – assume okay
            df = df.copy()
            df["Start"] = pd.to_numeric(df["Start"], errors="coerce")
            df["End"] = pd.to_numeric(df["End"], errors="coerce")
            if min_year is not None:
                df = df[df["End"] >= min_year]
            if max_year is not None:
                df = df[df["Start"] <= max_year]
            return df

        filtered_existing_df = _apply_year_filter(existing_cubes_df)

        # For each selected unit, check if we have *any* rows after filtering.
        missing_unit_ids = [
            u
            for u in all_selected_unit_ids
            if filtered_existing_df.empty
            or filtered_existing_df[filtered_existing_df["g_unit"] == u].empty
        ]

    logger.info(
        "Units requiring a fresh fetch: %s (out of %s)",
        missing_unit_ids,
        all_selected_unit_ids,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # 5. Fetch cubes for any missing units
    # ──────────────────────────────────────────────────────────────────────────
    newly_fetched_dfs: list[pd.DataFrame] = []
    for g_unit in missing_unit_ids:
        try:
            raw_json = find_cubes_for_unit_theme({"g_unit": str(g_unit), "theme_id": theme_id})
            cubes_df = pd.read_json(io.StringIO(raw_json), orient="records")
            if cubes_df.empty:
                logger.debug("No cubes found for unit %s, theme %s.", g_unit, theme_id)
                continue

            # Year‑filter the newly fetched data
            if "Start" in cubes_df.columns and "End" in cubes_df.columns:
                cubes_df["Start"] = pd.to_numeric(cubes_df["Start"], errors="coerce")
                cubes_df["End"] = pd.to_numeric(cubes_df["End"], errors="coerce")
                if min_year is not None:
                    cubes_df = cubes_df[cubes_df["End"] >= min_year]
                if max_year is not None:
                    cubes_df = cubes_df[cubes_df["Start"] <= max_year]

            if cubes_df.empty:
                logger.debug(
                    "No cubes remained for unit %s after year filtering (%s–%s).",
                    g_unit,
                    min_year,
                    max_year,
                )
                continue

            cubes_df["g_unit"] = g_unit  # tag with the unit ID
            newly_fetched_dfs.append(cubes_df)
            logger.debug(
                "Fetched %d cube rows for unit %s (theme %s).", len(cubes_df), g_unit, theme_id
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error finding cubes for unit %s, theme %s: %s", g_unit, theme_id, exc, exc_info=True
            )
            state["messages"].append(
                AIMessage(content=f"Error fetching data for one of the areas (Unit ID: {g_unit}).")
            )

    # ──────────────────────────────────────────────────────────────────────────
    # 6. Merge existing + newly‑fetched cubes and update state
    # ──────────────────────────────────────────────────────────────────────────
    combined_df_list: list[pd.DataFrame] = []
    if not existing_cubes_df.empty:
        # CRITICAL: Filter existing cubes to only include currently selected units
        # This prevents cube data from previously removed units from persisting
        existing_cubes_filtered = existing_cubes_df[existing_cubes_df["g_unit"].isin(all_selected_unit_ids)]
        if not existing_cubes_filtered.empty:
            combined_df_list.append(existing_cubes_filtered)
            logger.info(
                "Filtered existing cubes: %d rows (from %d) for currently selected units %s",
                len(existing_cubes_filtered),
                len(existing_cubes_df),
                all_selected_unit_ids,
            )
        else:
            logger.info("No existing cubes remain after filtering for current units")

    combined_df_list.extend(newly_fetched_dfs)

    if not combined_df_list:
        logger.warning("No cube data found for any selected units.")
        state["messages"].append(
            AIMessage(
                content=f"No data found for theme '{theme_label}' in the selected areas."
                + (" Try adjusting the year range." if (min_year or max_year) else "")
            )
        )
        state["selected_cubes"] = None
        return state

    final_cubes_df = pd.concat(combined_df_list, ignore_index=True)
    logger.info("Final combined cubes count: %d rows.", len(final_cubes_df))

    # Store the updated cube data
    state["selected_cubes"] = final_cubes_df.to_json(orient="records")

    # ──────────────────────────────────────────────────────────────────────────
    # 7. Generate a summary message
    # ──────────────────────────────────────────────────────────────────────────
    units_count = len(all_selected_unit_ids)
    rows_count = len(final_cubes_df)
    units_str = "area" if units_count == 1 else "areas"
    rows_str = "data point" if rows_count == 1 else "data points"
    year_range_str = ""
    if min_year or max_year:
        year_range_str = f" (years: {min_year or '…'} to {max_year or '…'})"

    summary_msg = (
        f"Found {rows_count} {rows_str} for theme '{theme_label}' "
        f"across {units_count} {units_str}{year_range_str}."
    )
    state["messages"].append(AIMessage(content=summary_msg))

    # ──────────────────────────────────────────────────────────────────────────
    # 8. Emit an interrupt to signal the front‑end that data is ready
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("find_cubes_node: Emitting interrupt with cube data ready for visualization.")
    from langgraph.types import interrupt
    interrupt(value={"cube_data_ready": True, "theme_label": theme_label})

    return state