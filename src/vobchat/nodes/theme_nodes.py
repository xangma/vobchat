"""Theme-related nodes: AddTheme, RemoveTheme, ListThemes, DescribeTheme, resolve_theme."""
from __future__ import annotations
from typing import Dict, List, Union
import io
import json
import pandas as pd
from langgraph.types import Command
from langgraph.types import interrupt
from vobchat.state_schema import lg_State, get_selected_units
from vobchat.tools import (
    find_themes_for_unit,
    get_all_themes,
    get_theme_text
)
from .utils import _append_ai, _has_message_content, _clean_duplicate_intents_from_queue
import logging

logger = logging.getLogger(__name__)

# def ListThemesForSelection_node(state: lg_State) -> dict:
#     """List themes available for the current place selection."""
#     g_units = get_selected_units(state)
#     if not g_units:
#         _append_ai(state, "No place selected yet.")
#         return {"messages": state.get("messages", [])}

#     rows = []
#     for u in g_units:
#         try:
#             themes_json = find_themes_for_unit(str(u))
#             df = pd.read_json(io.StringIO(themes_json), orient="records")
#             if not df.empty:
#                 rows.append(df)
#         except Exception as exc:
#             _append_ai(state, f"Error fetching themes for unit {u}: {exc}")

#     if not rows:
#         _append_ai(state, "No themes found for your selection.")
#         return {"messages": state.get("messages", [])}

#     big = pd.concat(rows).drop_duplicates("ent_id")
#     listing = "\n".join(f"• {row.labl} ({row.ent_id})" for _, row in big.iterrows())
#     _append_ai(state, "Themes available:\n" + listing)
#     return {"messages": state.get("messages", [])}

# def ListAllThemes_node(state: lg_State) -> dict:
#     """List all available themes in the system."""
#     df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
#     if df.empty:
#         # Check if we already have this message to prevent duplicates
#         if not _has_message_content(state, "Theme catalogue appears empty."):
#             _append_ai(state, "Theme catalogue appears empty.")
#         return {"messages": state.get("messages", [])}

#     listing = "\n".join(f"• {row.labl} ({row.ent_id})" for _, row in df.iterrows())
#     themes_message = listing + "\n… all themes shown. Use keywords to narrow."

#     # Check if we already have this themes listing to prevent duplicates
#     if not _has_message_content(state, listing):
#         _append_ai(state, themes_message)

#     # Only return the fields this node updates
#     return {
#         "messages": state.get("messages", []),
#         "needs_clarification": False
#     }

# def AddTheme_node(state: lg_State) -> dict | Command:
#     """Add a theme to the selection."""
#     logger.info("AddTheme_node: adding a theme to the selection")

#     # CRITICAL: Clear ALL AddTheme intents from queue immediately to prevent infinite loops
#     intent_queue = state.get("intent_queue", [])
#     original_queue_size = len(intent_queue)

#     # Note: Removed intent_queue updates to prevent concurrent modification conflicts
#     # Theme intent processing now relies on workflow routing instead of queue management
#     cleaned_queue = [intent for intent in intent_queue if intent.get("intent") != "AddTheme"]

#     removed_count = original_queue_size - len(cleaned_queue)
#     if removed_count > 0:
#         logger.info(f"AddTheme_node: AGGRESSIVELY removed {removed_count} AddTheme intents from queue (was {original_queue_size}, now {len(cleaned_queue)})")

#     # Also clean other duplicate intents while we're at it
#     _clean_duplicate_intents_from_queue(state)

#     payload = state.get("last_intent_payload", {})
#     args = payload.get("arguments", {}) if payload else {}

#     # direct code
#     if "theme_code" in args:
#         code = args["theme_code"].strip().upper()
#         if not code.startswith("T_"):
#             _append_ai(state, f"'{code}' doesn't look like a valid theme code.")
#             return {"messages": state.get("messages", [])}

#         _append_ai(state, f"Theme set to {code}.")
#         selected_theme = pd.DataFrame({"ent_id": [code], "labl": [code]}).to_json(orient="records")

#         return {
#             "selected_theme": selected_theme,
#             "messages": state.get("messages", [])
#         }

#     # free text query
#     elif "theme_query" in args:
#         q = args["theme_query"].strip()
#         logger.info(f"AddTheme_node: Processing theme query '{q}'")

#         # Check if we already have this theme selected to avoid unnecessary changes
#         current_theme = state.get("selected_theme")
#         if current_theme:
#             try:
#                 theme_data = json.loads(current_theme)
#                 current_label = theme_data.get("labl", "").lower()
#                 query_lower = q.lower().strip()

#                 # If the query matches the current theme, don't change anything
#                 if query_lower in current_label or current_label in query_lower:
#                     logger.info(f"AddTheme_node: Theme query '{q}' matches current theme '{current_label}', keeping current selection")

#                     # CRITICAL: Clear ALL AddTheme intents from the queue to prevent infinite processing
#                     intent_queue = state.get("intent_queue", [])
#                     original_queue_length = len(intent_queue)
#                     # Note: Removed intent_queue updates to prevent concurrent modification conflicts
#                     # Theme intent processing now relies on workflow routing instead of queue management
#                     filtered_queue = [
#                         intent for intent in intent_queue
#                         if intent.get("intent") != "AddTheme"  # Remove ALL AddTheme intents, not just matching ones
#                     ]
#                     removed_count = original_queue_length - len(filtered_queue)
#                     if removed_count > 0:
#                         logger.info(f"AddTheme_node: Would remove {removed_count} ALL AddTheme intents from queue")

#                     # Note: Don't clear last_intent_payload here to avoid conflicts with agent router
#                     return {}
#             except (json.JSONDecodeError, KeyError):
#                 logger.warning(f"AddTheme_node: Error parsing current theme, proceeding with theme change")

#         logger.info(f"AddTheme_node: Setting extracted_theme to '{q}' and clearing selected_theme for theme resolution")

#         # CRITICAL: Use Command to route directly to resolve_theme with state updates
#         # This ensures the state changes are applied before resolve_theme runs
#         return Command(
#             goto="resolve_theme",
#             update={
#                 "selected_theme": None,  # Clear existing theme
#                 "extracted_theme": q,    # Set the theme query for resolution
#                 # "last_intent_payload": None,  # Clear to prevent infinite loop
#             }
#         )

#     else:
#         _append_ai(state, "AddTheme: no theme_code or theme_query provided.")

#     # Handle routing logic (moved from addtheme_router)
#     selected_theme = state.get("selected_theme")
#     extracted_theme = state.get("extracted_theme")
#     has_theme = bool(selected_theme)
#     has_units = bool(get_selected_units(state) or state.get("selected_polygons"))

#     logging.info(f"AddTheme_node routing: has_theme={has_theme}, has_units={has_units}")
#     logging.info(f"AddTheme_node routing: selected_theme='{selected_theme}', extracted_theme='{extracted_theme}'")

#     # CRITICAL: If we have extracted_theme, we need to process the theme change regardless of selected_theme
#     if extracted_theme:
#         logging.info("AddTheme_node: routing to resolve_theme (need to process extracted theme)")
#         return Command(goto="resolve_theme", update={"messages": state.get("messages", [])})
#     elif has_theme and has_units:
#         logging.info("AddTheme_node: routing to find_cubes_node (have both theme and units)")
#         return Command(goto="find_cubes_node", update={"messages": state.get("messages", [])})
#     elif has_theme and not has_units:
#         # Check if we're in a situation where places are being processed
#         current_place_index = state.get("current_place_index", 0) or 0
#         total_places = len(state.get("places", []) or [])

#         if current_place_index < total_places:
#             logging.info("AddTheme_node: routing to resolve_place_and_unit (have theme, need to continue place processing)")
#             return Command(goto="resolve_place_and_unit", update={"messages": state.get("messages", [])})
#         else:
#             logging.info("AddTheme_node: routing to agent_node (have theme, need units)")
#             return Command(goto="agent_node", update={"messages": state.get("messages", [])})
#     else:
#         logging.info("AddTheme_node: routing to resolve_theme (need theme)")
#         return Command(goto="resolve_theme", update={"messages": state.get("messages", [])})

# def RemoveTheme_node(state: lg_State) -> dict:
#     """Remove the currently selected theme."""
#     if not state.get("selected_theme"):
#         _append_ai(state, "No theme is currently selected.")
#         return {"messages": state.get("messages", [])}

#     _append_ai(state, "Theme selection cleared.")

#     # Only return the fields this node updates
#     return {
#         "messages": state.get("messages", []),
#         "selected_theme": None,
#     }

# def DescribeTheme_node(state: lg_State) -> dict:
#     """
#     Reply with the definition/metadata of a theme.
#     • Works even if *no* theme is currently selected.
#     • Clears last_intent_payload so the router won't loop.
#     """

#     payload = state.get("last_intent_payload") or {}
#     args    = payload.get("arguments", {})
#     query   = (args.get("theme") or "").strip()

#     logger.info(f"DescribeTheme_node: Processing query '{query}' from payload: {payload}")

#     # 1️⃣ Determine the theme code
#     theme_df = None

#     # a) use already-selected theme (if any)
#     if state.get("selected_theme"):
#         theme_df = pd.read_json(io.StringIO(state["selected_theme"]), orient="records")

#     # b) otherwise, fuzzy-match the query against *all* themes
#     if theme_df is None and query:
#         all_df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
#         logger.info(f"DescribeTheme_node: Loaded {len(all_df)} themes. Sample themes: {all_df.head(3)['labl'].tolist()}")

#         # Try exact case-insensitive match first
#         mask = all_df["labl"].str.contains(query, case=False, regex=False)
#         matches = all_df[mask]
#         logger.info(f"DescribeTheme_node: Exact match for '{query}': found {len(matches)} matches")
#         if mask.any():
#             theme_df = all_df[mask].head(1)
#             logger.info(f"DescribeTheme_node: Using exact match: {theme_df['labl'].iloc[0]}")
#         else:
#             # Try partial word matching if exact match fails
#             query_words = query.lower().split()
#             logger.info(f"DescribeTheme_node: No exact match, trying word matching for: {query_words}")
#             if query_words:
#                 # Look for themes that contain any of the query words
#                 combined_mask = pd.Series([False] * len(all_df))
#                 for word in query_words:
#                     word_mask = all_df["labl"].str.contains(word, case=False, regex=False)
#                     word_matches = all_df[word_mask]
#                     logger.info(f"DescribeTheme_node: Word '{word}' matched {len(word_matches)} themes: {word_matches['labl'].tolist()[:3]}")
#                     combined_mask |= word_mask

#                 if combined_mask.any():
#                     theme_df = all_df[combined_mask].head(1)
#                     logger.info(f"DescribeTheme_node: Using word match: {theme_df['labl'].iloc[0]}")
#                 else:
#                     logger.info(f"DescribeTheme_node: No word matches found for '{query}'")

#     # c) still nothing → ask a follow-up
#     if theme_df is None or theme_df.empty:
#         _append_ai(state, "I'm not sure which theme you mean. Try e.g. 'describe Population' or 'describe T_POP'.")
#         return {"messages": state.get("messages", [])}

#     code = theme_df["ent_id"].iat[0]
#     labl = theme_df["labl"].iat[0]

#     # 2️⃣ Fetch the long description
#     desc_df = pd.read_json(io.StringIO(get_theme_text(code)), orient="records")
#     text    = desc_df["text"].iat[0] if not desc_df.empty else "(no description available)"

#     _append_ai(state, f"**{labl}** ({code})\n\n{text}")

#     # Only return the fields this node updates
#     return {"messages": state.get("messages", [])}

# def resolve_theme(state: lg_State) -> dict | Command:
#     """Resolve an extracted theme query to a specific theme selection."""
#     logger.debug("=== URGENT DEBUG: resolve_theme FUNCTION ENTRY ===")
#     logger.info("resolve_theme: Starting theme resolution...")

#     # Debug: Log the complete state to understand what's being passed
#     logger.info(f"=== URGENT DEBUG: resolve_theme FULL STATE - selection_idx={state.get('selection_idx')}, current_node={state.get('current_node')}, options={len(state.get('options', []))} options ===")

#     # Handle routing logic first (moved from resolve_theme_router)
#     has_theme = bool(state.get("selected_theme"))
#     has_units = bool(get_selected_units(state))
#     has_options = bool(state.get("options"))
#     current_node = state.get("current_node")
#     selection_idx = state.get("selection_idx")
#     extracted_theme = state.get("extracted_theme")
#     last_intent_payload = state.get("last_intent_payload", {})

#     # Check if places still need processing
#     current_place_index = state.get("current_place_index", 0) or 0
#     places = state.get("places", []) or []
#     num_places = len(places)

#     logger.info(f"=== URGENT DEBUG: resolve_theme - has_theme={has_theme}, has_units={has_units}, has_options={has_options} ===")
#     logging.info(f"resolve_theme: has_theme={has_theme}, has_units={has_units}, has_options={has_options}, current_node={current_node}, selection_idx={selection_idx}, extracted_theme={extracted_theme}")
#     logging.info(f"resolve_theme: current_place_index={current_place_index}, num_places={num_places}")
#     logging.info(f"resolve_theme: last_intent_payload={last_intent_payload}")

#     # CRITICAL: If places still need processing, always go back to resolve_place_and_unit
#     if current_place_index < num_places:
#         logging.info(f"resolve_theme: returning resolve_place_and_unit (places still need processing: {current_place_index} of {num_places})")
#         return Command(goto="resolve_place_and_unit")

#     # CRITICAL: RECURSION PREVENTION - If we have a theme and units but no new theme work to do,
#     # NEVER route back to resolve_theme as it creates infinite loops
#     if has_theme and has_units and not extracted_theme and not (selection_idx is not None and has_options):
#         logger.info(f"=== RECURSION PREVENTION: resolve_theme - ROUTING TO CUBES (preventing loop) ===")
#         logging.info("resolve_theme: returning find_cubes_node (recursion prevention - have theme and units, no new theme work)")
#         return Command(goto="find_cubes_node")

#     # CRITICAL: If a theme button was clicked but not processed yet, continue processing
#     # Check for selection_idx when we have units but no theme (theme selection scenario)
#     if selection_idx is not None and has_units and not has_theme and not extracted_theme:
#         logger.info(f"=== URGENT DEBUG: resolve_theme - PROCESSING THEME SELECTION - selection_idx={selection_idx}, has_options={has_options}, current_node={current_node} ===")
#         logging.info(f"resolve_theme: processing theme button selection (selection_idx={selection_idx}, has_theme={has_theme})")

#         # If we don't have options in state, we need to regenerate them
#         # This happens because state changes before interrupt are not saved
#         if not has_options:
#             logger.info("resolve_theme: Options not in state after resume, regenerating theme list")
#             g_units = get_selected_units(state)

#             rows = []
#             for u in g_units:
#                 try:
#                     themes_json = find_themes_for_unit(str(u))
#                     df = pd.read_json(io.StringIO(themes_json), orient="records")
#                     if not df.empty:
#                         rows.append(df)
#                 except Exception as exc:
#                     logger.warning(f"Error fetching themes for unit {u}: {exc}")

#             if rows:
#                 df = pd.concat(rows).drop_duplicates("ent_id")
#                 logger.info(f"resolve_theme: Regenerated {len(df)} themes for selection")

#                 # Now process the selection with the regenerated data
#                 try:
#                     choice = int(selection_idx)
#                     if 0 <= choice < len(df):
#                         theme = df.iloc[choice]
#                         selected_theme = pd.DataFrame([theme]).to_json(orient="records")
#                         _append_ai(state, f"Selected theme: {theme['labl']} ({theme['ent_id']})")

#                         # We have units, so route to find_cubes_node
#                         logging.info("resolve_theme: User selected theme, have units, routing to find_cubes_node")
#                         return Command(goto="find_cubes_node", update={
#                             "selected_theme": selected_theme,
#                             "extracted_theme": None,
#                             "selection_idx": None,  # Clear selection index
#                             "options": None,  # Clear options
#                             "current_node": None,  # Clear current node
#                             "messages": state.get("messages", [])
#                         })
#                     else:
#                         logger.warning(f"resolve_theme: Invalid selection index {choice} (>= {len(df)} themes)")
#                         return Command(goto="agent_node")
#                 except (ValueError, TypeError) as e:
#                     logger.warning(f"resolve_theme: Error processing selection: {e}")
#                     return Command(goto="agent_node")
#             else:
#                 logger.warning("resolve_theme: No themes found when regenerating")
#                 return Command(goto="agent_node")
#         else:
#             # Handle theme selection based on selection_idx
#             options = state.get("options", []) or []
#             try:
#                 # Use query-based search results
#                 query = state.get("extracted_theme")
#                 if query:
#                     df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
#                     # Apply the same filtering logic as below
#                     if not df.empty:
#                         mask = df["labl"].str.contains(query, case=False, regex=False)
#                         if mask.any():
#                             df = df[mask]
#                         else:
#                             query_words = query.lower().split()
#                             combined_mask = pd.Series([False] * len(df))
#                             for word in query_words:
#                                 word_mask = df["labl"].str.contains(word, case=False, regex=False)
#                                 combined_mask |= word_mask
#                             if combined_mask.any():
#                                 df = df[combined_mask]

#                     if df is not None and not df.empty and choice < len(df):
#                         theme = df.iloc[choice]
#                         selected_theme = pd.DataFrame([theme]).to_json(orient="records")
#                         _append_ai(state, f"Selected theme: {theme['labl']} ({theme['ent_id']})")

#                         # Route based on whether we have units
#                         if get_selected_units(state):
#                             logging.info("resolve_theme: User selected theme, have units, routing to find_cubes_node")
#                             return Command(goto="find_cubes_node", update={
#                                 "selected_theme": selected_theme,
#                                 "extracted_theme": None,
#                                 "messages": state.get("messages", [])
#                             })
#                         else:
#                             logging.info("resolve_theme: User selected theme, need units, routing to agent_node")
#                             return Command(goto="agent_node", update={
#                                 "selected_theme": selected_theme,
#                                 "extracted_theme": None,
#                                 "messages": state.get("messages", [])
#                             })
#             except (ValueError, TypeError):
#                 logging.warning(f"resolve_theme: Invalid selection_idx '{selection_idx}'")

#     query = state.get("extracted_theme")

#     # CRITICAL: If we already have a selected theme and no new query, skip processing
#     if state.get("selected_theme") and not query:
#         logger.info("resolve_theme: Already have selected_theme and no new query, skipping")
#         # Clear state and jump to cube retrieval if ready
#         temp_state = state.copy()
#         temp_state["extracted_theme"] = None

#         # if isinstance(result, Command):
#         #     return Command(goto=result.goto)
#         # else:
#         #     # If we have both theme and units, go to cubes
#         #     if has_theme and has_units:
#         #         logger.info(f"=== URGENT DEBUG: resolve_theme - ROUTING TO CUBES ===")
#         #         logging.info("resolve_theme: returning find_cubes_node (have theme and units)")
#         #         return Command(goto="find_cubes_node")
#         #     # If we have a theme but no units, go to agent to handle next steps
#         #     elif has_theme and not has_units:
#         #         logging.info("resolve_theme: returning agent_node (have theme, need units)")
#         #         return Command(goto="agent_node")
#         #     # If we're actively waiting for user selection and no selection was made yet, stay in resolve_theme
#         #     elif has_options and current_node == "resolve_theme" and selection_idx is None and not has_theme:
#         #         logging.info("resolve_theme: continuing in resolve_theme (waiting for theme selection)")
#         #         return {}
#         #     else:
#         #         logging.info("resolve_theme: returning agent_node (default)")
#         #         return Command(goto="agent_node")

#     if not query:
#         logger.warning("resolve_theme: No extracted_theme to resolve")
#         # Route based on current state
#         if has_theme and has_units:
#             logging.info("resolve_theme: returning find_cubes_node (have theme and units)")
#             return Command(goto="find_cubes_node")
#         elif has_theme and not has_units:
#             logging.info("resolve_theme: returning agent_node (have theme, need units)")
#             return Command(goto="agent_node")
#         elif has_units and not has_theme:
#             # We have units but no theme - show available themes for the selected units
#             logging.info("resolve_theme: Have units but no theme, fetching available themes")
#             g_units = get_selected_units(state)

#             rows = []
#             for u in g_units:
#                 try:
#                     themes_json = find_themes_for_unit(str(u))
#                     df = pd.read_json(io.StringIO(themes_json), orient="records")
#                     if not df.empty:
#                         rows.append(df)
#                 except Exception as exc:
#                     logger.warning(f"Error fetching themes for unit {u}: {exc}")

#             if not rows:
#                 _append_ai(state, "No themes found for your selection.")
#                 return Command(goto="agent_node")

#             big = pd.concat(rows).drop_duplicates("ent_id")

#             # Create options for user selection
#             options = []
#             for idx, (_, row) in enumerate(big.head(10).iterrows()):  # Limit to 10
#                 options.append({
#                     "option_type": "theme",
#                     "label": f"{row['labl']} ({row['ent_id']})",
#                     "value": idx,
#                     "color": "#333"
#                 })

#             # Ask user to select
#             # CRITICAL: Pass themes data through interrupt since state changes before interrupt are not saved
#             interrupt(
#                 value={
#                     "message": f"Found {len(big)} themes for your selection. Please choose one:",
#                     "options": options,
#                     "current_node": "resolve_theme",
#                 }
#             )
#         else:
#             logging.info("resolve_theme: returning agent_node (no theme or query)")
#             return Command(goto="agent_node")

#     # Search for themes
#     logger.info(f"resolve_theme: Searching for theme matching '{query}'")
#     df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")

#     # Filter themes based on query
#     if not df.empty:
#         # First try exact case-insensitive match
#         mask = df["labl"].str.contains(query, case=False, regex=False)
#         if mask.any():
#             df = df[mask]
#         else:
#             # If no exact match, try partial word matching
#             query_words = query.lower().split()
#             combined_mask = pd.Series([False] * len(df))
#             for word in query_words:
#                 word_mask = df["labl"].str.contains(word, case=False, regex=False)
#                 combined_mask |= word_mask
#             if combined_mask.any():
#                 df = df[combined_mask]
#             else:
#                 # No matches found, return empty DataFrame
#                 df = pd.DataFrame(columns=df.columns)

#     if df.empty:
#         _append_ai(state, f"I couldn't find any themes matching '{query}'. Try 'list all themes' to see what's available.")
#         # Clear extracted_theme to prevent infinite loops
#         return {
#             "messages": state.get("messages", []),
#         }

#     # Handle disambiguation if multiple themes found
#     if len(df) > 1:
#         logger.info(f"resolve_theme: Found {len(df)} themes matching '{query}', asking user to choose")

#         # Create options for user selection
#         options = []
#         for idx, (_, row) in enumerate(df.head(10).iterrows()):  # Limit to 10 options
#             options.append({
#                 "option_type": "theme",
#                 "label": f"{row['labl']} ({row['ent_id']})",
#                 "value": idx,
#                 "color": "#333"
#             })

#         # Ask user to select
#         interrupt(
#             value={
#                 "message": f"I found {len(df)} themes matching '{query}'. Please choose one:",
#                 "options": options,
#                 "current_node": "resolve_theme"
#             }
#         )


#     # Single theme found - select it
#     theme = df.iloc[0]
#     logger.info(f"resolve_theme: Single theme found - selecting {theme['labl']} ({theme['ent_id']})")

#     selected_theme = pd.DataFrame([theme]).to_json(orient="records")
#     _append_ai(state, f"Selected theme: {theme['labl']} ({theme['ent_id']})")

#     # Create temporary state to check what _maybe_route_to_cubes returns
#     # temp_state = state.copy()
#     # temp_state["selected_theme"] = selected_theme
#     # temp_state["extracted_theme"] = None
#     # temp_state["messages"] = state.get("messages", [])

#     # result = _maybe_route_to_cubes(temp_state)

#     # if isinstance(result, Command):
#     #     # Return Command with only the updated fields
#     #     return Command(goto=result.goto, update={
#     #         "selected_theme": selected_theme,
#     #         "messages": state.get("messages", [])
#     #     })
#     # else:
#     #     # Return partial state with only updated fields
#     return Command(goto="find_cubes_node", update={
#         "selected_theme": selected_theme,
#         "messages": state.get("messages", [])
#     })

# theme_nodes.py – streamlined theme‑handling utilities for the LangGraph workflow
# ==================================================================================
# Public interface (exported nodes)
# --------------------------------
# • HandleTheme_node           – select / resolve a theme (buttons, free‑text, or auto)
# • ThemesForSelection_node    – list themes for the user’s current place selection
# • ListAllThemes_node         – show the full theme catalogue (optionally narrowed)
# • RemoveTheme_node           – clear the current theme selection
# • DescribeTheme_node         – return the long description of a theme
#
# The heavy lifting sits in two private helpers:
#     _find_theme_candidates()   – search the catalogue
#     _ask_user_to_choose()      – interrupt with up to 10 buttons
#
# Assumes the surrounding application provides:
#     _append_ai(), interrupt(), get_selected_units(),
#     find_themes_for_unit(), get_all_themes(), get_theme_text(), Command class.
# ==================================================================================

# -----------------------------------------------------------------------------
# Helper – fetch candidate themes
# -----------------------------------------------------------------------------


def _find_theme_candidates(query: str | None, units: List[str] | None) -> pd.DataFrame:
    """Return themes matching *query* (None ⇒ no filtering).

    • If *units* is passed, we union the theme lists for those units only.
    • Otherwise we query the whole catalogue.
    """

    if units:
        rows: list[pd.DataFrame] = []
        for u in units:
            try:
                df = pd.read_json(io.StringIO(
                    find_themes_for_unit(str(u))), orient="records")
                if not df.empty:
                    rows.append(df)
            except Exception as exc:
                logger.warning("Theme fetch failed for unit %s: %s", u, exc)
        themes_df = pd.concat(rows) if rows else pd.DataFrame()
    else:
        themes_df = pd.read_json(io.StringIO(
            get_all_themes("")), orient="records")

    if themes_df.empty:
        return themes_df

    themes_df = themes_df.drop_duplicates("ent_id")
    if not query:
        return themes_df

    # First try simple substring (case‑insensitive)
    mask = themes_df["labl"].str.contains(query, case=False, regex=False)
    if mask.any():
        return themes_df[mask]

    # Otherwise match on individual words
    combined = pd.Series([False] * len(themes_df))
    for w in query.lower().split():
        combined |= themes_df["labl"].str.contains(w, case=False, regex=False)
    return themes_df[combined]

# -----------------------------------------------------------------------------
# Helper – ask user to pick from a list (interrupt)
# -----------------------------------------------------------------------------


def _ask_user_to_choose(state: lg_State, df: pd.DataFrame, prompt: str):
    top = df.head(10).reset_index(drop=True)
    options: List[Dict[str, str]] = []
    for i, row in top.iterrows():
        options.append({
            "option_type": "theme",
            "label": f"{row.labl} ({row.ent_id})",
            "value": row.ent_id,
        })

    interrupt({
        "message": prompt,
        "options": options,
        "current_node": "resolve_theme",
    })

# -----------------------------------------------------------------------------
# Node – HandleTheme_node
# -----------------------------------------------------------------------------


def resolve_theme(state: lg_State):
    """Resolve a theme from either buttons or free text, or prompt the user."""

    msgs = state.get("messages", [])
    sel_units = get_selected_units(state)
    selection_idx = state.get("selection_idx")
    payload = state.get("last_intent_payload", {}) or {}
    args = payload.get("arguments", {}) or {}

    # A: Button click ====================================================
    if selection_idx is not None:
        try:
            # In the new system, selection_idx is the theme ent_id directly
            code = str(selection_idx)
            
            # Get theme details from database to get the label
            df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
            theme_row = df[df["ent_id"] == code]
            if theme_row.empty:
                raise ValueError(f"Theme '{code}' not found")
            
            label = theme_row.iloc[0]["labl"]
        except (ValueError, IndexError, KeyError):
            _append_ai(
                state, "That choice wasn’t recognised – please try again.")
            return {"messages": msgs}

        selected_theme = pd.DataFrame(
            [{"ent_id": code, "labl": label}]).to_json(orient="records")
        _append_ai(state, f"Theme set to {label} ({code}).")
        return Command(goto="find_cubes_node", update={
            "selected_theme": selected_theme,
            "selection_idx": None,
            "options": None,
            "extracted_theme": None,
            "messages": state["messages"],
        })

    # B: Free text ========================================================
    query = (args.get("theme_query") or state.get(
        "extracted_theme") or "").strip()
    if query:
        df = _find_theme_candidates(query, [str(u) for u in sel_units] if sel_units else None)
        if df.empty:
            _append_ai(
                state, f"I couldn’t find any themes matching ‘{query}’. Try again or type ‘list themes’.")
            # Show available themes as fallback
            fallback_df = _find_theme_candidates(None, [str(u) for u in sel_units] if sel_units else None)
            if not fallback_df.empty:
                _ask_user_to_choose(
                    state, fallback_df, f"Please choose from {len(fallback_df)} available themes:")
                return {"messages": state["messages"]}
            return {"messages": msgs}
        if len(df) == 1:
            theme = df.iloc[0]
            selected_theme = pd.DataFrame([theme]).to_json(orient="records")
            _append_ai(state, f"Theme set to {theme.labl} ({theme.ent_id}).")
            return Command(goto="find_cubes_node", update={
                "selected_theme": selected_theme,
                "extracted_theme": None,
                "messages": state["messages"],
            })
        _ask_user_to_choose(
            state, df, f"I found {len(df)} themes for ‘{query}’. Please pick one:")
        return {"messages": state["messages"]}

    # C: No query – list themes for current place or whole catalogue =====
    df = _find_theme_candidates(None, [str(u) for u in sel_units] if sel_units else None)
    if df.empty:
        _append_ai(state, "No themes available for the current selection.")
        return {"messages": msgs}

    if len(df) == 1:
        theme = df.iloc[0]
        selected_theme = pd.DataFrame([theme]).to_json(orient="records")
        _append_ai(state, f"Theme set to {theme.labl} ({theme.ent_id}).")
        return Command(goto="find_cubes_node", update={
            "selected_theme": selected_theme,
            "messages": state["messages"],
        })

    _ask_user_to_choose(
        state, df, f"Found {len(df)} themes. Please choose one:")
    return {"messages": state["messages"]}

# -----------------------------------------------------------------------------
# Node – ThemesForSelection_node (pure listing, no state mutation)
# -----------------------------------------------------------------------------


def ListThemesForSelection_node(state: lg_State):
    sel_units = get_selected_units(state)
    if not sel_units:
        _append_ai(state, "No place selected yet.")
        return {"messages": state.get("messages", [])}

    df = _find_theme_candidates(None, [str(u) for u in sel_units] if sel_units else None)
    if df.empty:
        _append_ai(state, "No themes found for your selection.")
        return {"messages": state["messages"]}

    listing = "\n".join(
        f"• {row.labl} ({row.ent_id})" for _, row in df.iterrows())
    _append_ai(state, "Themes available:\n" + listing)
    return {"messages": state["messages"]}

# -----------------------------------------------------------------------------
# Node – ListAllThemes_node (pure listing, global)
# -----------------------------------------------------------------------------


def ListAllThemes_node(state: lg_State):
    df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
    if df.empty:
        _append_ai(state, "Theme catalogue appears empty.")
        return {"messages": state["messages"]}

    listing = "\n".join(
        f"• {row.labl} ({row.ent_id})" for _, row in df.iterrows())
    _append_ai(state, listing + "\n… all themes shown. Use keywords to narrow.")
    return {"messages": state["messages"]}

# -----------------------------------------------------------------------------
# Node – RemoveTheme_node
# -----------------------------------------------------------------------------


def RemoveTheme_node(state: lg_State):
    if not state.get("selected_theme"):
        _append_ai(state, "No theme is currently selected.")
        return {"messages": state["messages"]}

    _append_ai(state, "Theme selection cleared.")
    return {
        "messages": state["messages"],
        "selected_theme": None,
    }

# -----------------------------------------------------------------------------
# Node – DescribeTheme_node
# -----------------------------------------------------------------------------


def DescribeTheme_node(state: lg_State):
    payload = state.get("last_intent_payload", {})
    query = (payload.get("arguments", {}).get("theme") or "").strip()

    theme_df: pd.DataFrame | None = None
    if state.get("selected_theme"):
        theme_df = pd.read_json(io.StringIO(
            state["selected_theme"]), orient="records")

    if theme_df is None or theme_df.empty:
        if not query:
            _append_ai(
                state, "Please specify a theme, e.g. ‘describe Population’.")
            return {"messages": state["messages"]}
        matches = _find_theme_candidates(query, None)
        if matches.empty:
            _append_ai(
                state, f"I couldn’t find a theme matching ‘{query}’. Try ‘list themes’.")
            return {"messages": state["messages"]}
        theme_df = matches.head(1)

    code = theme_df["ent_id"].iat[0]
    labl = theme_df["labl"].iat[0]

    desc_df = pd.read_json(io.StringIO(get_theme_text(code)), orient="records")
    text = desc_df["text"].iat[0] if not desc_df.empty else "(no description available)"

    _append_ai(state, f"**{labl}** ({code})\n\n{text}")
    return {"messages": state["messages"]}
