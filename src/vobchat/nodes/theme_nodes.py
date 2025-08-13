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
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from .utils import _append_ai, _has_message_content, _clean_duplicate_intents_from_queue, serialize_messages, clean_database_text
import logging

logger = logging.getLogger(__name__)

# Initialize LLM for theme matching
import os
_OLLAMA_HOST = os.getenv("OLLAMA_HOST", "localhost")
_OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434")
_OLLAMA_SUBPATH = os.getenv("OLLAMA_SUBPATH", "")
_OLLAMA_USE_SSL = os.getenv("OLLAMA_USE_SSL", "true").lower() == "true"
protocol = "https" if _OLLAMA_USE_SSL else "http"
_BASE_URL = f"{protocol}://{_OLLAMA_HOST}:{_OLLAMA_PORT}/{_OLLAMA_SUBPATH}"


model = ChatOllama(
    model="deepseek-r1-wt:latest",
    base_url=_BASE_URL,
    client_kwargs={"verify": False}
)

class ThemeDecision(BaseModel):
    theme_code: str = Field(..., description="The selected theme code from available themes, e.g. T_POP")

def _build_theme_prompt(themes_dict: Dict[str, str]) -> ChatPromptTemplate:
    """Build the theme selection prompt with available themes."""
    if not themes_dict:
        themes_dict = {"T_POP": "Population"}  # Emergency fallback

    return ChatPromptTemplate.from_messages([
        (
            "system",
            "You are an expert in selecting the best statistical theme."
        ),
        (
            "system",
            "Available themes:\n" +
            "\n".join(f"{k}: {v}" for k, v in themes_dict.items())
        ),
        (
            "user",
            "Question: {question}\n"
            "Return *only* this JSON (no code fences, no extra text):\n"
            "{{\"theme_code\": \"<one_of_the_codes_above>\"}}"
        )
    ])

def _semantic_theme_match(query: str, themes_df: pd.DataFrame) -> str | None:
    """Use LLM to semantically match query to available themes."""
    if themes_df.empty:
        return None

    try:
        # Create themes dict for LLM
        themes_dict = dict(zip(themes_df['ent_id'], themes_df['labl']))

        # Build prompt and chain
        prompt = _build_theme_prompt(themes_dict)
        chain = prompt | model.with_structured_output(schema=ThemeDecision)

        # Get LLM decision
        logger.info(f"Using LLM to match '{query}' to {len(themes_dict)} available themes")
        theme_decision = chain.invoke({"question": query})

        # Extract theme_code
        if hasattr(theme_decision, 'theme_code'):
            theme_code = theme_decision.theme_code
        elif isinstance(theme_decision, dict):
            theme_code = theme_decision.get('theme_code')
        else:
            logger.warning(f"Unexpected LLM response format: {type(theme_decision)}")
            return None

        # Validate theme_code exists in available themes
        if theme_code and theme_code in themes_dict:
            logger.info(f"LLM matched '{query}' -> '{themes_dict[theme_code]}' ({theme_code})")
            return theme_code
        else:
            logger.warning(f"LLM returned invalid theme_code: {theme_code}")
            return None

    except Exception as e:
        logger.error(f"LLM semantic matching failed: {e}")
        return None


def _find_theme_candidates(query: str | None, units: List[str] | None) -> pd.DataFrame:
    """Return themes matching *query* (None ⇒ no filtering).

    • If *units* is passed, we union the theme lists for those units only.
    • Otherwise we query the whole catalogue.
    """
    logger.info(f"DEBUG: _find_theme_candidates called with query='{query}', units={units}")

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

    # First try LLM-based semantic matching - this handles variations like "Life and death" vs "Life & Death"
    logger.info(f"Using LLM semantic matching for '{query}'")
    matched_theme_code = _semantic_theme_match(query, themes_df)
    if matched_theme_code:
        # Return only the semantically matched theme
        semantic_match = themes_df[themes_df['ent_id'] == matched_theme_code]
        if not semantic_match.empty:
            logger.info(f"LLM semantic match found: {semantic_match.iloc[0]['labl']}")
            return semantic_match

    # Fallback to exact substring match for simple cases
    mask = themes_df["labl"].str.contains(query, case=False, regex=False)
    if mask.any():
        logger.info(f"Found {mask.sum()} themes with substring match for '{query}'")
        return themes_df[mask]

    # Last resort: match on individual words
    combined = pd.Series([False] * len(themes_df))
    for w in query.lower().split():
        combined |= themes_df["labl"].str.contains(w, case=False, regex=False)

    if combined.any():
        logger.info(f"Found {combined.sum()} themes with word match for '{query}'")
        return themes_df[combined]

    # Return empty if no matches found
    logger.info(f"No matches found for '{query}' using any method")
    return pd.DataFrame()

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
        "messages": serialize_messages(state.get("messages", []))
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
            [{"ent_id": code, "labl": label}]).to_json(orient='records', force_ascii=False, default_handler=str)
        _append_ai(state, f"Theme set to {label} ({code}).")
        return Command(goto="find_cubes_node", update={
            "selected_theme": selected_theme,
            "selection_idx": None,
            "options": None,
            "extracted_theme": None,
        })

    # B: Free text ========================================================
    query = (args.get("theme_query") or state.get(
        "extracted_theme") or "").strip()
    if query:
        logger.info(f"DEBUG: resolve_theme processing query='{query}' with units={sel_units}")
        df = _find_theme_candidates(query, [str(u) for u in sel_units] if sel_units else None)
        logger.info(f"DEBUG: _find_theme_candidates returned {len(df)} themes")
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
            selected_theme = pd.DataFrame([theme]).to_json(orient='records', force_ascii=False, default_handler=str)
            _append_ai(state, f"Theme set to {theme.labl} ({theme.ent_id}).")
            return Command(goto="find_cubes_node", update={
                "selected_theme": selected_theme,
                "extracted_theme": None,
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
# Node – AddTheme_node (handles theme query intents)
# -----------------------------------------------------------------------------


def AddTheme_node(state: lg_State) -> dict | Command:
    """Process AddTheme intent and route to resolve_theme."""
    logger.info("AddTheme_node: Processing AddTheme intent")

    payload = state.get("last_intent_payload", {})
    args = payload.get("arguments", {}) if payload else {}

    # Extract theme query from intent
    theme_query = args.get("theme_query", "").strip()

    if theme_query:
        logger.info(f"AddTheme_node: Setting extracted_theme to '{theme_query}'")
        # Set extracted_theme and route to resolve_theme
        return Command(goto="resolve_theme", update={
            "extracted_theme": theme_query,
            "last_intent_payload": {},  # Clear after processing
            "messages": state.get("messages", [])
        })
    else:
        logger.warning("AddTheme_node: No theme_query in arguments")
        # Route to resolve_theme anyway to show available themes
        return Command(goto="resolve_theme", update={
            "last_intent_payload": {},  # Clear after processing
            "messages": state.get("messages", [])
        })

# -----------------------------------------------------------------------------
# Node – ListThemes_node (unified listing for all/selection)
# -----------------------------------------------------------------------------


def ListThemes_node(state: lg_State):
    """List themes relevant to the current selection or the full catalogue.

    If units are selected, lists themes available for those units; otherwise
    lists all known themes. Does not interrupt; prints a formatted list into
    the conversation and returns updated messages.
    """
    sel_units = get_selected_units(state)

    # Get themes based on context - if there are selected places, show themes for those
    if sel_units:
        # List themes for selected places
        df = _find_theme_candidates(None, [str(u) for u in sel_units])
        if df.empty:
            _append_ai(state, "No themes found for your selection.")
            return {"messages": state["messages"]}
        header = "Themes available for your selection:"
        footer = ""
    else:
        # List all themes
        df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")
        if df.empty:
            _append_ai(state, "Theme catalogue appears empty.")
            return {"messages": state["messages"]}
        header = "All available themes:"
        footer = "\n… all themes shown. Use keywords to narrow."

    # Format the listing
    listing = "\n".join(f"• {row.labl}" for _, row in df.iterrows())
    _append_ai(state, f"{header}\n{listing}{footer}")
    return {"messages": state["messages"]}

# -----------------------------------------------------------------------------
# Node – RemoveTheme_node
# -----------------------------------------------------------------------------


def RemoveTheme_node(state: lg_State):
    """Clear the currently-selected theme, if any, and acknowledge."""
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
    """Show a short, cleaned description for the given or selected theme.

    If an explicit theme is supplied via intent arguments it is resolved; else
    the currently selected theme is described. Falls back to a helpful prompt
    if nothing can be determined.
    """
    payload = state.get("last_intent_payload", {})
    query = (payload.get("arguments", {}).get("theme") or "").strip()

    theme_df: pd.DataFrame | None = None
    if state.get("selected_theme"):
        theme_df = pd.read_json(io.StringIO(
            state["selected_theme"]), orient="records")

    if theme_df is None or theme_df.empty:
        if not query:
            _append_ai(
                state, "Please specify a theme, e.g. 'describe Population'.")
            return {"messages": state["messages"]}
        matches = _find_theme_candidates(query, None)
        if matches.empty:
            _append_ai(
                state, f"I couldn't find a theme matching '{query}'. Try 'list themes'.")
            return {"messages": state["messages"]}
        theme_df = matches.head(1)

    code = theme_df["ent_id"].iat[0]
    labl = theme_df["labl"].iat[0]

    desc_df = pd.read_json(io.StringIO(get_theme_text(code)), orient="records")
    raw_text = desc_df["text"].iat[0] if not desc_df.empty else "(no description available)"
    
    # Clean up the database text using the shared utility function
    clean_text = clean_database_text(raw_text)

    _append_ai(state, f"**{labl}**\n\n{clean_text}")
    return {"messages": state["messages"]}
