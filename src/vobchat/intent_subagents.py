"""Intent extraction using specialized subagents.

This module breaks the problem into three focused extractors:
- Place extraction: names and postcodes
- Theme extraction: statistical categories and theme-like phrases
- Action extraction: high-level action classification (add/remove/list/etc.)

An orchestrator combines these signals into the canonical `AssistantIntent`
payload consumed by the workflow router.
"""

from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from vobchat.llm_factory import get_llm
from .configure_logging import get_llm_callback
import logging
import os
import json

from .intent_handling import (
    AssistantIntent,
    SingleIntent,
    AssistantIntentPayload,
)
from vobchat.utils.constants import UNIT_TYPES
from .configure_logging import log_llm_interaction

logger = logging.getLogger(__name__)


def _summarize_recent_conversation(
    messages: List, max_messages: int = 6, max_chars: int = 600
) -> str:
    """Return a compact summary of the last few conversation turns.

    Includes role and truncated content. Avoids dumping large or sensitive text.
    """
    try:
        msgs = list(messages or [])
    except Exception:
        msgs = []
    if not msgs:
        return "(none)"
    recent = msgs[-max_messages:]
    lines: List[str] = []
    for m in recent:
        try:
            role = getattr(m, "type", None) or getattr(m, "role", None) or "message"
            content = (getattr(m, "content", None) or "").strip()
            if len(content) > 200:
                content = content[:200] + "…"
            lines.append(f"{role}: {content}")
        except Exception:
            continue
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[-max_chars:]
    return text if text else "(none)"


def _get_theme_labels() -> List[str]:
    """Return theme labels for dynamic prompt inclusion.

    Returns:
        list[str]: A curated list of labels, falling back to common examples if
        the database lookup fails.
    """
    try:
        from .tools import get_all_themes

        themes_json = get_all_themes.invoke({})  # Use invoke instead of direct call
        themes_data = json.loads(themes_json)
        return [theme["labl"] for theme in themes_data if theme.get("labl")]
    except Exception as e:
        logger.warning(f"Failed to load themes for prompt: {e}")
        # Fallback to common themes
        return [
            "Population",
            "Housing",
            "Employment",
            "Education",
            "Crime",
            "Agriculture",
            "Transport",
        ]


_REASONING_ENV = os.getenv("VOBCHAT_OLLAMA_REASONING", "true").lower() == "true"

# Shared LLM instance via centralized factory (JSON mode for structured outputs).
# Disable reasoning here to prevent non-JSON reasoning tokens from polluting
# structured outputs and causing parse issues.
_subagent_llm = get_llm(json_mode=True, reasoning=False)

# =====================================================================
# 1. Place Extraction Subagent
# =====================================================================


class PlaceReference(BaseModel):
    """A geographic place reference found in user text."""

    name: str = Field(..., description="The place name or postcode")
    is_postcode: bool = Field(
        default=False, description="Whether this is a UK postcode"
    )
    confidence: float = Field(
        default=1.0, description="Confidence in extraction (0.0-1.0)"
    )


class PlaceExtractionResult(BaseModel):
    """Result from place extraction subagent."""

    places: List[PlaceReference] = Field(default_factory=list)


_PLACE_EXTRACTION_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """
Extract UK place names and postcodes from user text.

Extract:
- UK cities/towns.
- UK postcodes.

Rules:
- Extract only explicit place names mentioned
- Include place names in action contexts: "add <place_name>", "remove <place_name>", "include <place_name>"
- Don't extract from "my selected places", "current selection"
- For postcodes, set is_postcode=true
- PRESERVE EXACT SPELLING of place names as they appear in the text

Examples:
"<theme> stats for <place_name> and <place_name>" → [{{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}, {{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}]
"<theme> data for <place_name>" → [{{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}]
"<uk_postcode>" → [{{"name": "<uk_postcode>", "is_postcode": true, "confidence": 1.0}}]
"stats for <place_name>" → [{{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}]
"add <place_name> and <place_name>" → [{{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}, {{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}]
"fetch <theme> data for <place_name> and <place_name>" → [{{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}, {{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}]
"<theme> stats for my selected places and also add <place_name>" → [{{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}]
"remove <place_name>" → [{{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}]
"where's <place_name>?" → [{{"name": "<place_name>", "is_postcode": false, "confidence": 1.0}}]
"set theme to <theme>" → [] (no places mentioned)

Reply with JSON only.
""",
        ),
        (
            "user",
            """
Context:
Recent conversation (last turns):
{recent_conversation}

USER_TEXT
{text}
""",
        ),
    ]
)

_place_extraction_chain = (
    _PLACE_EXTRACTION_PROMPT
    | _subagent_llm.with_structured_output(schema=PlaceExtractionResult)
).with_config(
    {
        "tags": ["subagent", "no_ui_stream"],
        "run_name": "place_extractor",
        "callbacks": [get_llm_callback()],
    }
)

# =====================================================================
# 2. Theme Extraction Subagent
# =====================================================================


class ThemeReference(BaseModel):
    """A statistical theme or data category found in user text."""

    theme_query: str = Field(..., description="The theme or statistical category")
    confidence: float = Field(
        default=1.0, description="Confidence in extraction (0.0-1.0)"
    )


class ThemeExtractionResult(BaseModel):
    """Result from theme extraction subagent."""

    themes: List[ThemeReference] = Field(default_factory=list)


def _create_theme_extraction_prompt():
    """Create the theme extraction prompt augmented with current labels."""
    theme_labels = _get_theme_labels()
    theme_examples = ", ".join(theme_labels[:10])  # Use first 10 themes as examples

    # Build the system message string to avoid f-string conflicts
    system_message = (
        """
Extract statistical themes from user text.

Available themes include: """
        + theme_examples
        + """, and others.

Extract themes when you see:
- Statistical topics matching available themes
- Data words: stats, statistics, data, figures
- Theme switching: "use X", "change to X", "set theme to X"  
- Theme description requests: "what is X", "describe X", "tell me about X" where X is a theme name

Rules:
- Extract ONLY ONE theme per query (most specific)
- Don't extract from pure place queries: "add <place_name>", "show me <place>" 
- Don't extract from postcodes alone: "<uk_postcode>"
- Don't extract themes from queries that ONLY contain postcodes
- Don't extract themes from location queries like "where's X?"
- Don't extract themes from listing queries: "what stats are there?", "what themes available?"
- DO extract themes from description queries: "what is <theme>?", "describe <theme>"

Examples:
"<theme> stats for <place_name>" → [{{"theme_query": "<theme>", "confidence": 1.0}}]
"<theme> data for <place_name>" → [{{"theme_query": "<theme>", "confidence": 1.0}}]
"stats for <place_name>" → [{{"theme_query": "stats", "confidence": 1.0}}]
"use <theme>" → [{{"theme_query": "<theme>", "confidence": 1.0}}]
"<theme> for <place_name>" → [{{"theme_query": "<theme>", "confidence": 1.0}}]
"<theme> in <place_name>" → [{{"theme_query": "<theme>", "confidence": 1.0}}]
"what is <theme>?" → [{{"theme_query": "<theme>", "confidence": 1.0}}]
"describe <theme>" → [{{"theme_query": "<theme>", "confidence": 1.0}}]
"tell me about <theme>" → [{{"theme_query": "<theme>", "confidence": 1.0}}]
"add <place_name>" → [] (no theme)
"<uk_postcode>" → [] (no theme)
"show data for <uk_postcode>" → [] (postcode only query)
"show me <place_name>" → [] (no theme)
"where's <place_name>?" → [] (location query)
"what stats are there?" → [] (listing query)
"what statistics do you have?" → [] (listing query)
"what data is available?" → [] (listing query)

Reply with JSON only.
"""
    )

    return ChatPromptTemplate.from_messages(
        [
            ("system", system_message),
            (
                "user",
                """
Context:
Recent conversation (last turns):
{recent_conversation}

USER_TEXT
{text}
""",
            ),
        ]
    )


_THEME_EXTRACTION_PROMPT = _create_theme_extraction_prompt()

_theme_extraction_chain = (
    _THEME_EXTRACTION_PROMPT
    | _subagent_llm.with_structured_output(schema=ThemeExtractionResult)
).with_config(
    {
        "tags": ["subagent", "no_ui_stream"],
        "run_name": "theme_extractor",
        "callbacks": [get_llm_callback()],
    }
)

# =====================================================================
# 3. Action Extraction Subagent
# =====================================================================


class ActionType(str, Enum):
    ADD = "add"
    REMOVE = "remove"
    STATE = "state"
    DESCRIBE = "describe"
    EXPLAIN_VISIBLE = "explain_visible"
    RESET = "reset"
    INFO = "info"
    CHAT = "chat"


class ActionReference(BaseModel):
    """An action determined from user text.

    This includes the type of action, the target it applies to, and the confidence in the extraction.
    """

    action: ActionType = Field(..., description="The type of action")
    target: Optional[str] = Field(
        default=None,
        description="The target the action applies to (place, theme, unit type)",
    )
    confidence: float = Field(
        default=1.0, description="Confidence in extraction (0.0-1.0)"
    )


class ActionExtractionResult(BaseModel):
    """Result from action extraction subagent."""

    actions: List[ActionReference] = Field(default_factory=list)


_ACTION_EXTRACTION_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """
You are an action classification agent. 
Classify the user's intent into one/multiple of these actions:

- add: Adding/selecting places and/or data/statistical themes.
- remove: Removing/deselecting places and/or data themes from selection.
- state: Current selected places and/or data themes.
- describe: Return information about a place or the definition of a unit type/data theme/data entity.
- explain_visible: Explain the data currently shown/visualised by summarising all visible data entities.
- reset: Start over/clear everything.
- info: Provide a list of available data themes.
- chat: General conversation/anything else.

Classification rules (be strict and prefer concrete actions):
- If the user asks to "show/get/display/fetch/see" (or similar) "stats/data/statistics/figures" for one or more places, classify as add (not describe).
- When both a place and a theme are present (e.g., "<theme> stats for <place>"), classify as add (so downstream logic can AddPlace and AddTheme).
- Use describe only for definition/explanation requests (e.g., "tell me about <place>" for place info, or "what is the <theme> theme?").
- If the user asks to explain/describe the data currently shown (e.g., "explain this data", "what does this show", "explain the chart/graph"), use explain_visible.
- Use info for listing available options (e.g., "what themes are available?").
- Use state for queries about the current selection (e.g., "what have I selected").
- Use reset for starting over/clearing selection.
- Use chat only when the user is just chatting and not asking to do anything.
 - Use the recent conversation to resolve deictics like "that place" or "that theme" to the most recently mentioned explicit entity. If resolution is not possible, prefer a clarifying PlaceInfo/DescribeTheme action instead of guessing.

Examples (JSON only; set higher confidence for the correct action):
"show <theme> stats for <place>" → {{"actions": [{{"action": "add", "target": "<place>", "confidence": 1.0}}]}}
"<theme> stats for <place> and <place>" → {{"actions": [{{"action": "add", "target": "<place>", "confidence": 1.0}}]}}
"show data for <uk_postcode>" → {{"actions": [{{"action": "add", "target": "<place>", "confidence": 1.0}}]}}
"tell me about <place>" → {{"actions": [{{"action": "describe", "target": "<place>", "confidence": 1.0}}]}}
"what is the <theme> theme?" → {{"actions": [{{"action": "describe", "target": "<theme>", "confidence": 1.0}}]}}
"what themes are available?" → {{"actions": [{{"action": "info", "confidence": 1.0}}]}}
"explain this data" → {{"actions": [{{"action": "explain_visible", "confidence": 1.0}}]}}
""",
        ),
        (
            "user",
            """
Context:
Recent conversation (last turns):
{recent_conversation}

USER_TEXT
{text}
""",
        ),
    ]
)

_action_extraction_chain = (
    _ACTION_EXTRACTION_PROMPT
    | _subagent_llm.with_structured_output(schema=ActionExtractionResult)
).with_config(
    {
        "tags": ["subagent", "no_ui_stream"],
        "run_name": "action_extractor",
        "callbacks": [get_llm_callback()],
    }
)

# =====================================================================
# 4. Orchestrator
# =====================================================================


def extract_intent_with_subagents(
    user_text: str, messages: List
) -> AssistantIntentPayload:
    """Extract intents using place/theme/action subagents and combine results.

    Args:
        user_text: The raw user utterance to classify.
        messages: Recent conversation history (not currently used by subagents).

    Returns:
        AssistantIntentPayload: Combined, de-duplicated intents with minimal
        arguments suitable for workflow routing.
    """
    try:
        logger.info(f"Extracting intent with subagents for: '{user_text}'")

        # Prepare recent conversation context for subagents
        recent_conv = _summarize_recent_conversation(messages)

        # Run all subagents in parallel (for now, sequentially), with logging
        try:
            msgs = _PLACE_EXTRACTION_PROMPT.format_messages(
                {"text": user_text, "recent_conversation": recent_conv}
            )
            log_llm_interaction(
                name="place_extractor",
                prompt_vars={
                    "text": user_text,
                    "recent_conversation": recent_conv[:180],
                },
                formatted_messages=msgs,
                extra={"reasoning_enabled": _REASONING_ENV},
            )
        except Exception:
            pass
        place_result = _place_extraction_chain.invoke(
            {"text": user_text, "recent_conversation": recent_conv}
        )
        try:
            log_llm_interaction(name="place_extractor_result", output=place_result)
        except Exception:
            pass

        try:
            msgs_t = _THEME_EXTRACTION_PROMPT.format_messages(
                {"text": user_text, "recent_conversation": recent_conv}
            )
            log_llm_interaction(
                name="theme_extractor",
                prompt_vars={
                    "text": user_text,
                    "recent_conversation": recent_conv[:180],
                },
                formatted_messages=msgs_t,
                extra={"reasoning_enabled": _REASONING_ENV},
            )
        except Exception:
            pass
        theme_result = _theme_extraction_chain.invoke(
            {"text": user_text, "recent_conversation": recent_conv}
        )
        try:
            log_llm_interaction(name="theme_extractor_result", output=theme_result)
        except Exception:
            pass

        try:
            msgs_a = _ACTION_EXTRACTION_PROMPT.format_messages(
                {"text": user_text, "recent_conversation": recent_conv}
            )
            log_llm_interaction(
                name="action_extractor",
                prompt_vars={
                    "text": user_text,
                    "recent_conversation": recent_conv[:180],
                },
                formatted_messages=msgs_a,
                extra={"reasoning_enabled": _REASONING_ENV},
            )
        except Exception:
            pass
        action_result = _action_extraction_chain.invoke(
            {"text": user_text, "recent_conversation": recent_conv}
        )
        try:
            log_llm_interaction(name="action_extractor_result", output=action_result)
        except Exception:
            pass

        logger.info(f"Place extraction: {place_result.model_dump()}")
        logger.info(f"Theme extraction: {theme_result.model_dump()}")
        # logger.info(f"UnitTypeInfo extraction: {unit_type_result.model_dump()}")
        logger.info(f"Action extraction: {action_result.model_dump()}")

        # Combine results into final intents
        intents = _combine_subagent_results(
            place_result,
            theme_result,
            action_result,
            user_text,
        )

        result = AssistantIntentPayload(intents=intents)
        logger.info(
            f"Final combined intents: {[i.intent.value for i in result.intents]}"
        )

        return result

    except Exception as e:
        logger.error(f"Subagent extraction failed: {e}")
        # Fallback to chat intent
        return AssistantIntentPayload(
            intents=[
                SingleIntent(
                    intent=AssistantIntent.CHAT,
                    arguments={
                        "text": f"I'm having trouble processing your request: {user_text}. Could you please try rephrasing?"
                    },
                )
            ]
        )


def _combine_subagent_results(
    places: PlaceExtractionResult,
    themes: ThemeExtractionResult,
    actions: ActionExtractionResult,
    user_text: str,
) -> List[SingleIntent]:
    """Combine subagent results into the final list of SingleIntent.

    Preference order is driven by the primary action; place intents are listed
    before theme intents when adding, and safe fallbacks are provided for
    remove/list/reset/info/chat.

    Returns:
        list[SingleIntent]: Canonical intent list for the router.
    """
    intents = []

    # Determine primary action
    # Prefer the highest-confidence action if multiple are returned.
    primary_action = ActionType.CHAT
    if actions.actions:
        try:
            primary_action = (
                sorted(
                    actions.actions,
                    key=lambda a: getattr(a, "confidence", 1.0),
                    reverse=True,
                )[0]
            ).action
        except Exception:
            primary_action = actions.actions[0].action

    logger.info(f"Primary action: {primary_action}")
    logger.info(f"Places found: {len(places.places)}")
    logger.info(f"Themes found: {len(themes.themes)}")

    # # If no clear action but we found entities, default to ADD
    # if primary_action == ActionType.CHAT and (places.places or themes.themes):
    #     primary_action = ActionType.ADD

    # Handle different action types
    if primary_action == ActionType.ADD:
        # Add place intents first
        for place in places.places:
            if place.is_postcode:
                intents.append(
                    SingleIntent(
                        intent=AssistantIntent.ADD_PLACE,
                        arguments={"postcode": place.name},
                    )
                )
            else:
                intents.append(
                    SingleIntent(
                        intent=AssistantIntent.ADD_PLACE,
                        arguments={"place": place.name},
                    )
                )

        # Add theme intents only if themes were found
        # Special case: if this is a postcode-only query and the theme is generic
        # (e.g., "data", "stats", etc.), skip adding the theme. Allow specific
        # themes like "population" or "housing" to go through.
        generic_themes = {"data", "stat", "stats", "statistics", "figures", "numbers"}
        is_postcode_only = len(places.places) == 1 and places.places[0].is_postcode
        if themes.themes:
            for theme in themes.themes:
                tq = (theme.theme_query or "").strip().lower()
                if is_postcode_only and (tq in generic_themes):
                    continue
                intents.append(
                    SingleIntent(
                        intent=AssistantIntent.ADD_THEME,
                        arguments={"theme_query": theme.theme_query},
                    )
                )

    elif primary_action == ActionType.REMOVE:
        # Handle remove operations - let the LLM determine what to remove based on context
        if places.places:
            # Remove specific places
            for place in places.places:
                intents.append(
                    SingleIntent(
                        intent=AssistantIntent.REMOVE_PLACE,
                        arguments={"place": place.name},
                    )
                )
        elif themes.themes:
            # Remove themes
            intents.append(
                SingleIntent(intent=AssistantIntent.REMOVE_THEME, arguments={})
            )
        else:
            # Generic remove - assume theme removal
            intents.append(
                SingleIntent(intent=AssistantIntent.REMOVE_THEME, arguments={})
            )

    elif primary_action == ActionType.STATE:
        intents.append(SingleIntent(intent=AssistantIntent.SHOW_STATE, arguments={}))

    # 'list' action removed; use INFO for available options

    elif primary_action == ActionType.DESCRIBE:
        if places.places:
            # Describing a place
            intents.append(
                SingleIntent(
                    intent=AssistantIntent.PLACE_INFO,
                    arguments={"place": places.places[0].name},
                )
            )
        elif themes.themes:
            # Describing a theme
            intents.append(
                SingleIntent(
                    intent=AssistantIntent.DESCRIBE_THEME,
                    arguments={"theme_query": themes.themes[0].theme_query},
                )
            )

    elif primary_action == ActionType.EXPLAIN_VISIBLE:
        intents.append(
            SingleIntent(intent=AssistantIntent.EXPLAIN_VISIBLE_DATA, arguments={})
        )

    elif primary_action == ActionType.RESET:
        intents.append(SingleIntent(intent=AssistantIntent.RESET, arguments={}))

    elif primary_action == ActionType.INFO:
        # Informational queries about what's available → list themes/options
        intents.append(
            SingleIntent(intent=AssistantIntent.LIST_ALL_THEMES, arguments={})
        )

    else:  # CHAT or unknown
        # Do not force a Chat fallback here. Leaving no intents allows the
        # conversational planner to generate a natural reply.
        pass

    # If no intents were generated, return an empty list so the planner
    # path in conversational_agent can produce a natural reply.
    if not intents:
        return []

    logger.info(f"Generated intents: {[i.intent.value for i in intents]}")
    return intents
