"""Intent extraction using specialized subagents.

This module breaks the problem into three focused extractors:
- Place extraction: names and postcodes
- Theme extraction: statistical categories and theme-like phrases
- Action extraction: high-level action classification (add/remove/list/etc.)

An orchestrator combines these signals into the canonical `AssistantIntent`
payload consumed by the workflow router.
"""

from enum import Enum
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama
import logging
import os
import re
import json

from .intent_handling import AssistantIntent, SingleIntent, AssistantIntentPayload

logger = logging.getLogger(__name__)

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
        return [theme['labl'] for theme in themes_data if theme.get('labl')]
    except Exception as e:
        logger.warning(f"Failed to load themes for prompt: {e}")
        # Fallback to common themes
        return ["Population", "Housing", "Employment", "Education", "Crime", "Agriculture", "Transport"]

# LLM Configuration
_MODEL_NAME = "deepseek-r1-wt:latest"
_OLLAMA_HOST = os.getenv("OLLAMA_HOST", "localhost")
_OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434")
_OLLAMA_SUBPATH = os.getenv("OLLAMA_SUBPATH", "")
_OLLAMA_USE_SSL = os.getenv("OLLAMA_USE_SSL", "true").lower() == "true"
protocol = "https" if _OLLAMA_USE_SSL else "http"
_BASE_URL = f"{protocol}://{_OLLAMA_HOST}:{_OLLAMA_PORT}/{_OLLAMA_SUBPATH}/"

# Shared LLM instance for all subagents
_subagent_llm = ChatOllama(
    model=_MODEL_NAME,
    base_url=_BASE_URL,
    # temperature=0.0
    client_kwargs={"verify": False}
)

# =====================================================================
# 1. Place Extraction Subagent
# =====================================================================

class PlaceReference(BaseModel):
    """A geographic place reference found in user text."""
    name: str = Field(..., description="The place name or postcode")
    is_postcode: bool = Field(default=False, description="Whether this is a UK postcode")
    confidence: float = Field(default=1.0, description="Confidence in extraction (0.0-1.0)")

class PlaceExtractionResult(BaseModel):
    """Result from place extraction subagent."""
    places: List[PlaceReference] = Field(default_factory=list)

_PLACE_EXTRACTION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """
Extract UK place names and postcodes from user text.

Extract:
- UK cities/towns: London, Manchester, Birmingham, Portsmouth, etc.
- UK postcodes: SW1A 1AA, M1 1AE, OX1 3QD format

Rules:
- Extract only explicit place names mentioned
- Include place names in action contexts: "add london", "remove manchester", "include bristol"
- Don't extract from "my selected places", "current selection"
- For postcodes, set is_postcode=true
- PRESERVE EXACT SPELLING of place names as they appear in the text

Examples:
"population stats for portsmouth and newport" → [{{"name": "portsmouth", "is_postcode": false, "confidence": 1.0}}, {{"name": "newport", "is_postcode": false, "confidence": 1.0}}]
"housing data for london" → [{{"name": "london", "is_postcode": false, "confidence": 1.0}}]
"SW1A 1AA" → [{{"name": "SW1A 1AA", "is_postcode": true, "confidence": 1.0}}]
"stats for london" → [{{"name": "london", "is_postcode": false, "confidence": 1.0}}]
"add manchester and birmingham" → [{{"name": "manchester", "is_postcode": false, "confidence": 1.0}}, {{"name": "birmingham", "is_postcode": false, "confidence": 1.0}}]
"fetch education data for aberdeen and inverness" → [{{"name": "aberdeen", "is_postcode": false, "confidence": 1.0}}, {{"name": "inverness", "is_postcode": false, "confidence": 1.0}}]
"housing stats for my selected places and also add birmingham" → [{{"name": "birmingham", "is_postcode": false, "confidence": 1.0}}]
"remove london" → [{{"name": "london", "is_postcode": false, "confidence": 1.0}}]
"where's bristol?" → [{{"name": "bristol", "is_postcode": false, "confidence": 1.0}}]
"set theme to education" → [] (no places mentioned)

Reply with JSON only.
"""),
    ("user", "{text}")
])

_place_extraction_chain = _PLACE_EXTRACTION_PROMPT | _subagent_llm.with_structured_output(
    schema=PlaceExtractionResult
)

# =====================================================================
# 2. Theme Extraction Subagent  
# =====================================================================

class ThemeReference(BaseModel):
    """A statistical theme or data category found in user text."""
    theme_query: str = Field(..., description="The theme or statistical category")
    confidence: float = Field(default=1.0, description="Confidence in extraction (0.0-1.0)")

class ThemeExtractionResult(BaseModel):
    """Result from theme extraction subagent."""
    themes: List[ThemeReference] = Field(default_factory=list)

def _create_theme_extraction_prompt():
    """Create the theme extraction prompt augmented with current labels."""
    theme_labels = _get_theme_labels()
    theme_examples = ", ".join(theme_labels[:10])  # Use first 10 themes as examples
    
    # Build the system message string to avoid f-string conflicts
    system_message = """
Extract statistical themes from user text.

Available themes include: """ + theme_examples + """, and others.

Extract themes when you see:
- Statistical topics matching available themes
- Data words: stats, statistics, data, figures
- Theme switching: "use X", "change to X", "set theme to X"  
- Theme description requests: "what is X", "describe X", "tell me about X" where X is a theme name

Rules:
- Extract ONLY ONE theme per query (most specific)
- Don't extract from pure place queries: "add manchester", "show me cambridge" 
- Don't extract from postcodes alone: "SW1A 1AA", "M1 1AE"
- Don't extract themes from queries that ONLY contain postcodes
- Don't extract themes from location queries like "where's X?"
- Don't extract themes from listing queries: "what stats are there?", "what themes available?"
- DO extract themes from description queries: "what is population?", "describe housing"

Examples:
"population stats for portsmouth" → [{{"theme_query": "population", "confidence": 1.0}}]
"housing data for london" → [{{"theme_query": "housing", "confidence": 1.0}}] 
"stats for london" → [{{"theme_query": "stats", "confidence": 1.0}}]
"use employment statistics" → [{{"theme_query": "employment", "confidence": 1.0}}]
"housing for manchester" → [{{"theme_query": "housing", "confidence": 1.0}}]
"population in london" → [{{"theme_query": "population", "confidence": 1.0}}]
"what is agriculture and land use?" → [{{"theme_query": "agriculture and land use", "confidence": 1.0}}]
"describe transport" → [{{"theme_query": "transport", "confidence": 1.0}}]
"tell me about population" → [{{"theme_query": "population", "confidence": 1.0}}]
"add manchester" → [] (no theme)
"SW1A 1AA" → [] (no theme)
"M1 1AE" → [] (no theme)
"show data for M1 1AE" → [] (postcode only query)
"show me cambridge" → [] (no theme)
"where's bristol?" → [] (location query)
"what stats are there?" → [] (listing query)
"what statistics do you have?" → [] (listing query)
"what data is available?" → [] (listing query)

Reply with JSON only.
"""
    
    return ChatPromptTemplate.from_messages([
        ("system", system_message),
        ("user", "{text}")
    ])

_THEME_EXTRACTION_PROMPT = _create_theme_extraction_prompt()

_theme_extraction_chain = _THEME_EXTRACTION_PROMPT | _subagent_llm.with_structured_output(
    schema=ThemeExtractionResult
)

# =====================================================================
# 3. Action Extraction Subagent
# =====================================================================

class ActionType(str, Enum):
    ADD = "add"
    REMOVE = "remove" 
    SHOW = "show"
    LIST = "list"
    DESCRIBE = "describe"
    RESET = "reset"
    INFO = "info"
    CHAT = "chat"

class ActionReference(BaseModel):
    """An action or command found in user text."""
    action: ActionType = Field(..., description="The type of action")
    target: Optional[str] = Field(default=None, description="What the action applies to (place, theme, state)")
    confidence: float = Field(default=1.0, description="Confidence in extraction (0.0-1.0)")

class ActionExtractionResult(BaseModel):
    """Result from action extraction subagent."""
    actions: List[ActionReference] = Field(default_factory=list)

_ACTION_EXTRACTION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """
You are an action classification agent. Classify the user's intent into one of these actions:

Actions:
- add: Adding places or themes to selection  
- remove: Removing places or themes
- show: Show current state/selection
- list: List available options
- describe: Get information about themes
- reset: Start over/clear everything
- info: Get information about places
- chat: General conversation

Key patterns:
- "start over", "reset" → reset
- "remove [something]" → remove  
- "show my selection", "what have I selected" → show
- "list themes", "what themes available" → list
- "what stats/statistics/data are there", "what X available" → list
- "what stats/data/themes do you have" → list
- Data requests with places, stats, or data → add
- Location queries "where's", "show me", "find" [place] → add
- "tell me about [place]", "information about" → info
- "what is [theme]" → describe

IMPORTANT:
- Questions about available options like "what stats are there?" should be classified as 'list'
- "where's X?", "show me X", "find X" should be classified as 'add' (user wants to add place to selection)
- Only use 'info' for explicit information requests like "tell me about X"

Examples:
"start over" → [{{"action": "reset", "target": null, "confidence": 1.0}}]
"remove london" → [{{"action": "remove", "target": null, "confidence": 1.0}}]
"what have I selected" → [{{"action": "show", "target": null, "confidence": 1.0}}]
"list themes" → [{{"action": "list", "target": null, "confidence": 1.0}}]
"what stats are there?" → [{{"action": "list", "target": null, "confidence": 1.0}}]
"what statistics do you have?" → [{{"action": "list", "target": null, "confidence": 1.0}}]
"what data is available?" → [{{"action": "list", "target": null, "confidence": 1.0}}]
"population stats for london" → [{{"action": "add", "target": null, "confidence": 1.0}}]
"where's bristol?" → [{{"action": "add", "target": null, "confidence": 1.0}}]
"show me cambridge" → [{{"action": "add", "target": null, "confidence": 1.0}}]
"find manchester" → [{{"action": "add", "target": null, "confidence": 1.0}}]
"show data for M1 1AE" → [{{"action": "add", "target": null, "confidence": 1.0}}]
"tell me about manchester" → [{{"action": "info", "target": null, "confidence": 1.0}}]
"what is population theme" → [{{"action": "describe", "target": null, "confidence": 1.0}}]

Reply with JSON only.
"""),
    ("user", "{text}")
])

_action_extraction_chain = _ACTION_EXTRACTION_PROMPT | _subagent_llm.with_structured_output(
    schema=ActionExtractionResult
)

# =====================================================================
# 4. Orchestrator
# =====================================================================

def extract_intent_with_subagents(user_text: str, messages: List) -> AssistantIntentPayload:
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
        
        # Run all subagents in parallel (for now, sequentially)
        place_result = _place_extraction_chain.invoke({"text": user_text})
        theme_result = _theme_extraction_chain.invoke({"text": user_text})
        action_result = _action_extraction_chain.invoke({"text": user_text})
        
        logger.info(f"Place extraction: {place_result.model_dump()}")
        logger.info(f"Theme extraction: {theme_result.model_dump()}")
        logger.info(f"Action extraction: {action_result.model_dump()}")
        
        # Combine results into final intents
        intents = _combine_subagent_results(place_result, theme_result, action_result, user_text)
        
        result = AssistantIntentPayload(intents=intents)
        logger.info(f"Final combined intents: {[i.intent.value for i in result.intents]}")
        
        return result
        
    except Exception as e:
        logger.error(f"Subagent extraction failed: {e}")
        # Fallback to chat intent
        return AssistantIntentPayload(
            intents=[SingleIntent(
                intent=AssistantIntent.CHAT, 
                arguments={"text": f"I'm having trouble processing your request: {user_text}. Could you please try rephrasing?"}
            )]
        )

def _combine_subagent_results(
    places: PlaceExtractionResult, 
    themes: ThemeExtractionResult, 
    actions: ActionExtractionResult,
    user_text: str
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
    primary_action = ActionType.CHAT
    if actions.actions:
        primary_action = actions.actions[0].action
    
    logger.info(f"Primary action: {primary_action}")
    logger.info(f"Places found: {len(places.places)}")
    logger.info(f"Themes found: {len(themes.themes)}")
    
    # Handle different action types
    if primary_action == ActionType.ADD:
        # Add place intents first
        for place in places.places:
            if place.is_postcode:
                intents.append(SingleIntent(
                    intent=AssistantIntent.ADD_PLACE,
                    arguments={"place": place.name, "postcode": place.name}
                ))
            else:
                intents.append(SingleIntent(
                    intent=AssistantIntent.ADD_PLACE,
                    arguments={"place": place.name}
                ))
        
        # Add theme intents only if themes were found
        # Special case: don't add theme for postcode-only queries
        if themes.themes and not (len(places.places) == 1 and places.places[0].is_postcode and "data" in user_text.lower()):
            for theme in themes.themes:
                intents.append(SingleIntent(
                    intent=AssistantIntent.ADD_THEME,
                    arguments={"theme_query": theme.theme_query}
                ))
            
    elif primary_action == ActionType.REMOVE:
        # Handle remove operations - let the LLM determine what to remove based on context
        if places.places:
            # Remove specific places
            for place in places.places:
                intents.append(SingleIntent(
                    intent=AssistantIntent.REMOVE_PLACE,
                    arguments={"place": place.name}
                ))
        elif themes.themes:
            # Remove themes
            intents.append(SingleIntent(
                intent=AssistantIntent.REMOVE_THEME,
                arguments={}
            ))
        else:
            # Generic remove - assume theme removal
            intents.append(SingleIntent(
                intent=AssistantIntent.REMOVE_THEME,
                arguments={}
            ))
            
    elif primary_action == ActionType.SHOW:
        intents.append(SingleIntent(
            intent=AssistantIntent.SHOW_STATE,
            arguments={}
        ))
        
    elif primary_action == ActionType.LIST:
        intents.append(SingleIntent(
            intent=AssistantIntent.LIST_ALL_THEMES,
            arguments={}
        ))
        
    elif primary_action == ActionType.DESCRIBE:
        if places.places:
            # Describing a place
            intents.append(SingleIntent(
                intent=AssistantIntent.PLACE_INFO,
                arguments={"place": places.places[0].name}
            ))
        elif themes.themes:
            # Describing a theme
            intents.append(SingleIntent(
                intent=AssistantIntent.DESCRIBE_THEME,
                arguments={"theme": themes.themes[0].theme_query}
            ))
            
    elif primary_action == ActionType.RESET:
        intents.append(SingleIntent(
            intent=AssistantIntent.RESET,
            arguments={}
        ))
        
    elif primary_action == ActionType.INFO:
        if places.places:
            intents.append(SingleIntent(
                intent=AssistantIntent.PLACE_INFO,
                arguments={"place": places.places[0].name}
            ))
            
    else:  # CHAT or unknown
        intents.append(SingleIntent(
            intent=AssistantIntent.CHAT,
            arguments={"text": "I can help you explore historical data. Try asking about population, housing, or employment statistics for UK places."}
        ))
    
    # If no intents were generated, default to chat
    if not intents:
        intents.append(SingleIntent(
            intent=AssistantIntent.CHAT,
            arguments={"text": "I can help you explore historical data. Try asking about population, housing, or employment statistics for UK places."}
        ))
    
    logger.info(f"Generated intents: {[i.intent.value for i in intents]}")
    return intents
