"""Intent routing schema and LLM-based extraction.

Defines the canonical `AssistantIntent` enum, the structured payload returned by
the intent extractor, and the chain/prompt used to classify user input. The
`extract_intent` entry point delegates to the subagent-based extractor in
`intent_subagents.py` for better robustness, but keeps the original prompt and
structured-output chain available for reference or fallback.
"""

from enum import Enum
from typing import Optional, List, Dict, Any
from langchain_core.messages import AIMessage
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import SystemMessage, HumanMessage, AnyMessage
from langchain_ollama import ChatOllama
import logging

logger = logging.getLogger(__name__)
# -------------------------------------------------------------------------------------
# 1.  AssistantIntent enum - canonical names routed inside the graph
# -------------------------------------------------------------------------------------

class AssistantIntent(str, Enum):
    """Canonical intents that the agent routes inside the graph."""
    DESCRIBE_THEME = "DescribeTheme"
    ADD_PLACE = "AddPlace"
    REMOVE_PLACE = "RemovePlace"
    ADD_THEME = "AddTheme"
    REMOVE_THEME = "RemoveTheme"
    SHOW_STATE = "ShowState"
    LIST_ALL_THEMES = "ListThemes"
    PLACE_INFO = "PlaceInfo"  # general information about a place
    RESET = "Reset"
    CHAT = "Chat"  # free-form response - no state mutation


# -------------------------------------------------------------------------------------
# 2.  Structured LLM response schema
# -------------------------------------------------------------------------------------

# One payload per intent …
class SingleIntent(BaseModel):
    """A single intent with optional arguments extracted from a message."""
    intent: AssistantIntent = Field(..., description="The recognized intent name.")
    arguments: Dict[str, Any] = Field(
        default_factory=dict,
        description="For AddPlace either {'place': str} or {'places': list[str]}",
   )
class AssistantIntentPayload(BaseModel):
    """Minimal contract returned by the intent extractor before routing.

    Example:
        {"intents": [{"intent": "AddPlace", "arguments": {"place": "London"}}]}
    """
    intents: List[SingleIntent]


# -------------------------------------------------------------------------------------
# 3.  Prompt + chain to extract the intent
# -------------------------------------------------------------------------------------

import os

_MODEL_NAME = "deepseek-r1-wt:latest"  # keep in sync with workflow.py
_OLLAMA_HOST = os.getenv("OLLAMA_HOST", "localhost")
_OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434")
_BASE_URL = f"https://{_OLLAMA_HOST}:{_OLLAMA_PORT}/"

# CRITICAL: Use a separate non-streaming LLM instance for intent extraction
# This prevents the JSON parsing from getting stuck in streaming mode
_intent_llm = ChatOllama(
    model=_MODEL_NAME,
    base_url=_BASE_URL,
    # temperature=0.0,
    client_kwargs={"verify": False}
)

intent_list = ", \n".join([f"{intent.value}" for intent in AssistantIntent])

_INTENT_EXTRACT_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """
        You are the routing brain of the DDME assistant.

        Map the user's message to the following intents and extract any arguments:
        {intent_list}

        There can be multiple intents in the same message.

        • If the user explicitly asks to add / include a place/s, use AddPlace and return {{"place": "<name>"}}.
        • IMPORTANT: Location queries like "Where's X?", "Show me X", "Find X" where X is a place name should use AddPlace with {{"place": "<name>"}}.
        • For general place information requests like "Tell me about X", "What do you know about X", "Information about X", "Describe X" where X is a place name, use PlaceInfo with {{"place": "<name>"}}.
        • For "What about X?" queries, analyze the context:
            - If X appears to be asking for information/explanation about a concept, statistical category, or data theme, use DescribeTheme
            - If X is clearly a geographic location (city, region, etc.) and asking for general information, use PlaceInfo
            - If X is a geographic location and asking to add it to selection, use AddPlace
            - Consider the conversational context and what type of information the user is seeking
        • If they ask to remove a place/s, RemovePlace with {{"place": "<name>"}}.
        • CRITICAL: If they mention a postcode (UK format like "SW1A 1AA", "M1 1AE", "OX1 3QD"), treat it as AddPlace with {{"place": "<code>", "postcode": "<code>"}}.
        • If they request a statistical topic, use AddTheme with {{"theme_query": "<words from user>"}}.
        • If they ask to change the theme, switch themes, or want different data categories, use AddTheme with {{"theme_query": "<words from user>"}}.
        • IMPORTANT: "Change theme to X", "switch to X", "use X theme", "use X statistics", "back to X", "set theme to X" are ALL AddTheme intents, NOT RemoveTheme or ListThemes.
        • CRITICAL: If the query contains "stats", "data", or "statistics" (with or without "for") and place names, you MUST extract:
            - AddPlace for EACH place mentioned separately: {{"place": "<place_name>"}}
            - AddTheme for the theme: {{"theme_query": "<theme_words>"}}
        • IMPORTANT: Even simple queries like "stats for london" must extract BOTH AddPlace {{"place": "london"}} AND AddTheme (even if no specific theme is mentioned)
        • This applies to ALL patterns including but not limited to:
            - "[theme] stats for [places]"
            - "get [theme] data for [places]"
            - "please get [theme] statistics for [places]"
            - "show [theme] stats for [places]"
            - ANY variation with stats/data/statistics and place names
            - "fetch [theme] data for [places]"
            - "display [theme] statistics for [places]"
        • CRITICAL: Always look for ALL place names/city names in requests, especially in patterns like "[THEME] [stats/data/statistics] for [PLACE1] and [PLACE2]".
        • CRITICAL: When you see "place1 and place2", extract BOTH as separate AddPlace intents: AddPlace {{"place": "place1"}}, AddPlace {{"place": "place2"}}
        • CRITICAL: Extract each place as a separate AddPlace intent AND extract the theme as AddTheme.
        • DescribeTheme is ONLY for asking about theme definitions/descriptions, like "What is the Population theme?", "Explain Housing statistics", NOT for place queries.
        • RemoveTheme is ONLY for explicitly clearing/removing themes, like "remove the theme", "clear theme", "no theme". NOT for changing themes.
        • For state inspection requests ("what have I selected?", "show my current selection") use ShowState.
        • Listing intents:
            - ListThemes: list themes (automatically shows available themes for current selection if any, otherwise all themes) - use for "what statistics", "what themes", "what data", "what's available", "show all themes", "list themes", "what other statistics"
        • The phrase "start over" maps to Reset.
        • Anything else: Chat.  Set arguments.text to the assistant's normal reply.

        EXAMPLES:
        • "Where's [TownX]?" →
        AddPlace {{"place": "TownX"}}

        • "Show me [CityAlpha]" →
        AddPlace {{"place": "CityAlpha"}}

        • "Find [VillageDelta]" →
        AddPlace {{"place": "VillageDelta"}}

        • "SW1A 1AA" →
        AddPlace {{"place": "SW1A 1AA", "postcode": "SW1A 1AA"}}

        • "show data for M1 1AE" →
        AddPlace {{"place": "M1 1AE", "postcode": "M1 1AE"}}, AddTheme {{"theme_query": "data"}}

        • "population stats for OX1 3QD" →
        AddPlace {{"place": "OX1 3QD", "postcode": "OX1 3QD"}}, AddTheme {{"theme_query": "population"}}

        • "What about [RegionOmega]?" →
        AddPlace {{"place": "RegionOmega"}}

        • "Tell me about [TownX]" →
        PlaceInfo {{"place": "TownX"}}

        • "What do you know about [CityDelta]?" →
        PlaceInfo {{"place": "CityDelta"}}

        • "Where is [CityBeta]?" →
        PlaceInfo {{"place": "CityBeta"}}

        • "Information about [VillageGamma]" →
        PlaceInfo {{"place": "VillageGamma"}}

        • "The place, [TownX]" →
        AddPlace {{"place": "TownX"}}

        • "Please show [ThemeOne] stats for [CityBeta] and [CityGamma]" →
        AddPlace {{"place": "CityBeta"}}, AddPlace {{"place": "CityGamma"}}, AddTheme {{"theme_query": "ThemeOne"}}

        • "please get population stats for portsmouth and newport" →
        AddPlace {{"place": "portsmouth"}}, AddPlace {{"place": "newport"}}, AddTheme {{"theme_query": "population"}}

        • "[ThemeTwo] data for [MetroOne]" →
        AddPlace {{"place": "MetroOne"}}, AddTheme {{"theme_query": "ThemeTwo"}}

        • "[ThemeThree] stats for [CityDelta] and [CityEpsilon]" →
        AddPlace {{"place": "CityDelta"}}, AddPlace {{"place": "CityEpsilon"}}, AddTheme {{"theme_query": "ThemeThree"}}

        • "population stats for portsmouth and newport" →
        AddPlace {{"place": "portsmouth"}}, AddPlace {{"place": "newport"}}, AddTheme {{"theme_query": "population"}}

        • "education data for birmingham and leeds" →
        AddPlace {{"place": "birmingham"}}, AddPlace {{"place": "leeds"}}, AddTheme {{"theme_query": "education"}}

        • "fetch education data for aberdeen and inverness" →
        AddPlace {{"place": "aberdeen"}}, AddPlace {{"place": "inverness"}}, AddTheme {{"theme_query": "education"}}

        • "stats for london" →
        AddPlace {{"place": "london"}}, AddTheme {{"theme_query": "stats"}}

        • "use employment statistics" →
        AddTheme {{"theme_query": "employment statistics"}}

        • "housing stats for my selected places and also add birmingham" →
        AddPlace {{"place": "birmingham"}}, AddTheme {{"theme_query": "housing"}}

        • "housing statistics for london" →
        AddPlace {{"place": "london"}}, AddTheme {{"theme_query": "housing"}}

        • "Add [MunicipalityAlpha] and [MunicipalityBeta]" →
        AddPlace {{"place": "MunicipalityAlpha"}}, AddPlace {{"place": "MunicipalityBeta"}}

        • "Change the theme to [ThemeThree]" →
        AddTheme {{"theme_query": "ThemeThree"}}

        • "Can you switch to [ThemeFour] theme?" →
        AddTheme {{"theme_query": "ThemeFour"}}

        • "Back to [ThemeTwo] data" →
        AddTheme {{"theme_query": "ThemeTwo"}}

        • "What is the [ThemeFive] theme?" →
        DescribeTheme {{"theme": "ThemeFive"}}

        • "Explain [ThemeSix] statistics" →
        DescribeTheme {{"theme": "ThemeSix"}}

        • "What other statistics do you have?" →
        ListThemes {{}}

        • "What themes are available?" →
        ListThemes {{}}

        • "Show me all available data" →
        ListThemes {{}}

        • "List all themes" →
        ListThemes {{}}

        • "Remove the current theme" →
        RemoveTheme {{}}

        You MUST reply with valid JSON in this exact format:
        {{ "intents": [ {{ "intent": "<intent_name>", "arguments": {{ }} }} ] }} }}

        Do NOT include any other text, explanations, or formatting. ONLY return the JSON object.

        Previous conversation:
        {history}

        """),
    (
        "user",
        "{text}"
    ),
])

# Create a structured output chain instead of using format="json"
_intent_extraction_chain = _INTENT_EXTRACT_PROMPT | _intent_llm.with_structured_output(
    schema=AssistantIntentPayload
)

def extract_intent(user_text: str, messages: list[AnyMessage]) -> AssistantIntentPayload:
    """Extract intents from `user_text` using specialized subagents.

    Args:
        user_text: The latest user utterance to classify.
        messages: Recent conversation history (may be used by subagents).

    Returns:
        AssistantIntentPayload: One or more intents with minimal arguments ready
        for workflow routing.
    """
    from .intent_subagents import extract_intent_with_subagents
    logger.info("Using subagent approach for intent extraction")
    return extract_intent_with_subagents(user_text, messages)
