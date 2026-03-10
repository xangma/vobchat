"""Intent routing schema and LLM-based extraction.

Defines the canonical `AssistantIntent` enum, the structured payload returned by
the intent extractor, and the chain/prompt used to classify user input. The
`extract_intent` entry point delegates to the subagent-based extractor in
`intent_subagents.py` for better robustness, but keeps the original prompt and
structured-output chain available for reference or fallback.
"""

from enum import Enum
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from langchain_core.messages import AnyMessage
from vobchat.llm_factory import get_llm
from .configure_logging import get_llm_callback
import logging

logger = logging.getLogger(__name__)
# -------------------------------------------------------------------------------------
# 1.  AssistantIntent enum - canonical names routed inside the graph
# -------------------------------------------------------------------------------------


class AssistantIntent(str, Enum):
    """Canonical intents that the agent routes inside the graph."""

    DESCRIBE_THEME = "DescribeTheme"
    DATA_ENTITY_INFO = "DataEntityInfo"
    EXPLAIN_VISIBLE_DATA = "ExplainVisibleData"
    ADD_PLACE = "AddPlace"
    REMOVE_PLACE = "RemovePlace"
    ADD_THEME = "AddTheme"
    REMOVE_THEME = "RemoveTheme"
    SHOW_STATE = "ShowState"
    LIST_ALL_THEMES = "ListThemes"
    PLACE_INFO = "PlaceInfo"  # general information about a place
    RESET = "Reset"
    CHAT = "Chat"  # free-form response - no state mutation
    UNIT_TYPE_INFO = "UnitTypeInfo"  # describe a unit type (by code or label)


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
        {"intents": [{"intent": "AddPlace", "arguments": {"place": "<place_name>"}}]}
    """

    intents: List[SingleIntent]
