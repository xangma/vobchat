from enum import Enum
from typing import Optional, List, Dict, Any
import json
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
    DESCRIBE_THEME = "DescribeTheme"
    ADD_PLACE = "AddPlace"
    REMOVE_PLACE = "RemovePlace"
    ADD_THEME = "AddTheme"
    REMOVE_THEME = "RemoveTheme"
    SHOW_STATE = "ShowState"
    LIST_SELECTION_THEMES = "ListThemesForSelection"
    LIST_ALL_THEMES = "ListAllThemes"
    RESET = "Reset"
    CHAT = "Chat"  # free-form response - no state mutation


# -------------------------------------------------------------------------------------
# 2.  Structured LLM response schema
# -------------------------------------------------------------------------------------

# One payload per intent …
class SingleIntent(BaseModel):
    intent: AssistantIntent = Field(..., description="Name of the intent recognised in the user utterance.")
    arguments: Dict[str, Any] = Field(
        default_factory=dict,
        description="For AddPlace either {'place': str} or {'places': list[str]}",
   )
class AssistantIntentPayload(BaseModel):
    """Minimal contract returned by agent-LLM before routing."""
    """ Example: {"intent": "AddPlace", "arguments": {"place": "London"}} """
    intents: List[SingleIntent]


# -------------------------------------------------------------------------------------
# 3.  Prompt + chain to extract the intent
# -------------------------------------------------------------------------------------

_MODEL_NAME = "llama3.3:latest"  # keep in sync with workflow.py
_BASE_URL = "https://148.197.150.162/ollama_api/"

_llm = ChatOllama(model=_MODEL_NAME, base_url=_BASE_URL, client_kwargs={"verify": False})

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
        • If they ask to remove a place/s, RemovePlace with {{"place": "<name>"}}.   
        • If they mention a postcode, treat it as AddPlace with {{"postcode": "<code>"}}.  
        • If they request a statistical topic, use AddTheme with {{"theme_query": "<words from user>"}}.
        • If they ask what a theme is, use DescribeTheme with {{"theme": "<name>"}}. 
        • If they ask to clear the current theme, use RemoveTheme.  
        • For state inspection requests ("what have I selected?", "show my current selection") use ShowState.  
        • Listing intents:  
            - ListThemesForSelection: list themes *available for the current selection*  
            - ListAllThemes: list all themes in the DB
        • The phrase "start over" maps to Reset.  
        • Anything else: Chat.  Set arguments.text to the assistant's normal reply.

        Reply **only** with JSON matching this schema:
        {{"intents": [
            {{"intent": <intent string>, "arguments": <object>}}, ...
        ]
        }}
                
        Previous conversation:
        {history}
        
        """),
    (
        "user", 
        "{text}"
    ),
])

def extract_intent(user_text: str, messages: list[AnyMessage]) -> AssistantIntentPayload:
    """
    Call the LLM, read the raw text, then parse / validate it with Pydantic.
    If the model doesn't give valid JSON we fall back to the Chat intent.
    """
    
    # 1. build the prompt (template already has {intent_list} substituted)

    history_snippet = "\n".join(m.content for m in messages[:-1])   # tune N
    messages_llm = _INTENT_EXTRACT_PROMPT.format_messages(
        intent_list=intent_list,
        history=history_snippet,
        text=user_text,
    )

    # 2. get the LLM's reply
    llm_reply: AIMessage = _llm.invoke(messages_llm)     # returns AIMessage
    raw = llm_reply.content.strip()

    # 3. try JSON → pydantic
    try:
        data = json.loads(raw)
        logger.info(f"LLM reply: {data}")
        return AssistantIntentPayload.model_validate(data)
    except Exception:
        # fallback: treat entire reply as a free-form assistant answer
        return AssistantIntentPayload(
            intents=[SingleIntent(intent=AssistantIntent.CHAT, arguments={"text": raw})],
        )