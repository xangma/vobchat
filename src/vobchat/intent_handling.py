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

_MODEL_NAME = "deepseek-r1-wt:latest"  # keep in sync with workflow.py
_BASE_URL = "http://localhost:11434/"

_llm = ChatOllama(model=_MODEL_NAME, base_url=_BASE_URL, format="json", temperature=0.0, max_tokens=512)

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
        • If they ask to change the theme, switch themes, or want different data categories, use AddTheme with {{"theme_query": "<words from user>"}}.
        • IMPORTANT: "Change theme to X", "switch to X", "use X theme", "back to X", "set theme to X" are ALL AddTheme intents, NOT RemoveTheme.
        • If they ask for "[theme] stats for [places]" or "show [theme] data for [places]", extract BOTH:
            - AddPlace for EACH place mentioned separately: {{"place": "<place_name>"}}
            - AddTheme for the theme: {{"theme_query": "<theme_words>"}}
        • IMPORTANT: Always look for place names/city names in requests, even if they ask for data or stats "for" those places. Extract each place as a separate AddPlace intent.
        • If they ask what a theme is, use DescribeTheme with {{"theme": "<name>"}}.
        • RemoveTheme is ONLY for explicitly clearing/removing themes, like "remove the theme", "clear theme", "no theme". NOT for changing themes.
        • For state inspection requests ("what have I selected?", "show my current selection") use ShowState.
        • Listing intents:
            - ListThemesForSelection: list themes *available for the current selection*
            - ListAllThemes: list all themes in the DB
        • The phrase "start over" maps to Reset.
        • Anything else: Chat.  Set arguments.text to the assistant's normal reply.

        EXAMPLES:
        • "Please show Life & Death stats for Southampton and Portsmouth" →
          AddPlace {{"place": "Southampton"}}, AddPlace {{"place": "Portsmouth"}}, AddTheme {{"theme_query": "Life & Death"}}
        • "Population data for London" →
          AddPlace {{"place": "London"}}, AddTheme {{"theme_query": "Population"}}
        • "Add Manchester and Leeds" →
          AddPlace {{"place": "Manchester"}}, AddPlace {{"place": "Leeds"}}
        • "Change the theme to population" →
          AddTheme {{"theme_query": "population"}}
        • "Can you switch to life & death theme?" →
          AddTheme {{"theme_query": "life & death"}}
        • "Back to population data" →
          AddTheme {{"theme_query": "population"}}
        • "Remove the current theme" →
          RemoveTheme {{}}

        You MUST reply with valid JSON in this exact format:
        {{ "intents": [ {{ "intent": "<intent_name>", "arguments": {{ }} }} ] }}

        Do NOT include any other text, explanations, or formatting. ONLY return the JSON object.

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
    logger.info(f"Raw LLM response: '{raw}'")

    # 3. try JSON → pydantic
    try:
        if not raw:
            logger.warning("LLM returned empty response")
            # If empty response, fall back to Chat intent
            return AssistantIntentPayload(
                intents=[SingleIntent(intent=AssistantIntent.CHAT, arguments={"text": "I'm having trouble understanding. Could you please rephrase your request?"})],
            )

        data = json.loads(raw)
        logger.info(f"Parsed LLM reply: {data}")

        # Validate that we have intents
        if not data or "intents" not in data or not data["intents"]:
            logger.warning("LLM reply missing 'intents' field or empty intents")
            return AssistantIntentPayload(
                intents=[SingleIntent(intent=AssistantIntent.CHAT, arguments={"text": "I'm having trouble understanding. Could you please rephrase your request?"})],
            )

        return AssistantIntentPayload.model_validate(data)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse LLM response as JSON: {e}. Raw response: '{raw}'")
        # fallback: treat entire reply as a free-form assistant answer
        return AssistantIntentPayload(
            intents=[SingleIntent(intent=AssistantIntent.CHAT, arguments={"text": raw})],
        )
    except Exception as e:
        logger.error(f"Error processing LLM response: {e}. Raw response: '{raw}'")
        return AssistantIntentPayload(
            intents=[SingleIntent(intent=AssistantIntent.CHAT, arguments={"text": "I'm having trouble understanding. Could you please rephrase your request?"})],
        )
