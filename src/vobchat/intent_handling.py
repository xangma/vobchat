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
    """ Example: { "intent": "AddPlace", "arguments": {"place": "London"} } """
    intents: List[SingleIntent]


# -------------------------------------------------------------------------------------
# 3.  Prompt + chain to extract the intent
# -------------------------------------------------------------------------------------

_MODEL_NAME = "deepseek-r1-wt:latest"  # keep in sync with workflow.py
_BASE_URL = "http://localhost:11434/"

# CRITICAL: Use a separate non-streaming LLM instance for intent extraction
# This prevents the JSON parsing from getting stuck in streaming mode
_intent_llm = ChatOllama(
    model=_MODEL_NAME,
    base_url=_BASE_URL,
    temperature=0.0
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
        • For "What about X?" queries, analyze the context:
            - If X appears to be asking for information/explanation about a concept, statistical category, or data theme, use DescribeTheme
            - If X is clearly a geographic location (city, region, etc.), use AddPlace
            - Consider the conversational context and what type of information the user is seeking
        • If they ask to remove a place/s, RemovePlace with {{"place": "<name>"}}.
        • If they mention a postcode, treat it as AddPlace with {{"postcode": "<code>"}}.
        • If they request a statistical topic, use AddTheme with {{"theme_query": "<words from user>"}}.
        • If they ask to change the theme, switch themes, or want different data categories, use AddTheme with {{"theme_query": "<words from user>"}}.
        • IMPORTANT: "Change theme to X", "switch to X", "use X theme", "back to X", "set theme to X" are ALL AddTheme intents, NOT RemoveTheme.
        • If they ask for "[theme] stats for [places]" or "show [theme] data for [places]", extract BOTH:
            - AddPlace for EACH place mentioned separately: {{"place": "<place_name>"}}
            - AddTheme for the theme: {{"theme_query": "<theme_words>"}}
        • IMPORTANT: Always look for place names/city names in requests, even if they ask for data or stats "for" those places. Extract each place as a separate AddPlace intent.
        • DescribeTheme is ONLY for asking about theme definitions/descriptions, like "What is the Population theme?", "Explain Housing statistics", NOT for place queries.
        • RemoveTheme is ONLY for explicitly clearing/removing themes, like "remove the theme", "clear theme", "no theme". NOT for changing themes.
        • For state inspection requests ("what have I selected?", "show my current selection") use ShowState.
        • Listing intents:
            - ListThemesForSelection: list themes *available for the current selection*
            - ListAllThemes: list all themes in the DB - use for "what statistics", "what themes", "what data", "what's available", "show all themes", "list themes", "what other statistics"
        • The phrase "start over" maps to Reset.
        • Anything else: Chat.  Set arguments.text to the assistant's normal reply.

        EXAMPLES:
        • "Where's newport?" →
          AddPlace {{"place": "newport"}}
        • "Show me Bristol" →
          AddPlace {{"place": "Bristol"}}
        • "Find Oxford" →
          AddPlace {{"place": "Oxford"}}
        • "What about Birmingham?" →
          AddPlace {{"place": "Birmingham"}}
        • "The place, newport" →
          AddPlace {{"place": "newport"}}
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
        • "What is the Population theme?" →
          DescribeTheme {{"theme": "Population"}}
        • "Explain Housing statistics" →
          DescribeTheme {{"theme": "Housing"}}
        • "What other statistics do you have?" →
          ListAllThemes {{}}
        • "What themes are available?" →
          ListAllThemes {{}}
        • "Show me all available data" →
          ListAllThemes {{}}
        • "List all themes" →
          ListAllThemes {{}}
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
    """
    Call the LLM, read the raw text, then parse / validate it with Pydantic.
    If the model doesn't give valid JSON we fall back to the Chat intent.
    """

    # 1. Use the structured output chain to get direct Pydantic object
    try:
        history_snippet = "\n".join(str(m.content) for m in messages[:-1])   # Convert to string and tune N

        # 2. get the LLM's reply using structured output chain
        logger.info(f"Extracting intent for user text: '{user_text}'")

        try:
            # CRITICAL: Use structured output chain to avoid JSON parsing issues
            import time
            start_time = time.time()
            logger.info("Starting LLM intent extraction using structured output chain")

            # Invoke the chain directly with the input parameters
            intent_payload = _intent_extraction_chain.invoke({
                "intent_list": intent_list,
                "history": history_snippet,
                "text": user_text,
            })

            end_time = time.time()
            logger.info(f"LLM intent extraction completed in {end_time - start_time:.2f}s")

            # Ensure we got a proper AssistantIntentPayload object
            if isinstance(intent_payload, AssistantIntentPayload):
                logger.info(f"Extracted intents: {[intent.intent.value for intent in intent_payload.intents]}")
                logger.info(f"Full intent payload: {intent_payload.model_dump()}")
                return intent_payload
            else:
                logger.warning(f"Structured output returned unexpected type: {type(intent_payload)}")
                # Fallback to Chat intent
                return AssistantIntentPayload(
                    intents=[SingleIntent(intent=AssistantIntent.CHAT, arguments={"text": "I'm having trouble understanding. Could you please rephrase your request?"})],
                )
        except Exception as llm_error:
            logger.error(f"LLM invocation failed: {llm_error}")
            # Fallback to Chat intent if LLM fails
            return AssistantIntentPayload(
                intents=[SingleIntent(intent=AssistantIntent.CHAT, arguments={"text": f"I'm having trouble processing your request: {user_text}. Could you please try rephrasing?"})],
            )
    except Exception as prompt_error:
        logger.error(f"Error building prompt: {prompt_error}")
        # Fallback to Chat intent if prompt building fails
        return AssistantIntentPayload(
            intents=[SingleIntent(intent=AssistantIntent.CHAT, arguments={"text": "I'm having trouble understanding. Could you please rephrase your request?"})],
        )
