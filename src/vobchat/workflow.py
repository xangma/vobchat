# app/workflow.py

# -------------------------------
# Import standard libraries and type hints
# -------------------------------
from typing import Annotated, Optional, List
import io
import json
import re  # For regular expression operations (e.g., postcode validation)
import pandas as pd  # For data manipulation, primarily with database results
from typing_extensions import TypedDict  # For defining the structure of the workflow state
import logging  # For logging information and debugging
# Import constant definitions for themes from a local utility module
from vobchat.utils.constants import UNIT_TYPES, UNIT_THEMES

# -------------------------------
# Import Pydantic for data validation and models
# -------------------------------
# Used to define structured data models, especially for LLM outputs
from pydantic import BaseModel, Field

# -------------------------------
# Import LangChain and LangGraph modules
# -------------------------------
from langgraph.graph import END, StateGraph, START  # Core components for building the graph
from langgraph.graph.message import AnyMessage, add_messages  # For handling messages in the state
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit  # For interacting with SQL databases
from langchain_openai import ChatOpenAI  # OpenAI LLM integration (if used)
from langchain_ollama import ChatOllama  # Ollama LLM integration (used here)
from langchain_core.runnables import RunnableConfig  # For configuring LangChain runnables
from langgraph.checkpoint.memory import MemorySaver  # Basic in-memory checkpointer (not used here)
# Core message types used in LangChain/LangGraph conversations
from langchain_core.messages import SystemMessage, AIMessage, HumanMessage, ToolMessage, AnyMessage
from langchain_core.prompts import ChatPromptTemplate  # For creating prompts for the LLM
from langgraph.types import interrupt, Command  # For interrupting the graph execution and controlling flow
from langchain_core.runnables.graph import MermaidDrawMethod  # For generating graph visualizations

# -------------------------------
# Import local modules (configuration, DB setup, tools, etc.)
# -------------------------------
from vobchat.config import load_config, get_db  # Functions to load app config and get DB connection
from vobchat.tools import (  # Custom functions to interact with the database/data
    find_cubes_for_unit_theme,
    find_units_by_postcode,
    find_themes_for_unit,
    find_places_by_name,
    get_all_themes
)
# Import Redis checkpointer for persistent state saving
from vobchat.utils.redis_checkpoint import RedisSaver, AsyncRedisSaver
from redis.asyncio import Redis  # Asynchronous Redis client
import asyncio  # For running asynchronous operations (like Redis interaction)
from vobchat.state_nodes import (
    ShowState_node, ListThemesForSelection_node,
    ListAllThemes_node, Reset_node,
    AddPlace_node, RemovePlace_node,
    AddTheme_node, RemoveTheme_node,
    DescribeTheme_node,
    theme_hint_node,
    ask_followup_node

)
from vobchat.agent_routing import agent_node  # Main entry point for user interactions
from vobchat.intent_handling import AssistantIntent  # Enum for routing intents
from vobchat.state_schema import lg_State  # TypedDict for the workflow state

# -------------------------------
# Set up logging for debugging and informational messages
# -------------------------------
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------
# CONFIGURATION & SETUP
# ----------------------------------------------------------------------------------------

logger.info("Loading configuration and initializing components...")

# Load application configuration (e.g., database credentials, API keys)
config = load_config()
# Get a database connection/engine based on the loaded configuration
db = get_db(config)


# Initialize memory saver for checkpointing the workflow state.
# NOTE: Although initialized, the Redis checkpointer is used in compilation later.
logger.debug("Initializing memory saver for checkpointing...")
memory = MemorySaver() # This instance isn't actually used later, AsyncRedisSaver is.

# Initialize the language model (ChatOllama in this case)
# Specifies the model name and the API endpoint for the Ollama service.
logger.info("Initializing language model...")
model = ChatOllama(
    model="deepseek-r1-wt:latest",  # The specific Ollama model to use
    base_url="http://localhost:11434/",  # URL of the Ollama API server
    # base_url="https://148.197.150.162/ollama_api/",  # URL of the Ollama API server
    # client_kwargs={"verify": False}  # Disables SSL verification if needed (use cautiously)
)

# Set up the SQL toolkit using the database connection and the LLM.
# This toolkit provides tools for the LLM to interact with the database (list tables, get schema, run queries).
logger.info("Setting up database toolkit and tools...")
toolkit = SQLDatabaseToolkit(db=db, llm=model)
# Get the list of tools provided by the toolkit.
tools = toolkit.get_tools()

# Extract specific, frequently used tools from the toolkit list by their names.
# This makes them easier to call directly if needed (though not explicitly used later in this code).
list_tables_tool = next(
    tool for tool in tools if tool.name == "sql_db_list_tables")
get_schema_tool = next(tool for tool in tools if tool.name == "sql_db_schema")

# -------------------------------
# Define a regex for UK postcodes
# -------------------------------
# This pattern is used to identify UK postcodes in user input.
logger.debug("Initializing UK postcode regex pattern...")
postcode_regex = (
    r"([Gg][Ii][Rr] 0[Aa]{2})|"  # GIR 0AA
    r"((([A-Za-z][0-9]{1,2})|"  # A9, A99
    r"(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|"  # AA9, AA99
    r"(([A-Za-z][0-9][A-Za-z])|"  # A9A
    r"([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))"  # AA9A, AA9?
    r"))\s?[0-9][A-Za-z]{2})"  # Optional space + 9AA
)

# ----------------------------------------------------------------------------------------
# CHAINS AND PYDANTIC MODELS FOR STRUCTURED OUTPUT
# ----------------------------------------------------------------------------------------

# Define a Pydantic model to structure the information extracted from the user's initial query.
# Ensures the LLM returns data in a predictable format.
class UserQuery(BaseModel):
    # `places`: Mandatory list of place names identified.
    places: List[str] = Field(
        ..., description="A list of place names mentioned in the user query"
    )
    # `counties`: Optional list of corresponding county codes/names.
    counties: Optional[List[str]] = Field(
        default=[], description="A list of county codes corresponding to the places (if any)"
    )
    # `theme`: Optional statistical theme requested.
    theme: Optional[str] = Field(
        default=None,
        description="The statistics theme requested by the user (e.g. population)"
    )
    # `min_year`: Optional start year for data.
    min_year: Optional[int] = Field(
        default=None, description="The start year for the statistics"
    )
    # `max_year`: Optional end year for data.
    max_year: Optional[int] = Field(
        default=None, description="The end year for the statistics"
    )


# Create a prompt template for the LLM to guide the extraction process based on the UserQuery model.
# The extraction prompt instructs the model to extract lists specifically.
initial_query_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are an expert extraction algorithm. Only extract the following variables from the text: "
        "places (as a list of place names), counties (as a list, if mentioned), theme, min_year, and max_year. "
        "Return null or an empty list for any variable that is not mentioned."
    ),
    ("user", "{text}")  # Placeholder for the user's input message
])

# Create a LangChain "chain" that combines the prompt and the LLM.
# `.with_structured_output(UserQuery)` forces the LLM to return a JSON object matching the UserQuery model.
initial_query_chain = initial_query_prompt | model.with_structured_output(
    schema=UserQuery
)

# -------------------------------
# Define a Pydantic model for theme decision output
# -------------------------------
# Ensures the LLM returns a valid theme code from the predefined list.
class ThemeDecision(BaseModel):
    theme_code: str = Field(...,
                            description="The selected theme code from UNIT_THEMES, e.g. T_POP")


# Create a prompt template for the LLM to choose the most relevant theme code based on the user's question.
choose_theme_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are an expert in determining the appropriate statistical theme based on a user's question."
    ),
    (
        "system",
        # Dynamically include the available theme codes and descriptions in the prompt context.
        "The available themes are:\n" +
        "\n".join([f"{k}: {v}" for k, v in UNIT_THEMES.items()]) +
        "\n\n" +
        "Please try to match similar sentiments, for example, a user might ask for \"education statistics\" or \"qualification data\", where the closest statistical theme may be \"Learning & Language\"."
    ),
    (
        "user",
        "User Question: {question}\n" # Placeholder for the user's query
        "Please output a JSON object with the field 'theme_code' set to one of the above available theme codes."
    )
])
# Chain the theme decision prompt with the model for structured output using the ThemeDecision schema.
choose_theme_chain = choose_theme_prompt | model.with_structured_output(
    schema=ThemeDecision)

def postcode_tool_call(state: lg_State) -> lg_State:
    """
    If a postcode was previously extracted (`extracted_postcode` is set), this node calls
    the `find_units_by_postcode` tool to search the database for matching geographical units.
    Updates the state with the search results (`selected_place`, `selected_place_g_units`, etc.).
    """
    logger.info("Starting postcode tool call...")
    state["current_node"] = "postcode_tool_call"
    logger.debug({"current_state": state})

    # Get the postcode from the state.
    extracted_postcode = state.get("extracted_postcode")
    if not extracted_postcode:
        # If no postcode is present (shouldn't happen if routed correctly, but good practice to check).
        logger.warning("No valid postcode found in state for postcode_tool_call")
        state["messages"].append(
            AIMessage(content="I couldn't find a postcode to search for.")
        )
        return state # Return early

    try:
        logger.info(f"Searching for units with postcode: {extracted_postcode}")
        # Call the database tool function with the postcode.
        response_df = find_units_by_postcode(extracted_postcode)
        logger.debug({"search_results_df": response_df})

        # Check if the database query returned any results.
        if not response_df.empty:
            logger.info("Units found for postcode")
            # Update the state with the details from the first found unit.
            # Converts the first row of the DataFrame to JSON for storage.
            state["selected_place"] = response_df.iloc[0:1].to_json(orient="records") # Store first result row as JSON
            # Initialize lists if they don't exist and append the g_unit and g_place IDs.
            state.setdefault("selected_place_g_units", []).append(
                int(response_df["g_unit"].values[0]))
            state.setdefault("selected_place_g_places", []).append(
                int(response_df["g_place"].values[0]))
            # Potentially add unit type as well if available in response_df
            # state.setdefault("selected_place_g_unit_types", []).append(response_df["g_unit_type"].values[0])
        else:
            # If no units were found, inform the user.
            logger.warning(
                f"No units found for postcode: {extracted_postcode}")
            state["messages"].append(
                AIMessage(content=f"Sorry, I couldn't find any data for the postcode '{extracted_postcode}'.")
            )

    except Exception as e:
        # Handle potential errors during the database call.
        logger.error("Error in postcode tool call", exc_info=True)
        state["messages"].append(
            AIMessage(content=f"Sorry, there was an error looking up the postcode: {str(e)}")
        )

    logger.debug({"updated_state": state})
    return state


def multi_place_tool_call(state: lg_State) -> lg_State:
    """
    Build state["places"] = [
        { "name": str,
          "candidate_rows": [ {...DB row...}, … ],
          "g_place": None, "g_unit": None, "g_unit_type": None }
    ]
    The heavy lifting (disambiguation / map prompt) is done by
    resolve_place_and_unit().
    """
    logger.info("multi_place_tool_call – searching DB for each place")
    place_names = state.get("extracted_place_names", [])
    counties     = state.get("extracted_counties", [])      # may be shorter
    unit_types  = state.get("extracted_unit_types", [])
    polygon_ids = state.get("extracted_polygon_ids", [])   # may be shorter

    places: list[dict] = []

    for idx, place_name in enumerate(place_names):
        county = counties[idx] if idx < len(counties) else "0"
        unit_type = unit_types[idx] if idx < len(unit_types) else "0"
        polygon_id = polygon_ids[idx] if idx < len(polygon_ids) else None
        try:
            df = pd.read_json(
                io.StringIO(
                    find_places_by_name({"place_name": place_name,
                                         "county": county,
                                         "unit_type": unit_type})
                ),
                orient="records",
            )
            candidate_rows = df.to_dict("records")
        except Exception as exc:
            logger.error(f"DB error searching “{place_name}”: {exc}",
                         exc_info=True)
            candidate_rows = []

        places.append({
            "name":            place_name,
            "candidate_rows":  candidate_rows,
            "g_place":         None,
            "unit_rows":       [],        # filled later
            "g_unit":          polygon_id,  # Use polygon_id from map click if available
            "g_unit_type":     unit_type if polygon_id else None,
        })

    state["places"]        = places
    state["place_cursor"]  = 0          # start the loop
    state["selection_idx"] = None       # clear any stale click

    logger.info("multi_place_tool_call: Cleared selection_idx for new place processing")
    return state


    # # Combine all the individual DataFrames into one large DataFrame.
    # if all_results_dfs:
    #     big_df = pd.concat(all_results_dfs, ignore_index=True)
    # else:
    #     # If no results were found for any place, create an empty DataFrame.
    #     logger.warning("No results found for any extracted place names.")
    #     big_df = pd.DataFrame() # Ensure big_df exists even if empty

    # # Store the combined DataFrame as a JSON string in the state.
    # # 'orient="records"' stores it as a list of dictionaries, which is often convenient.
    # state["multi_place_search_df"] = big_df.to_json(orient="records")
    # # Initialize the index for processing these places one by one.
    # # state["current_place_index"] = 0
    # logger.debug(f"Combined place search results stored for {len(place_names)} places.")
    # return state


def select_unit_on_map(state: lg_State) -> lg_State | Command:
    """
    Node intended to trigger map interaction in the frontend.
    It checks if units have been selected for the *most recently processed* place
    (using `current_place_index - 1` because the index was just incremented).
    If the unit hasn't already been added to the map's `selected_polygons` list (which is
    updated by the frontend), it issues an `interrupt`.
    This interrupt signals the frontend (`chat.py`) to:
    1. Potentially highlight or add the corresponding unit polygon(s) to the map.
    2. Wait for the user to potentially click/select polygons on the map.
    3. The frontend map callback updates `map-state` (specifically `selected_polygons`),
       which then triggers the `retrigger_chat_callback` in `chat.py`.
    4. `retrigger_chat_callback` triggers the main `update_chat` callback again, which
       resumes the LangGraph workflow, potentially entering the `decide_if_map_selected` router.

    Args:
        state (lg_State): The current workflow state.

    Returns:
        lg_State: The potentially updated state (though this node primarily interrupts).
    """
    logger.info("Node: select_unit_on_map entered.")
    state["current_node"] = "select_unit_on_map"
    last_intent = state.get("last_intent_payload")
    if last_intent:
        if last_intent.get("intent") == "AddPlace" or last_intent.get("intent") == "RemovePlace":
            # Hand control back to the normal router so e.g. AddPlace_node runs
            logging.info(f"resolve_theme: last_intent_payload set to {last_intent}, returning to agent_node.")
            return Command(goto="agent_node")
    # Get the list of units selected so far by the workflow (place/unit selection nodes).
    selected_workflow_units = state.get("selected_place_g_units", [])
    # Get the list of units selected *by the user on the map* (from frontend state).
    selected_map_polygons_str = [str(p) for p in state.get("selected_polygons", [])] # Ensure string comparison

    # Check if there are any workflow-selected units that need to be added to the map
    if selected_workflow_units:
        # Find all units that are in the workflow but not yet on the map
        missing_units = []
        for i, unit_id in enumerate(selected_workflow_units):
            if str(unit_id) not in selected_map_polygons_str:
                missing_units.append((i, unit_id))

        if missing_units:
            # Take the first missing unit and trigger interrupt for it
            first_missing_index, first_missing_unit = missing_units[0]
            current_place_index = state.get("current_place_index")
            logger.info(f"Unit {first_missing_unit} (index {first_missing_index}) not found in map selections. Issuing interrupt to update map.")
            logger.info(f"select_unit_on_map: DEBUG - current_place_index in state = {current_place_index}")
            logger.info(f"select_unit_on_map: DEBUG - first_missing_index = {first_missing_index}")
            # Issue an interrupt to signal the frontend.
            # IMPORTANT: Must pass current_place_index to preserve it through the interrupt
            interrupt(value={
                 # Message might be displayed or just used internally by frontend.
                # "message": f"Please confirm or select the area for '{state['extracted_place_names'][first_missing_index]}' on the map.",
                 # Pass the current state of selections for context.
                "selected_place_g_places": state.get("selected_place_g_places", []),
                "selected_place_g_units": state.get("selected_place_g_units", []),
                "selected_place_g_unit_types": state.get("selected_place_g_unit_types", []),
                 # Pass the CORRECT current_place_index to preserve state through interrupt
                "current_place_index": current_place_index,
                 # Pass node name for potential resume logic.
                "current_node": "select_unit_on_map",
                # CRITICAL: Clear selection_idx through interrupt to ensure it's persisted
                "selection_idx": None,
            })
            # Execution stops here, waits for frontend map interaction and retrigger.
            return state

    # Only proceed with routing if no interrupt was issued (i.e., all units are on map or no units exist)
    logger.info(f"All workflow units are already selected on map or no units exist. Proceeding with routing.")
    logger.info(f"select_unit_on_map: About to exit and use conditional routing. Current state: current_place_index={state.get('current_place_index')}, extracted_place_names={state.get('extracted_place_names')}")

    # Let the conditional edges handle routing - just return state
    return state


def find_cubes_node(state: lg_State) -> lg_State | Command:
    """
    Retrieves the data‑cubes (statistical datasets) for the **currently selected theme**
    (``state["selected_theme"]``) and every selected geographical unit
    (``selected_place_g_units`` ∪ ``selected_polygons``).

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
    # 1. Early‑exit for AddPlace / RemovePlace intents so the normal router runs
    # ──────────────────────────────────────────────────────────────────────────
    last_intent = state.get("last_intent_payload")
    if last_intent and last_intent.get("intent") in {"AddPlace", "RemovePlace"}:
        logging.info(
            "find_cubes_node: last_intent_payload set to %s, returning to agent_node.",
            last_intent,
        )
        return Command(goto="agent_node")

    # ──────────────────────────────────────────────────────────────────────────
    # 2. Collect the full list of selected geographical‑unit IDs
    # ──────────────────────────────────────────────────────────────────────────
    workflow_units: list[int] = state.get("selected_place_g_units", [])
    map_selected_units_int: list[int] = [
        int(p) for p in state.get("selected_polygons", []) if str(p).isdigit()
    ]
    all_selected_unit_ids: list[int] = sorted(set(workflow_units + map_selected_units_int))

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
        combined_df_list.append(existing_cubes_df)
    combined_df_list.extend(newly_fetched_dfs)

    if not combined_df_list:
        logger.warning(
            "No cube data found for theme '%s' and selected units %s (Years: %s–%s).",
            theme_label,
            all_selected_unit_ids,
            min_year,
            max_year,
        )
        state["messages"].append(
            AIMessage(
                content=f"Sorry, I couldn't find any data matching '{theme_label}' for the specified criteria and selected area(s)."
            )
        )
        return state

    big_cubes_df = pd.concat(combined_df_list, ignore_index=True).drop_duplicates()

    # Before saving back to state, re‑apply year filter *one more time* to ensure consistency.
    if "Start" in big_cubes_df.columns and "End" in big_cubes_df.columns:
        big_cubes_df["Start"] = pd.to_numeric(big_cubes_df["Start"], errors="coerce")
        big_cubes_df["End"] = pd.to_numeric(big_cubes_df["End"], errors="coerce")
        if min_year is not None:
            big_cubes_df = big_cubes_df[big_cubes_df["End"] >= min_year]
        if max_year is not None:
            big_cubes_df = big_cubes_df[big_cubes_df["Start"] <= max_year]

    # Persist the up‑to‑date cubes so future invocations can reuse them
    state["selected_cubes"] = big_cubes_df.to_json(orient="records")

    # CRITICAL: Clear ALL remaining intents since we've reached the final data stage
    original_queue_size = len(state.get("intent_queue", []))
    if original_queue_size > 0:
        state["intent_queue"] = []
        logger.info(f"find_cubes_node: Cleared {original_queue_size} remaining intents from queue (workflow complete)")

    logger.info(
        "Combined %d cube rows across %d units (theme %s).",
        len(big_cubes_df),
        len(all_selected_unit_ids),
        theme_id,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # 7. Notify the front‑end via interrupt
    # ──────────────────────────────────────────────────────────────────────────
    logger.info(f"Emitting cube data interrupt with {len(big_cubes_df)} rows of data")
    interrupt(
        value={
            "message": f"Here is the data for '{theme_label}' across the selected area(s):",
            "cubes": state["selected_cubes"],
            "current_node": "find_cubes_node",
            "last_intent_payload": {},
            # CRITICAL: Clear selection_idx through interrupt to prevent stale values
            "selection_idx": None,
        }
    )

    # The graph pauses after the interrupt; return state for completeness
    return state


def resolve_place_and_unit(state: lg_State) -> lg_State | Command:
    """
    Resolve exactly *one* place per call:
        • disambiguate place name   (may interrupt)
        • disambiguate unit type    (may interrupt)
        • write g_place / g_unit / g_unit_type
    It never mutates state *before* raising an interrupt.
    """
    logger.info("Node: resolve_place_and_unit entered.")
    i       = state.get("current_place_index", 0) or 0
    places  = state.get("places", []) or []

    # Log current state for debugging
    current_selection_idx = state.get("selection_idx")
    logger.info(f"resolve_place_and_unit: Processing place {i}, current selection_idx={current_selection_idx}")

    # SIMPLIFIED: Don't clear numeric selection_idx in resolve_place_and_unit
    # Let the normal selection logic handle it - if it's invalid, it will fall back to defaults
    # The interrupt mechanism already clears selection_idx when new prompts are issued
    logger.info(f"resolve_place_and_unit: selection_idx={current_selection_idx} - letting normal selection logic handle it")

    # done?
    if not places or i >= len(places):
        # All places processed, let conditional edges handle routing
        logger.info(f"resolve_place_and_unit: All places processed, clearing selection_idx and returning state for routing")
        # CRITICAL: Clear selection_idx when all places are processed to prevent stale values in theme processing
        state["selection_idx"] = None
        return state

    place   = places[i].copy()         # work on a private copy

    # If this place is already fully resolved, advance to next place
    if place.get("g_place") is not None and place.get("g_unit") is not None:
        logger.info(f"resolve_place_and_unit: Place {i} ({place['name']}) already resolved, skipping")
        old_index = state.get("current_place_index", 0)
        new_index = i + 1
        state["current_place_index"] = new_index
        logger.info(f"resolve_place_and_unit: UPDATED current_place_index from {old_index} to {new_index}")
        # CRITICAL: Clear selection_idx when skipping resolved places to prevent stale values
        state["selection_idx"] = None
        # Return state to let conditional edges handle routing - this preserves state
        return state

    # ───────────────────────────────────────── place disambiguation
    if place["g_place"] is None:
        rows = place["candidate_rows"]

        # Handle case where no place candidates were found
        if not rows:
            logger.warning(f"No candidate rows found for place '{place['name']}'")
            # Check if this place already has g_unit info (e.g., from polygon click)
            if place.get("g_unit") is not None:
                logger.info(f"Place '{place['name']}' has g_unit {place['g_unit']}, proceeding with unit info")
                # This place already has unit info, add to global lists and mark as resolved
                selected_units = state.get("selected_place_g_units", [])
                selected_units.append(place["g_unit"])
                state["selected_place_g_units"] = selected_units

                selected_unit_types = state.get("selected_place_g_unit_types", [])
                selected_unit_types.append(place.get("g_unit_type"))
                state["selected_place_g_unit_types"] = selected_unit_types

                places[i] = place
                state["current_place_index"] = i + 1
                logger.info(f"Added g_unit {place['g_unit']} to selected_place_g_units")
                return state
            else:
                # No place candidates and no existing unit info, skip this place
                logger.warning(f"Skipping place '{place['name']}' - no candidates and no unit info")
                state["current_place_index"] = i + 1
                return state

        multiple_options = len(rows) > 1
        sel_idx = state.get("selection_idx")      # refresh in case callback set it

        if multiple_options and sel_idx is None:
            options = [
                {
                    "option_type": "place",
                    "label": f"{r['g_name']}, {r['county_name']}",
                    "color": "#333",
                    "value": j,
                }
                for j, r in enumerate(rows)
            ]
            interrupt(value={
                "message": f"More than one “{place['name']}”. Please choose:",
                "options": options,
                "current_node": "resolve_place_and_unit",
                "current_place_index": i,
                # CRITICAL: Clear selection_idx through interrupt to prevent stale values
                "selection_idx": None,
            })

        # from here on we **only** fall through if
        #   a) exactly one option  OR
        #   b) user has clicked → selection_idx set
        if multiple_options and sel_idx is None:
            return state          # safety (normally unreachable after interrupt)

        if multiple_options and sel_idx is not None:
            # For place disambiguation, sel_idx should be an index
            try:
                choice = int(sel_idx)
                chosen_row = rows[choice]
            except ValueError:
                # If sel_idx is not a number (e.g., it's a unit type like "LG_DIST"),
                # and we have multiple place options, this means the sel_idx is stale
                # from a previous interaction (unit selection) but we need place selection
                logger.info(f"resolve_place_and_unit: sel_idx '{sel_idx}' is not numeric for place selection, clearing and triggering place disambiguation for '{place['name']}'")
                state["selection_idx"] = None
                # Trigger place disambiguation since we have multiple options
                options = [
                    {
                        "option_type": "place",
                        "label": f"{r['g_name']}, {r['county_name']}",
                        "color": "#333",
                        "value": j,
                    }
                    for j, r in enumerate(rows)
                ]
                interrupt(value={
                    "message": f"More than one \"{place['name']}\". Please choose:",
                    "options": options,
                    "current_node": "resolve_place_and_unit",
                    "current_place_index": i,
                # CRITICAL: Clear selection_idx through interrupt to prevent stale values
                "selection_idx": None,
                })
                return state
        elif multiple_options and sel_idx is None:
            # This is the normal case - show place disambiguation options
            # (This case is already handled above in the earlier if statement)
            pass
        else:
            # Single option or no selection needed - use first option
            logger.info(f"resolve_place_and_unit: Auto-selecting single place option for '{place['name']}': {rows[0]['g_name']}")
            chosen_row = rows[0]

        # commit local
        place["g_place"] = chosen_row["g_place"]
        # explode units into a list[{g_unit, g_unit_type}]
        g_units      = chosen_row["g_unit"]
        g_unit_types = chosen_row["g_unit_type"]
        if not isinstance(g_units, list):
            g_units, g_unit_types = [g_units], [g_unit_types]
        place["unit_rows"] = [
            {"g_unit": u, "g_unit_type": ut}
            for u, ut in zip(g_units, g_unit_types)
        ]
        # Clear selection_idx from state if it was used for place selection
        if multiple_options:
            state["selection_idx"] = None   # consume the click

    # ───────────────────────────────────────── unit disambiguation

    if place["g_unit"] is None:
        urows = place["unit_rows"]
        multiple_options = len(urows) > 1
        sel_idx = state.get("selection_idx")      # refresh in case callback set it
        logger.info(f"resolve_place_and_unit: Unit selection for '{place['name']}': {len(urows)} unit options, sel_idx='{sel_idx}'")

        # Always let users choose unit type - no automatic selection based on patterns

        if multiple_options and sel_idx is None:
            options = [
                {
                    "option_type": "unit",
                    "label": UNIT_TYPES.get(r["g_unit_type"], {})
                                    .get("long_name", r["g_unit_type"]),
                    "color": UNIT_TYPES.get(r["g_unit_type"], {})
                                    .get("color", "#333"),
                    "value": r["g_unit_type"],
                }
                for j, r in enumerate(urows)
            ]

            interrupt(value={
                "message": f"Which geography for “{place['name']}”?",
                "options": options,               #  persisted in state
                "current_node": "resolve_place_and_unit",
                "place_cursor": i,
                # IMPORTANT: Preserve current_place_index through interrupt
                "current_place_index": state.get("current_place_index", 0),
                # CRITICAL: Clear selection_idx through interrupt to prevent stale values
                "selection_idx": None,
            })

        # from here on we **only** fall through if
        #   a) exactly one option  OR
        #   b) user has clicked → selection_idx set
        if multiple_options and sel_idx is None:
            return state          # safety (normally unreachable after interrupt)

        if sel_idx is not None:
            logger.info(f"resolve_place_and_unit: sel_idx={sel_idx}, available unit types: {[r['g_unit_type'] for r in urows]}")
            # Find the unit row that matches the selected unit type
            chosen_unit = next((r for r in urows if r["g_unit_type"] == sel_idx), urows[0])
            logger.info(f"resolve_place_and_unit: chosen_unit for '{place['name']}': {chosen_unit}")
        else:
            chosen_unit = urows[0]
            logger.info(f"resolve_place_and_unit: No sel_idx, using first unit for '{place['name']}': {chosen_unit}")
        place["g_unit"]      = chosen_unit["g_unit"]
        place["g_unit_type"] = chosen_unit["g_unit_type"]
        # friendly confirmation
        long_name = UNIT_TYPES.get(place["g_unit_type"], {}) \
                            .get("long_name", place["g_unit_type"])
        state.setdefault("messages", []).append(
            AIMessage(content=f"Using {long_name} data for “{place['name']}”.")
        )

    # ───────────────────────────────────────── commit + advance
    places[i] = place
    state["places"]               = places
    state["current_place_index"]         = i + 1

    # CRITICAL: Clear selection state for next place and ensure it's persisted via Command
    logger.info(f"resolve_place_and_unit: About to clear selection_idx for next place. Current state: selection_idx={state.get('selection_idx')}, current_place_index={state.get('current_place_index')}")

    # keep legacy flat lists for downstream code (can be removed later)
    state.setdefault("selected_place_g_units", []).append(place["g_unit"])
    state.setdefault("selected_place_g_unit_types", []).append(place["g_unit_type"])
    state.setdefault("selected_place_g_places", []).append(place["g_place"])

    # CRITICAL: Use Command to ensure selection_idx clearing is persisted to checkpointer
    state_update = {
        "places": places,
        "current_place_index": i + 1,
        "selection_idx": None,  # CRITICAL: Clear for next place
        "options": [],  # Clear consumed options
        "selected_place_g_units": state["selected_place_g_units"],
        "selected_place_g_unit_types": state["selected_place_g_unit_types"],
        "selected_place_g_places": state["selected_place_g_places"],
    }

    logger.info(f"resolve_place_and_unit: Completed place {i} ({place['name']}), advancing to place {i + 1}, CLEARED selection_idx via Command update")

    # Use Command with update to ensure state changes are persisted properly
    return Command(goto="select_unit_on_map", update=state_update)


def _theme_already_matches(current_theme_json: str, query: str) -> bool:
    """Check if the extracted theme query already matches the current selected theme."""
    if not current_theme_json or not query:
        return False

    try:
        import json
        theme_data = json.loads(current_theme_json)
        theme_label = theme_data.get("labl", "").lower()
        query_lower = query.lower().strip()

        # Check if the query matches the current theme label
        return query_lower in theme_label or theme_label in query_lower
    except (json.JSONDecodeError, KeyError):
        return False


def resolve_theme(state: lg_State) -> lg_State | Command:
    """Choose a theme and, if no units are known yet, prompt for a place."""
    # ------------------------------------------------------------------
    # Step 0 · CRITICAL: Ensure all places are processed before theme processing
    # ------------------------------------------------------------------
    logging.info("Node: resolve_theme entered.")

    # Check if there are still places being processed
    current_place_index = state.get("current_place_index", 0) or 0
    extracted_place_names = state.get("extracted_place_names", [])
    num_places = len(extracted_place_names)

    if current_place_index < num_places:
        logging.info(f"resolve_theme: Places still being processed ({current_place_index} of {num_places}), returning state to use conditional routing")
        return state

    logging.info(f"resolve_theme: Call details - selected_theme={bool(state.get('selected_theme'))}, extracted_theme='{state.get('extracted_theme')}', units={len(state.get('selected_place_g_units', []))}")
    # Don't set current_node here - only set it when we actually interrupt for theme selection

    # Check intent queue for AddTheme intents and process them
    intent_queue = state.get("intent_queue", [])
    if intent_queue:
        theme_intents = [intent for intent in intent_queue if intent.get("intent") == "AddTheme"]
        if theme_intents:
            # Process the first AddTheme intent
            theme_intent = theme_intents[0]
            theme_query = theme_intent.get("arguments", {}).get("theme_query")
            if theme_query:
                logging.info(f"resolve_theme: Processing AddTheme intent from queue: '{theme_query}'")
                # Set extracted_theme so the normal processing logic handles it
                state["extracted_theme"] = theme_query
                # Remove the processed intent from queue
                remaining_queue = [intent for intent in intent_queue if not (intent.get("intent") == "AddTheme" and intent.get("arguments", {}).get("theme_query") == theme_query)]
                state["intent_queue"] = remaining_queue
                logging.info(f"resolve_theme: Removed AddTheme intent from queue, {len(remaining_queue)} intents remaining")

    last_intent = state.get("last_intent_payload")
    if last_intent:
        if last_intent.get("intent") == "AddPlace" or last_intent.get("intent") == "RemovePlace":
            # Hand control back to the normal router so e.g. AddPlace_node runs
            logging.info(f"resolve_theme: last_intent_payload set to {last_intent}, returning to agent_node.")
            return Command(goto="agent_node")

    units = state.get("selected_place_g_units", []) + [
        int(p) for p in state.get("selected_polygons", []) if str(p).isdigit()
    ]

    # ------------------------------------------------------------------
    # Step 1 · Build the ‹available› theme list
    #          → if *no* units yet, fall back to the catalogue
    # ------------------------------------------------------------------
    if units:
        dfs = [
            pd.read_json(io.StringIO(find_themes_for_unit(str(u))), orient="records")
            for u in set(units)
        ]
        available_df = pd.concat(dfs).drop_duplicates("ent_id") if dfs else pd.DataFrame()
    else:
        available_df = pd.read_json(io.StringIO(get_all_themes("")), orient="records")

    if available_df.empty:
        state.setdefault("messages", []).append(
            AIMessage(content="I couldn't find any statistical themes.")
        )
        return state

    available = available_df[["ent_id", "labl"]].to_dict("records")

    # ------------------------------------------------------------------
    # Step 2 · Has a theme been fixed already? (or is a new theme being requested?)
    # ------------------------------------------------------------------
    logger.info(f"resolve_theme: selected_theme={state.get('selected_theme')}, extracted_theme={state.get('extracted_theme')}")

    # Check if we need to process a theme change
    current_theme = state.get("selected_theme")
    extracted_theme_query = state.get("extracted_theme")

    # Early return if theme is already resolved and no new theme query
    if current_theme and not extracted_theme_query:
        logging.info("resolve_theme: Theme already resolved, no new theme query. Returning early.")
        return state

    # Only process theme change if:
    # 1. No theme is selected yet, OR
    # 2. There's a new theme query that differs from current theme
    should_process_theme_change = (
        not current_theme or
        (extracted_theme_query and not _theme_already_matches(current_theme, extracted_theme_query))
    )

    logging.info(f"resolve_theme: should_process_theme_change={should_process_theme_change}, current_theme={bool(current_theme)}, extracted_theme_query='{extracted_theme_query}'")
    if current_theme and extracted_theme_query:
        matches = _theme_already_matches(current_theme, extracted_theme_query)
        logging.info(f"resolve_theme: _theme_already_matches returned {matches}")

    # Check for button click first (takes priority over theme query)
    selection_idx = state.get("selection_idx")
    current_node = state.get("current_node")
    has_theme_options = bool(state.get("options")) and current_node == "resolve_theme"

    logger.info(f"resolve_theme: Checking for theme selection - selection_idx={selection_idx}, current_node={current_node}, has_theme_options={has_theme_options}")

    # 2 b · Button click (prioritize this over theme query)
    # CRITICAL: Only process selection_idx if we're actually in a theme selection context
    if selection_idx is not None and has_theme_options:
        # Validate that this is a numeric theme selection, not a unit type string
        try:
            theme_index = int(selection_idx)
            if 0 <= theme_index < len(available):
                logger.info(f"resolve_theme: Processing valid theme button click selection_idx={selection_idx}")
                state["selected_theme"] = json.dumps(available[theme_index])
                state["selection_idx"] = None
                # Clear interrupt state and extracted theme
                state.pop("options", None)
                state.pop("current_node", None)
                state.pop("extracted_theme", None)  # Clear the extracted theme to prevent re-matching
                logger.info(f"resolve_theme: Button click processed, theme set to: {available[theme_index]['labl']}")
                return state
            else:
                logger.warning(f"resolve_theme: Invalid theme index {theme_index}, clearing selection_idx")
                state["selection_idx"] = None  # Clear invalid selection
        except (ValueError, TypeError):
            logger.info(f"resolve_theme: selection_idx '{selection_idx}' is not a valid theme index, clearing it")
            state["selection_idx"] = None  # Clear invalid selection
    elif selection_idx is not None and not has_theme_options:
        logger.info(f"resolve_theme: Ignoring stale selection_idx={selection_idx} - not in theme selection context (current_node={current_node}, has_options={bool(state.get('options'))})")
        # CRITICAL: Clear stale selection_idx to prevent it from interfering with future operations
        state["selection_idx"] = None

    if should_process_theme_change:
        theme_query = extracted_theme_query or ""
        logger.info(f"resolve_theme: Processing theme change - theme_query='{theme_query}'")

        # 2 a · Simple text matching (fallback when LLM doesn't work)
        if theme_query:
            logger.info(f"resolve_theme: Attempting to match theme_query: '{theme_query}'")
            logger.info(f"resolve_theme: Available themes: {[f'{t['ent_id']}: {t['labl']}' for t in available]}")

            # Simple text matching approach
            query_lower = theme_query.lower().strip()
            chosen = None

            # Direct label matching - check both directions
            for theme in available:
                theme_label_lower = theme["labl"].lower()
                # Check if query contains theme label OR theme label contains query
                if query_lower in theme_label_lower or theme_label_lower in query_lower:
                    chosen = theme
                    logger.info(f"resolve_theme: Found direct match: {theme['labl']}")
                    break

            # If no direct match, try keyword matching
            if not chosen:
                query_words = query_lower.split()
                for theme in available:
                    theme_words = theme["labl"].lower().split()
                    # Check if any query word matches any theme word
                    if any(qword in tword or tword in qword for qword in query_words for tword in theme_words):
                        chosen = theme
                        logger.info(f"resolve_theme: Found keyword match: {theme['labl']}")
                        break

            # If still no match, try LLM-based semantic matching using choose_theme_chain
            if not chosen:
                try:
                    logger.info(f"resolve_theme: No text match found, trying LLM semantic matching for '{theme_query}'")
                    # Use the LLM to semantically match the query to available themes
                    theme_decision = choose_theme_chain.invoke({"question": theme_query})

                    # Extract theme_code, handling both dict and object responses
                    if hasattr(theme_decision, 'theme_code'):
                        theme_code = theme_decision.theme_code
                    elif isinstance(theme_decision, dict):
                        theme_code = theme_decision.get('theme_code')
                    else:
                        logger.warning(f"resolve_theme: Unexpected LLM response format: {type(theme_decision)}")
                        theme_code = None

                    # Find the theme with matching ent_id
                    if theme_code:
                        for theme in available:
                            if theme["ent_id"] == theme_code:
                                chosen = theme
                                logger.info(f"resolve_theme: Found LLM semantic match: '{theme_query}' -> '{theme['labl']}' (LLM chose {theme_code})")
                                break

                        if not chosen:
                            logger.warning(f"resolve_theme: LLM chose theme_code '{theme_code}' but it's not in available themes")
                    else:
                        logger.warning(f"resolve_theme: Could not extract theme_code from LLM response")

                except Exception as e:
                    logger.error(f"resolve_theme: Error in LLM semantic matching: {e}", exc_info=True)

            if chosen:
                state["selected_theme"] = json.dumps(chosen)
                state.setdefault("messages", []).append(
                    AIMessage(content=f"Changed theme to '{chosen['labl']}'")
                )
                # Clear interrupt state and extracted theme
                state.pop("options", None)
                state.pop("current_node", None)
                state.pop("extracted_theme", None)  # Clear the extracted theme to prevent re-matching
                # CRITICAL: Clear selection_idx when theme is automatically matched
                state["selection_idx"] = None
            else:
                # Theme not found - show available themes and clear current selection
                state.setdefault("messages", []).append(
                    AIMessage(content=f"Sorry, I couldn't find a theme matching '{theme_query}'. Let me show you what's available:")
                )
                # Clear the current theme to force theme selection
                state.pop("selected_theme", None)
                # CRITICAL: Clear selection_idx when theme not found to prevent stale values
                state["selection_idx"] = None
                # This will trigger the theme selection UI below

        # 2 c · Need manual choice
        if not state.get("selected_theme"):
            options = [
                {
                    "option_type": "theme",
                    "label": t["labl"],
                    "color": "#333",
                    "value": idx,
                }
                for idx, t in enumerate(available)
            ]
            interrupt(
                value={
                    "message": "Which statistical theme did you have in mind?",
                    "options": options,
                    "current_node": "resolve_theme",
                    # CRITICAL: Clear selection_idx through interrupt to prevent stale values
                    "selection_idx": None,
                }
            )
            return state   # execution pauses here

    # If we reach here, we have a theme - clear extracted_theme to prevent reprocessing
    if state.get("selected_theme") and state.get("extracted_theme"):
        logger.info("resolve_theme: Theme resolved, clearing extracted_theme to prevent reprocessing")
        state.pop("extracted_theme", None)
        state.pop("options", None)
        state.pop("current_node", None)

    # ------------------------------------------------------------------
    # Step 3 · If we now *have* a theme *but* still no units → ask for a place
    # ------------------------------------------------------------------
    if state.get("selected_theme") and not units:
        chosen = pd.read_json(io.StringIO(state["selected_theme"]), typ='series')
        state.setdefault("messages", []).append(
            AIMessage(content=f"Got it – I'll use the **{chosen['labl']}** theme. ")
            )
        # interrupt(
        #     value={
        #         "message": (
        #             f"Got it – I'll use the **{chosen.labl}** theme. "
        #             "Which place or postcode should I fetch it for?"
        #         ),
        #         # "options": [
        #         #     {
        #         #         "option_type": "intent",
        #         #         "label": "Add a place",
        #         #         "value": 0,       # handled by ask_followup_node
        #         #         "color": "#333",
        #         #     }
        #         # ],
        #         "current_node": "resolve_theme",
        #     }
        # )
        state['current_node'] = "resolve_theme"
        state["last_intent_payload"] = {}
        return state                      # wait for user input
    else:
        logger.info("resolve_theme: No theme change needed - already have theme and no new extracted_theme")
    return state



def should_continue_to_themes(state: lg_State) -> str:
    """
    After a place’s geographical unit has been fixed, decide the next step:

    •  If there are still places left to disambiguate  → keep looping.
    •  If every place now has a unit AND a theme is
       already selected                               → jump straight to cubes.
    •  Otherwise                                       → fetch/choose a theme.
    """
    logging.info("Routing: should_continue_to_themes()")
    num_places   = len(state.get("extracted_place_names", []))
    current_index = state.get("current_place_index", 0)
    selected_units = state.get("selected_place_g_units", [])
    units_ready  = len(selected_units) >= num_places > 0
    have_theme   = bool(state.get("selected_theme"))

    logging.info(f"Routing decision: num_places={num_places}, current_index={current_index}, selected_units={len(selected_units)}, have_theme={have_theme}")

    # If no units are selected at all (e.g., after deselection), go to agent_node
    if not selected_units:
        logging.info("Routing to agent_node: no units selected")
        return "agent_node"

    # If no places to process, go to agent_node
    if num_places == 0:
        logging.info("Routing to agent_node: no places to process")
        return "agent_node"

    if current_index >= num_places:
        logging.info("Routing to resolve_theme: all places processed")
        return "resolve_theme"
    else:
        logging.info("Routing to resolve_place_and_unit: more places to process")
        return "resolve_place_and_unit"
# ----------------------------------------------------------------------------------------
# WORKFLOW DEFINITION
# ----------------------------------------------------------------------------------------


def create_workflow(lg_state: TypedDict):
    """
    Constructs and compiles the LangGraph StateGraph.
    - Defines all the nodes.
    - Defines the edges (transitions) between nodes, including conditional edges based on router functions.
    - Compiles the graph with a persistent checkpointer (AsyncRedisSaver).
    - Optionally generates and saves visual diagrams of the graph (ASCII, PNG).

    Args:
        lg_state (TypedDict): The TypedDict class defining the workflow's state structure (lg_State).

    Returns:
        CompiledStateGraph: The compiled LangGraph workflow instance ready for execution.
    """
    logger.info("Creating workflow graph...")
    # Initialize the StateGraph with the defined state structure.
    workflow = StateGraph(lg_state)

    # --- Add Nodes ---
    # Add each node function defined earlier to the graph, associating it with a unique name.
    workflow.add_node("agent_node", agent_node) # General LLM agent
    workflow.add_node("postcode_tool_call", postcode_tool_call) # Handles postcode search
    workflow.add_node("multi_place_tool_call", multi_place_tool_call) # Searches multiple places
    workflow.add_node("select_unit_on_map", select_unit_on_map) # Triggers map interaction (interrupt)
    workflow.add_node("find_cubes_node", find_cubes_node) # Retrieves final data cubes (interrupt)

    workflow.add_node("ShowState_node", ShowState_node)
    workflow.add_node("ListThemesForSelection_node", ListThemesForSelection_node)
    workflow.add_node("ListAllThemes_node", ListAllThemes_node)
    workflow.add_node("Reset_node", Reset_node)
    workflow.add_node("AddPlace_node", AddPlace_node)
    workflow.add_node("RemovePlace_node", RemovePlace_node)
    workflow.add_node("AddTheme_node", AddTheme_node)
    workflow.add_node("RemoveTheme_node", RemoveTheme_node)

    workflow.add_node("DescribeTheme_node", DescribeTheme_node)
    workflow.add_node("ask_followup_node", ask_followup_node)
    workflow.add_node("resolve_place_and_unit", resolve_place_and_unit)

    workflow.add_node("resolve_theme", resolve_theme)

    # agent-edge - single mapping
    # workflow.add_conditional_edges(
    #     "agent_node",
    #     lambda s: (s.get("last_intent_payload") or {}).get("intent") or "NO_INTENT",
    #     {
    #         **{i.value: f"{i.value}_node"
    #         for i in AssistantIntent
    #         if i is not AssistantIntent.CHAT},
    #         AssistantIntent.CHAT.value: END,
    #         "NO_INTENT": "ask_followup_node",
    #     },
    # )
    workflow.add_conditional_edges(
        "agent_node",
        lambda s: (s.get("last_intent_payload") or {}).get("intent") or "NO_INTENT",
        {
            **{i.value: f"{i.value}_node"
            for i in AssistantIntent
            if i is not AssistantIntent.CHAT},
            AssistantIntent.CHAT.value: END,
            "NO_INTENT": END,
        },
    )

    for n in [
        "ShowState_node", "ListThemesForSelection_node", "ListAllThemes_node",
        "DescribeTheme_node", "RemoveTheme_node", "Reset_node"
    ]:
        workflow.add_edge(n, END)


    # --- Define Edges (Workflow Logic) ---

    # START already goes straight to agent_node now
    workflow.add_edge(START, "agent_node")

    workflow.add_edge("multi_place_tool_call", "resolve_place_and_unit")

    # Add conditional edges for resolve_place_and_unit to handle state preservation
    def resolve_place_and_unit_router(state: lg_State) -> str:
        logging.info("==================== resolve_place_and_unit_router CALLED ====================")
        current_place_index = state.get("current_place_index", 0)
        extracted_place_names = state.get("extracted_place_names", [])
        num_places = len(extracted_place_names)
        selected_units = state.get("selected_place_g_units", [])

        logging.info(f"resolve_place_and_unit_router: current_place_index={current_place_index}, num_places={num_places}, selected_units={len(selected_units)}")

        # Always go to select_unit_on_map first to ensure polygon selection
        # select_unit_on_map will handle the routing to themes when all places are done
        logging.info("resolve_place_and_unit_router: Going to select_unit_on_map for polygon selection")
        return "select_unit_on_map"

    workflow.add_conditional_edges(
        "resolve_place_and_unit",
        resolve_place_and_unit_router,
        {
            "select_unit_on_map": "select_unit_on_map",
            "resolve_theme": "resolve_theme",
            "agent_node": "agent_node",
        },
    )

    def select_unit_on_map_router(state: lg_State) -> str:
        logging.info("==================== select_unit_on_map_router CALLED ====================")
        # Check if there are no units selected - this means we should go to agent_node
        selected_workflow_units = state.get("selected_place_g_units", [])
        current_place_index = state.get("current_place_index", 0)
        extracted_place_names = state.get("extracted_place_names", [])

        logging.info(f"select_unit_on_map_router: selected_workflow_units={selected_workflow_units}")
        logging.info(f"select_unit_on_map_router: current_place_index={current_place_index}, extracted_place_names={extracted_place_names}")

        if not selected_workflow_units:
            logging.info("select_unit_on_map_router: returning agent_node (no units)")
            return "agent_node"

        # CRITICAL: Always prioritize place processing over theme processing
        # Check if there are still places to process first, regardless of intent queue
        num_places = len(extracted_place_names)
        if current_place_index is not None and current_place_index < num_places:
            logging.info(f"select_unit_on_map_router: returning resolve_place_and_unit (place {current_place_index} of {num_places} still needs processing)")
            return "resolve_place_and_unit"

        # Only after ALL places are processed, check for theme intents
        intent_queue = state.get("intent_queue", [])
        if intent_queue:
            # Check if the intent is theme-related
            theme_intents = [intent for intent in intent_queue if intent.get("intent") == "AddTheme"]
            if theme_intents:
                logging.info(f"select_unit_on_map_router: All places processed, now handling AddTheme intents {theme_intents}")
                return "resolve_theme"
            else:
                logging.info(f"select_unit_on_map_router: returning agent_node (non-theme intent queue: {intent_queue})")
                return "agent_node"

        # Use normal routing logic for final decision
        result = should_continue_to_themes(state)
        logging.info(f"select_unit_on_map_router: should_continue_to_themes returned {result}")
        return result

    # Add conditional edges for select_unit_on_map to handle retrigger cases
    workflow.add_conditional_edges(
        "select_unit_on_map",
        select_unit_on_map_router,
        {
            "resolve_place_and_unit": "resolve_place_and_unit",
            "resolve_theme": "resolve_theme",
            "agent_node": "agent_node",
        },
    )

    def addtheme_router(state: lg_State) -> str:
        selected_theme = state.get("selected_theme")
        extracted_theme = state.get("extracted_theme")
        has_theme = bool(selected_theme)
        has_units = bool(state.get("selected_place_g_units") or state.get("selected_polygons"))

        logging.info(f"addtheme_router: has_theme={has_theme}, has_units={has_units}")
        logging.info(f"addtheme_router: selected_theme='{selected_theme}', extracted_theme='{extracted_theme}'")

        # CRITICAL: If we have extracted_theme, we need to process the theme change regardless of selected_theme
        if extracted_theme:
            logging.info("addtheme_router: returning resolve_theme (need to process extracted theme)")
            return "resolve_theme"
        elif has_theme and has_units:
            logging.info("addtheme_router: returning find_cubes_node (have both theme and units)")
            return "find_cubes_node"
        elif has_theme and not has_units:
            # Check if we're in a situation where places are being processed
            current_place_index = state.get("current_place_index", 0) or 0
            total_places = len(state.get("extracted_place_names", []))

            if current_place_index < total_places:
                logging.info("addtheme_router: returning resolve_place_and_unit (have theme, need to continue place processing)")
                return "resolve_place_and_unit"
            else:
                logging.info("addtheme_router: returning agent_node (have theme, need units)")
                return "agent_node"
        else:
            logging.info("addtheme_router: returning resolve_theme (need theme)")
            return "resolve_theme"

    workflow.add_conditional_edges(
        "AddTheme_node",
        addtheme_router,
        {
            "find_cubes_node": "find_cubes_node",
            "agent_node": "agent_node",
            "resolve_theme": "resolve_theme",
            "resolve_place_and_unit": "resolve_place_and_unit",
        },
    )

    def _have_any_units(s):
        """True if the user has supplied a unit in either slot."""
        return bool(
            s.get("selected_place_g_units") or
            s.get("selected_polygons")      # added
        )

    def resolve_theme_router(state: lg_State) -> str:
        has_theme = bool(state.get("selected_theme"))
        has_units = _have_any_units(state)
        has_options = bool(state.get("options"))
        current_node = state.get("current_node")
        selection_idx = state.get("selection_idx")
        extracted_theme = state.get("extracted_theme")

        # Check if places still need processing
        current_place_index = state.get("current_place_index", 0) or 0
        extracted_place_names = state.get("extracted_place_names", [])
        num_places = len(extracted_place_names)

        logging.info(f"resolve_theme_router: has_theme={has_theme}, has_units={has_units}, has_options={has_options}, current_node={current_node}, selection_idx={selection_idx}, extracted_theme={extracted_theme}")
        logging.info(f"resolve_theme_router: current_place_index={current_place_index}, num_places={num_places}")

        # CRITICAL: If places still need processing, always go back to resolve_place_and_unit
        if current_place_index < num_places:
            logging.info(f"resolve_theme_router: returning resolve_place_and_unit (places still need processing: {current_place_index} of {num_places})")
            return "resolve_place_and_unit"

        # If we have both theme and units, go to cubes
        if has_theme and has_units:
            logging.info("resolve_theme_router: returning find_cubes_node (have theme and units)")
            return "find_cubes_node"
        # If we have a theme but no units, go to agent to handle next steps
        elif has_theme and not has_units:
            logging.info("resolve_theme_router: returning agent_node (have theme, need units)")
            return "agent_node"
        # If we're actively waiting for user selection and no selection was made yet, stay in resolve_theme
        elif has_options and current_node == "resolve_theme" and selection_idx is None and not has_theme:
            logging.info("resolve_theme_router: returning resolve_theme (waiting for theme selection)")
            return "resolve_theme"
        # If we have an extracted theme to process but no theme yet selected, stay in resolve_theme
        elif extracted_theme and not has_theme:
            logging.info("resolve_theme_router: returning resolve_theme (processing extracted theme)")
            return "resolve_theme"
        # Otherwise go to agent
        else:
            logging.info("resolve_theme_router: returning agent_node (default)")
            return "agent_node"

    workflow.add_conditional_edges(
        "resolve_theme",
        resolve_theme_router,
        {
            "find_cubes_node": "find_cubes_node",
            "agent_node": "agent_node",
            "resolve_theme": "resolve_theme",
            "resolve_place_and_unit": "resolve_place_and_unit",
        },
    )

    workflow.add_edge("find_cubes_node", "agent_node")

    # workflow.add_edge("ask_followup_node", "agent_node")


    # workflow.add_edge("agent_node", END)


    # --- Compile the workflow ---
    logger.info("Compiling workflow with Redis checkpointer...")
    try:
        # Set up asynchronous Redis connection for the checkpointer.
        # Ensure Redis server is running at this host/port/db.
        conn = Redis(host="localhost", port=6379, db=0)

        # Initialize the asynchronous Redis checkpointer. This persists the state.
        checkpointer = AsyncRedisSaver(conn=conn)

        # Compile the graph definition with the checkpointer.
        # This creates the runnable workflow instance.
        compiled_workflow = workflow.compile(checkpointer=checkpointer)
        logger.info("Workflow compilation successful.")
    except Exception as e:
        # Catch errors during compilation (e.g., Redis connection issues).
        logger.error("Error compiling workflow", exc_info=True)
        raise # Re-raise the exception to prevent app startup if compilation fails.

    # --- Optionally produce diagrams ---
    # These are useful for visualizing and debugging the workflow structure.
    logger.info("Generating ASCII diagram of the workflow:")
    try:
        # Print a text-based representation of the graph to the console/logs.
        logger.info("\n" + compiled_workflow.get_graph().draw_ascii())
    except Exception as e:
        logger.warning("Could not generate ASCII diagram", exc_info=True) # Non-critical error

    logger.info("Attempting to generate Mermaid diagram and save as PNG:")
    try:
        # Generate a Mermaid diagram (requires Mermaid CLI or API access depending on method).
        # `draw_mermaid_png` might require internet access if using MermaidDrawMethod.API.
        compiled_workflow_image = compiled_workflow.get_graph().draw_mermaid_png(
             draw_method=MermaidDrawMethod.API, # Or MermaidDrawMethod.PYPPETEER if playwright installed
        )
        # Save the generated image to a file.
        with open("compiled_workflow.png", "wb") as png:
            png.write(compiled_workflow_image)
        logger.info("Successfully saved workflow diagram to compiled_workflow.png")
    except Exception as e:
        # Log errors during diagram generation (e.g., Mermaid service unavailable).
        logger.warning("Could not generate or save Mermaid PNG diagram", exc_info=True) # Non-critical error

    logger.info("Workflow creation and compilation completed.")
    # Return the compiled workflow object.
    return compiled_workflow
