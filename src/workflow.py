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
from utils.constants import UNIT_TYPES, UNIT_THEMES

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
from config import load_config, get_db  # Functions to load app config and get DB connection
from tools import (  # Custom functions to interact with the database/data
    find_cubes_for_unit_theme,
    find_units_by_postcode,
    find_themes_for_unit,
    find_places_by_name,
    get_all_themes
)
# Import Redis checkpointer for persistent state saving
from utils.redis_checkpoint import RedisSaver, AsyncRedisSaver
from redis.asyncio import Redis  # Asynchronous Redis client
import asyncio  # For running asynchronous operations (like Redis interaction)
from state_nodes import (
    ShowState_node, ListThemesForSelection_node,
    ListAllThemes_node, Reset_node,
    AddPlace_node, RemovePlace_node,
    AddTheme_node, RemoveTheme_node,
    DescribeTheme_node,
    theme_hint_node, 
    ask_followup_node
    
)
from agent_routing import agent_node  # Main entry point for user interactions
from intent_handling import AssistantIntent  # Enum for routing intents
from state_schema import lg_State  # TypedDict for the workflow state

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
    model="llama3.3:latest",  # The specific Ollama model to use
    base_url="https://148.197.150.162/ollama_api/",  # URL of the Ollama API server
    client_kwargs={"verify": False}  # Disables SSL verification if needed (use cautiously)
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
        "\n".join([f"{k}: {v}" for k, v in UNIT_THEMES.items()])
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

    places: list[dict] = []

    for idx, place_name in enumerate(place_names):
        county = counties[idx] if idx < len(counties) else "0"
        try:
            df = pd.read_json(
                io.StringIO(
                    find_places_by_name({"place_name": place_name,
                                         "county": county})
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
            "g_unit":          None,
            "g_unit_type":     None,
        })

    state["places"]        = places
    state["place_cursor"]  = 0          # start the loop
    state["selection_idx"] = None       # clear any stale click

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


def select_unit_on_map(state: lg_State) -> lg_State:
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
    # Get the list of units selected so far by the workflow (place/unit selection nodes).
    selected_workflow_units = state.get("selected_place_g_units", [])
    # Get the list of units selected *by the user on the map* (from frontend state).
    selected_map_polygons_str = [str(p) for p in state.get("selected_polygons", [])] # Ensure string comparison

    # Get the index of the place that was just processed in the previous node.
    last_processed_index = state.get("current_place_index", 0) - 1

    # Check if there are any workflow-selected units and the index is valid.
    if selected_workflow_units and last_processed_index >= 0 and last_processed_index < len(selected_workflow_units):
        # Get the specific unit ID selected by the workflow for the last processed place.
        last_unit_id = selected_workflow_units[last_processed_index]
        logger.debug(f"Checking map selection status for unit {last_unit_id} (index {last_processed_index}). Map selections: {selected_map_polygons_str}")

        # Check if this specific unit is *not yet* in the list of map-selected polygons.
        # This prevents re-interrupting if the user already selected it or if it was added programmatically.
        if str(last_unit_id) not in selected_map_polygons_str:
            logger.info(f"Unit {last_unit_id} not found in map selections. Issuing interrupt to update map.")
            # Issue an interrupt to signal the frontend.
            interrupt(value={
                 # Message might be displayed or just used internally by frontend.
                # "message": f"Please confirm or select the area for '{state['extracted_place_names'][last_processed_index]}' on the map.",
                 # Pass the current state of selections for context.
                "selected_place_g_places": state.get("selected_place_g_places", []),
                "selected_place_g_units": state.get("selected_place_g_units", []),
                "selected_place_g_unit_types": state.get("selected_place_g_unit_types", []),
                 # Pass index and node name for potential resume logic.
                "current_place_index": state.get("current_place_index"), # Pass the *incremented* index
                "current_node": "select_unit_on_map"
            })
            # Execution stops here, waits for frontend map interaction and retrigger.
        else:
            logger.info(f"Unit {last_unit_id} already selected on map. Skipping map interrupt.")
    else:
        # Log if there are no units or the index is somehow invalid.
        logger.warning(f"Skipping map selection trigger: No units selected in workflow or index mismatch (Index: {last_processed_index}, Units: {selected_workflow_units})")

    # Whether interrupted or not, return the state. The graph proceeds based on edges from this node.
    return state


def find_cubes_node(state: lg_State) -> lg_State:
    """
    Retrieves the actual data cubes (statistical data) based on the finally selected
    theme (`selected_theme`) and all selected geographical units (`selected_place_g_units` + `selected_polygons`).
    - Calls the `find_cubes_for_unit_theme` tool for each unit and the chosen theme.
    - Filters results based on `min_year` and `max_year` if provided in the state.
    - Combines the data cubes from all units.
    - Issues an `interrupt` with the combined cube data (`cubes`) to signal the frontend (`chat.py`) to display visualizations (charts, tables).
    """
    logger.info("Retrieving data cubes for selected theme and units...")
    state["current_node"] = "find_cubes_node"
    logger.debug({"current_state": state})

    # Combine workflow-selected and map-selected units again.
    workflow_units = state.get("selected_place_g_units", [])
    map_selected_units_int = [int(p) for p in state.get("selected_polygons", []) if str(p).isdigit()]
    all_selected_unit_ids = list(set(workflow_units + map_selected_units_int))

    if not all_selected_unit_ids:
        logger.warning("No units selected to find cubes for.")
        state["messages"].append(AIMessage(content="No areas selected to fetch data for."))
        return state

    # Get the selected theme JSON from the state.
    selected_theme_json = state.get("selected_theme")
    if not selected_theme_json:
        logger.warning("No theme selected to find cubes for.")
        state["messages"].append(AIMessage(content="Please select a theme first."))
        return state

    # Check if cubes have already been fetched and stored (e.g., if resuming after chart interaction).
    # This check depends on whether the frontend clears `selected_cubes` state or if we want re-fetching.
    if state.get('selected_cubes') and state.get('current_node') == 'find_cubes_node':
         logger.info("Cube data already present in selected_cubes, potentially from previous run. Skipping refetch.")
         return state

    try:
        # Parse the selected theme JSON to get the theme ID (e.g., 'T_POP').
        selected_theme_df = pd.read_json(io.StringIO(selected_theme_json), typ='series')
        if selected_theme_df.empty or 'ent_id' not in selected_theme_df.index:
            raise ValueError("Selected theme data is invalid or missing 'ent_id'.")
        theme_id = selected_theme_df["ent_id"]
        theme_label = selected_theme_df["labl"] # For messages
        logger.info(f"Fetching cubes for theme: '{theme_label}' ({theme_id}) across units: {all_selected_unit_ids}")
    except (ValueError, KeyError) as e:
        logger.error(f"Error parsing selected theme JSON: {e}", exc_info=True)
        state["messages"].append(AIMessage(content="Error reading the selected theme information."))
        return state

    # Get optional year filters from the state.
    min_year = state.get("min_year")
    max_year = state.get("max_year")

    all_cubes_dfs = [] # List to hold cube DataFrames for each unit
    # Iterate through each selected unit ID.
    for g_unit in all_selected_unit_ids:
        try:
            # Call the database tool to find cubes for this unit and theme.
            cubes_df = pd.read_json(io.StringIO(find_cubes_for_unit_theme(
                {"g_unit": str(g_unit), "theme_id": theme_id})), orient="records"
            )

            if cubes_df.empty:
                logger.debug(f"No cubes found for unit {g_unit}, theme {theme_id}.")
                continue # Skip to next unit if no data

            # --- Apply Year Filtering ---
            # Check if 'Start' and 'End' columns exist for filtering. Adjust column names if needed.
            if "Start" in cubes_df.columns and "End" in cubes_df.columns:
                # Convert year columns to numeric, coercing errors to NaN (which are then dropped implicitly by comparisons).
                cubes_df["Start"] = pd.to_numeric(cubes_df["Start"], errors="coerce")
                cubes_df["End"] = pd.to_numeric(cubes_df["End"], errors="coerce")
                # Apply min_year filter: Keep rows where the period *ends* at or after min_year.
                if min_year is not None:
                    cubes_df = cubes_df[cubes_df["End"] >= min_year]
                # Apply max_year filter: Keep rows where the period *starts* at or before max_year.
                if max_year is not None:
                    cubes_df = cubes_df[cubes_df["Start"] <= max_year]

            if cubes_df.empty:
                logger.debug(f"No cubes remained for unit {g_unit}, theme {theme_id} after year filtering ({min_year}-{max_year}).")
                continue # Skip if filtering removed all data

            # Add the 'g_unit' identifier back to the DataFrame (if not already present)
            # to know which unit these cube rows belong to after concatenation.
            cubes_df["g_unit"] = g_unit
            # Add the resulting DataFrame to the list.
            all_cubes_dfs.append(cubes_df)
            logger.debug(f"Found {len(cubes_df)} cubes for unit {g_unit} (theme: {theme_id}, years: {min_year}-{max_year}).")

        except Exception as e:
            logger.error(f"Error finding cubes for unit {g_unit}, theme {theme_id}", exc_info=True)
            state["messages"].append(AIMessage(content=f"Error fetching data for one of the areas (Unit ID: {g_unit})."))
            # Continue processing other units.

    # Combine all collected cube DataFrames.
    if all_cubes_dfs:
        big_cubes_df = pd.concat(all_cubes_dfs, ignore_index=True)
        logger.info(f"Successfully combined {len(big_cubes_df)} cube rows across {len(all_selected_unit_ids)} units.")

        # Convert the combined DataFrame to a list of dictionaries for the interrupt payload.
        cubes_data_list = big_cubes_df.to_json(orient="records")

        # --- Issue Interrupt for Visualization ---
        # Signal the frontend that data is ready for display.
        interrupt(value={
             # Message to potentially display to the user.
            "message": f"Here is the data for '{theme_label}' across the selected area(s):",
             # The core data payload for the frontend visualization components.
            "cubes": cubes_data_list,
            "current_node": "find_cubes_node", # Identify the interrupting node
            "last_intent_payload": {},
        })
        # Execution stops here, waits for frontend to handle the data (e.g., render charts)
        # and potentially resume the workflow later if needed (e.g., user asks follow-up question).
    else:
        # If no cubes were found for any unit after filtering.
        logger.warning(f"No cube data found for theme '{theme_label}' and selected units {all_selected_unit_ids} (Years: {min_year}-{max_year}).")
        state["messages"].append(
            AIMessage(content=f"Sorry, I couldn't find any data matching '{theme_label}' for the specified criteria and selected area(s).")
        )

    # Return state. If interrupt was called, graph pauses. If not, graph proceeds based on edges.
    return state

def resolve_place_and_unit(state: lg_State) -> lg_State:
    """
    Resolve exactly *one* place per call:
        • disambiguate place name   (may interrupt)
        • disambiguate unit type    (may interrupt)
        • write g_place / g_unit / g_unit_type
    It never mutates state *before* raising an interrupt.
    """
    i       = state.get("current_place_index", 0)
    places  = state.get("places", [])

    # done?
    if i >= len(places):
        return state

    place   = places[i].copy()         # work on a private copy

    # ───────────────────────────────────────── place disambiguation
    if place["g_place"] is None:        
        rows = place["candidate_rows"]
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
            })

        # from here on we **only** fall through if
        #   a) exactly one option  OR
        #   b) user has clicked → selection_idx set
        if multiple_options and sel_idx is None:
            return state          # safety (normally unreachable after interrupt)

        choice       = int(sel_idx) if sel_idx is not None else 0
        chosen_row  = rows[choice]

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
        sel_idx = None                   # consume the click

    # ───────────────────────────────────────── unit disambiguation
    
    if place["g_unit"] is None:
        urows = place["unit_rows"]
        multiple_options = len(urows) > 1
        sel_idx = state.get("selection_idx")      # refresh in case callback set it

        if multiple_options and sel_idx is None:
            options = [
                {
                    "option_type": "unit",
                    "label": UNIT_TYPES.get(r["g_unit_type"], {})
                                    .get("long_name", r["g_unit_type"]),
                    "color": UNIT_TYPES.get(r["g_unit_type"], {})
                                    .get("color", "#333"),
                    "value": j,
                }
                for j, r in enumerate(urows)
            ]

            interrupt(value={
                "message": f"Which geography for “{place['name']}”?",
                "options": options,               # 🌟 persisted in state
                "current_node": "resolve_place_and_unit",
                "place_cursor": i,
            })

        # from here on we **only** fall through if
        #   a) exactly one option  OR
        #   b) user has clicked → selection_idx set
        if multiple_options and sel_idx is None:
            return state          # safety (normally unreachable after interrupt)

        choice = int(sel_idx) if sel_idx is not None else 0   # safe now
        chosen_unit  = urows[choice]
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
    state["selection_idx"]        = None   # consumed
    state["options"]            = []     # consumed

    # keep legacy flat lists for downstream code (can be removed later)
    state.setdefault("selected_place_g_units", []).append(place["g_unit"])
    state.setdefault("selected_place_g_unit_types", []).append(place["g_unit_type"])
    state.setdefault("selected_place_g_places", []).append(place["g_place"])

    return state

def resolve_theme(state: lg_State) -> lg_State | Command:
    """Choose a theme and, if no units are known yet, prompt for a place."""
    # ------------------------------------------------------------------
    # Step 0 · How many units do we have?
    # ------------------------------------------------------------------
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
    # Step 2 · Has a theme been fixed already?
    # ------------------------------------------------------------------
    if not state.get("selected_theme"):
        theme_query = state.get("extracted_theme") or ""
        selection_idx = state.get("selection_idx")

        # 2 a · LLM auto-pick
        if theme_query:
            try:
                llm_code = choose_theme_chain.invoke({"question": theme_query}).theme_code
                chosen = next((t for t in available if t["ent_id"] == llm_code), None)
                if chosen:
                    state["selected_theme"] = json.dumps(chosen)
            except Exception as exc:
                logger.info(f"LLM pick failed: {exc}")

        # 2 b · Button click
        elif selection_idx is not None:
            state["selected_theme"] = json.dumps(available[int(selection_idx)])
            state["selection_idx"] = None

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
                }
            )
            return state   # execution pauses here

    # ------------------------------------------------------------------
    # Step 3 · If we now *have* a theme *but* still no units → ask for a place
    # ------------------------------------------------------------------
    if state.get("selected_theme") and not units:
        chosen = pd.read_json(state["selected_theme"], typ='series')
        interrupt(
            value={
                "message": (
                    f"Got it – I'll use the **{chosen.labl}** theme. "
                    "Which place or postcode should I fetch it for?"
                ),
                "options": [
                    {
                        "option_type": "intent",
                        "label": "Add a place",
                        "value": 0,       # handled by ask_followup_node
                        "color": "#333",
                    }
                ],
                "current_node": "ask_followup_node",
            }
        )
        return state                        # wait for user input
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
    units_ready  = len(state.get("selected_place_g_units", [])) >= num_places > 0
    have_theme   = bool(state.get("selected_theme"))

    if num_places > 0 and current_index >= num_places:
        return "resolve_theme"
    else:
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

    workflow.add_edge("multi_place_tool_call", "resolve_place_and_unit")
    workflow.add_edge("resolve_place_and_unit", "select_unit_on_map")
    workflow.add_node("resolve_theme", resolve_theme)

    # agent-edge - single mapping
    workflow.add_conditional_edges(
        "agent_node",
        lambda s: (s.get("last_intent_payload") or {}).get("intent") or "NO_INTENT",
        {
            **{i.value: f"{i.value}_node"
            for i in AssistantIntent
            if i is not AssistantIntent.CHAT},
            AssistantIntent.CHAT.value: END, 
            "NO_INTENT": "ask_followup_node",
        },
    )
        
    for n in [
        "ShowState_node", "ListThemesForSelection_node", "ListAllThemes_node",
        "DescribeTheme_node", "RemovePlace_node", "RemoveTheme_node"
    ]:
        workflow.add_edge(n, END)


    # --- Define Edges (Workflow Logic) ---

    # START already goes straight to agent_node now
    workflow.add_edge(START, "agent_node")

    workflow.add_conditional_edges(
        "select_unit_on_map",
        lambda s: (
            "agent_node" if s.get("intent_queue")              # queued intents?
            else should_continue_to_themes(s)                  # new logic
        ),
        {
            "agent_node"              : "agent_node",
            "resolve_place_and_unit" : "resolve_place_and_unit",
            "resolve_theme"          : "resolve_theme",
            "find_cubes_node"         : "find_cubes_node",     # ← NEW
        },
    )
    
    workflow.add_edge("AddTheme_node", "resolve_theme")

    workflow.add_conditional_edges(
        "resolve_theme",
        # go to cubes only if we have BOTH a theme and at least one unit
        lambda s: (
            "find_cubes_node"
            if s.get("selected_theme")
            and s.get("selected_place_g_units")
            else "agent_node"
        ),
        {
            "find_cubes_node": "find_cubes_node",
            "agent_node": "agent_node",
        },
    )

    workflow.add_edge("find_cubes_node", END)
    
    workflow.add_edge("ask_followup_node", "agent_node")


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