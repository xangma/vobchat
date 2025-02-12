# app/workflow.py

# -------------------------------
# Import standard libraries and type hints
# -------------------------------
from typing import Annotated, Optional, List
import re
import pandas as pd
from typing_extensions import TypedDict
import logging
from utils.constants import UNIT_THEMES  # Constant definitions for themes

# -------------------------------
# Import Pydantic for data validation and models
# -------------------------------
from pydantic import BaseModel, Field

# -------------------------------
# Import LangChain and LangGraph modules
# -------------------------------
from langgraph.graph import END, StateGraph, START
from langgraph.graph.message import AnyMessage, add_messages
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_openai import ChatOpenAI
from langchain_ollama import ChatOllama
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import SystemMessage, AIMessage, ToolMessage, AnyMessage
from langchain_core.prompts import ChatPromptTemplate
from langgraph.errors import NodeInterrupt
from langchain_core.runnables.graph import MermaidDrawMethod

# -------------------------------
# Import local modules (configuration, DB setup, tools, etc.)
# -------------------------------
from config import load_config, get_db
from tools import (
    find_cubes_for_unit_theme,
    find_units_by_postcode,
    find_themes_for_unit,
    find_places_by_name
)
from mapinit import get_polygons_by_type
from utils.polygon_cache import polygon_cache

# -------------------------------
# Set up logging for debugging and informational messages
# -------------------------------
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------
# CONFIGURATION & SETUP
# ----------------------------------------------------------------------------------------

logger.info("Loading configuration and initializing components...")

# Load application configuration and database connection.
config = load_config()
db = get_db(config)


# Initialize memory saver for checkpointing the workflow state.
logger.debug("Initializing memory saver for checkpointing...")
memory = MemorySaver()

# Initialize the language model (ChatOllama in this case) with specific model and API URL.
logger.info("Initializing language model...")
model = ChatOllama(
    model="llama3.3:latest",
    base_url="https://148.197.150.162/ollama_api/",
    client_kwargs={"verify": False}
)

# Set up the SQL toolkit and extract useful tools for database operations.
logger.info("Setting up database toolkit and tools...")
toolkit = SQLDatabaseToolkit(db=db, llm=model)
tools = toolkit.get_tools()

# Extract specific tools by name from the toolkit
list_tables_tool = next(
    tool for tool in tools if tool.name == "sql_db_list_tables")
get_schema_tool = next(tool for tool in tools if tool.name == "sql_db_schema")

# -------------------------------
# Define a regex for UK postcodes
# -------------------------------
logger.debug("Initializing UK postcode regex pattern...")
postcode_regex = (
    r"([Gg][Ii][Rr] 0[Aa]{2})|"
    r"((([A-Za-z][0-9]{1,2})|"
    r"(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|"
    r"(([A-Za-z][0-9][A-Za-z])|"
    r"([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))"
    r"))\s?[0-9][A-Za-z]{2})"
)

# ----------------------------------------------------------------------------------------
# STATE DEFINITION
# ----------------------------------------------------------------------------------------

# Define the state type (using TypedDict) that will be passed between nodes in the workflow.
class lg_State(TypedDict):
    messages: Annotated[List[AnyMessage], add_messages]  # List of messages in the conversation
    selection_idx: Optional[int]                         # Index for any user selection (e.g., from buttons)
    selected_place: Optional[str]                        # JSON representation of the selected place from DB
    selected_place_g_places: List[Optional[int]]                # The g_place identifier for the selected place
    # The g_unit identifier for the selected place
    selected_place_g_units: List[Optional[int]]
    # The g_unit_type for the selected place
    selected_place_g_unit_types: List[Optional[str]]
    selected_place_themes: Optional[str]                 # JSON representation of themes available for the selected place
    selected_theme: Optional[str]                        # JSON representation of the selected theme
    is_postcode: bool                                    # Flag indicating if a valid postcode was extracted
    extracted_postcode: Optional[str]                    # Extracted postcode from the user's message
    extracted_theme: Optional[str]                       # Extracted theme from the user's message
    extracted_place_names: List[str]
    extracted_counties: List[str]
    current_place_index: int    # which place we are currently handling
    current_unit_index: int     # which place's polygon we are handling
    min_year: Optional[int]                              # Start year (if provided)
    max_year: Optional[int]                              # End year (if provided)
    selected_polygons: Optional[List[int]]             # List of polygons (if map selection is used)
    interrupt_state: bool                                # Flag to indicate that the node has interrupted the workflow
    multi_place_search_df: Optional[str]                 # JSON representation of the search results for multiple places

# ----------------------------------------------------------------------------------------
# CHAINS AND PYDANTIC MODELS FOR STRUCTURED OUTPUT
# ----------------------------------------------------------------------------------------

# Define the UserQuery model to structure the extracted information from the user's initial query.


class UserQuery(BaseModel):
    places: List[str] = Field(
        ..., description="A list of place names mentioned in the user query"
    )
    counties: Optional[List[str]] = Field(
        default=[
        ], description="A list of county codes corresponding to the places (if any)"
    )
    theme: Optional[str] = Field(
        default=None,
        description="The statistics theme requested by the user (e.g. population)"
    )
    min_year: Optional[int] = Field(
        default=None, description="The start year for the statistics"
    )
    max_year: Optional[int] = Field(
        default=None, description="The end year for the statistics"
    )


# The extraction prompt now instructs the model to extract lists.
initial_query_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are an expert extraction algorithm. Only extract the following variables from the text: "
        "places (as a list of place names), counties (as a list, if mentioned), theme, min_year, and max_year. "
        "Return null or an empty list for any variable that is not mentioned."
    ),
    ("user", "{text}")
])

initial_query_chain = initial_query_prompt | model.with_structured_output(
    schema=UserQuery
)

# -------------------------------
# Define a Pydantic model for theme decision output
# -------------------------------
class ThemeDecision(BaseModel):
    theme_code: str = Field(...,
                            description="The selected theme code from UNIT_THEMES, e.g. T_POP")

# Create a prompt template and chain for deciding the appropriate theme based on the user's question.
choose_theme_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are an assistant that determines the appropriate statistical theme based on a user's question."
    ),
    (
        "system",
        "The available themes are:\n" +
        "\n".join([f"{k}: {v}" for k, v in UNIT_THEMES.items()])
    ),
    (
        "user",
        "User Question: {question}\n"
        "Please output a JSON object with the field 'theme_code' set to one of the above available theme codes."
    )
])
# Chain the theme decision prompt with the model for structured output using the ThemeDecision schema.
choose_theme_chain = choose_theme_prompt | model.with_structured_output(
    schema=ThemeDecision)

# ----------------------------------------------------------------------------------------
# NODE DEFINITIONS
# ----------------------------------------------------------------------------------------

def extract_initial_query_node(state: lg_State) -> lg_State:
    """
    Extract the initial query from the user's message.  
    This node extracts the place, county, theme, and year range from the last message in the conversation.
    """
    logger.info("Extracting variables from the initial user query...")
    # Get the last user message.
    user_message = state["messages"][-1].content if state["messages"] else ""
    try:
        # Invoke the extraction chain to parse the user message.
        extraction = initial_query_chain.invoke({"text": user_message})
        logger.debug(f"Extraction result: {extraction}")
        # Update state with extracted variables.
        state["extracted_place_names"] = extraction.places
        state["extracted_counties"] = extraction.counties
        state["extracted_theme"] = extraction.theme
        state["min_year"] = extraction.min_year
        state["max_year"] = extraction.max_year
    except Exception as e:
        logger.error("Error during initial query extraction", exc_info=True)
    return state


def validate_user_input(state: lg_State) -> lg_State:
    """
    Validate the user input by checking if it contains a valid UK postcode using a regex pattern.
    Updates the state with the postcode flag and extracted postcode if found.
    """
    logger.info("Starting user input validation...")
    logger.debug({"current_state": state})

    user_input = state["messages"][-1].content
    logger.debug({"user_input": user_input})

    # Use regex to check for a valid UK postcode.
    postcode_match = re.search(postcode_regex, user_input)
    if postcode_match:
        logger.info(f"Valid postcode found: {postcode_match.group(0)}")
        state["is_postcode"] = True
        state["extracted_postcode"] = postcode_match.group(0)
    else:
        logger.info("No valid postcode found in input")
        state["is_postcode"] = False
        state["extracted_postcode"] = None

    logger.debug({"updated_state": state})
    return state


def postcode_tool_call(state: lg_State) -> lg_State:
    """
    If a postcode was successfully extracted, use the postcode tool to find corresponding units in the database.
    """
    logger.info("Starting postcode tool call...")
    logger.debug({"current_state": state})

    extracted_postcode = state.get("extracted_postcode")
    if not extracted_postcode:
        logger.warning("No valid postcode found in state")
        state["messages"].append(
            AIMessage(content="No valid postcode was found.")
        )
        return state

    try:
        logger.info(f"Searching for units with postcode: {extracted_postcode}")
        # Query the database using the provided postcode.
        response = find_units_by_postcode(extracted_postcode)
        logger.debug({"search_results": response})

        if not response.empty:
            logger.info("Units found for postcode")
            # Update the state with details from the database query.
            state["selected_place"] = response.to_json(index=True)
            state["selected_place_g_units"].append(int(response["g_unit"].values[0]))
            state["selected_place_g_places"].append(int(response["g_place"].values[0]))
        else:
            logger.warning(f"No units found for postcode: {extracted_postcode}")
            state["messages"].append(
                AIMessage(content="No units found for that postcode.")
            )

    except Exception as e:
        logger.error("Error in postcode tool call", exc_info=True)
        state["messages"].append(
            AIMessage(content=f"Error processing postcode: {str(e)}")
        )

    logger.debug({"updated_state": state})
    return state


def multi_place_tool_call(state: lg_State) -> lg_State:
    """
    Look up each place in extracted_place_names.
    Store the combined DataFrame (all results for all places) in JSON form.
    """
    logger.info("Starting multi-place tool call...")
    place_names = state.get("extracted_place_names", [])
    counties = state.get("extracted_counties", [])

    all_results = []
    # If the user typed the same number of counties as places, zip them,
    # otherwise just pass "0" or an empty string for counties
    for i, place_name in enumerate(place_names):
        county = counties[i] if i < len(counties) else "0"
        try:
            df = find_places_by_name(
                {"place_name": place_name, "county": county})
            # Label which “requested place index” in this row:
            df["requested_place_index"] = i
            all_results.append(df)
        except Exception as e:
            logger.error(
                f"Error searching for place {place_name}", exc_info=True)
            state["messages"].append(
                AIMessage(
                    content=f"Error searching for {place_name}: {str(e)}")
            )

    if all_results:
        big_df = pd.concat(all_results, ignore_index=True)
    else:
        # If absolutely no results
        big_df = pd.DataFrame()

    # Store the combined data
    state["multi_place_search_df"] = big_df.to_json(orient="records")
    # Reset place selection index
    state["current_place_index"] = 0
    return state


def multi_place_tool_handler(state: lg_State) -> lg_State:
    logger.info("Handling multi-place results...")

    # Recover the big DataFrame
    big_df = pd.read_json(state["multi_place_search_df"], orient="records")

    # current_place_index says which place we’re handling right now
    i = state.get("current_place_index", 0)
    place_names = state.get("extracted_place_names", [])

    if i >= len(place_names):
        # We've already handled all requested places
        logger.info("All places handled; proceeding.")
        return state

    # Filter the big DF for rows belonging to place i
    sub_df = big_df[big_df["requested_place_index"] == i].copy()
    if sub_df.empty:
        # If no matches for this place, just let the user know
        logger.warning(f"No DB matches for place index={i}")
        place_name = place_names[i] if i < len(place_names) else "?"
        state["messages"].append(
            AIMessage(content=f"No matches found for '{place_name}'.")
        )
        # Move on to the next place
        state["current_place_index"] = i + 1
        return state

    selection_idx = state.get("selection_idx", None)

    # If user has not made a selection yet, and sub_df has multiple rows, interrupt
    if selection_idx is None and len(sub_df) > 1:
        logger.info(f"Multiple matches for place {i}, raising NodeInterrupt.")
        button_options = []
        for row_i, row in sub_df.iterrows():
            label = f"{row['g_name']}, {row['county_name']} (ID={row['g_place']})"
            button_options.append({
                "option_type": "place",
                "label": label,
                "value": row_i  # store the actual row index
            })
        # Raise an interrupt so that the UI can show multiple selection buttons
        state['interrupt_state'] = True
        raise NodeInterrupt(value={
            "message": f"Multiple places found for '{place_names[i]}'. Please pick the correct one:",
            "options": button_options
        })

    # If there is exactly one match, or user has picked one
    chosen_row = None
    if len(sub_df) == 1 and selection_idx is None:
        # Single row automatically chosen
        chosen_row = sub_df.iloc[0]
    elif selection_idx is not None:
        # User has chosen from the multiple
        if selection_idx in sub_df.index:
            chosen_row = sub_df.loc[selection_idx]
        else:
            # Safety check
            raise ValueError(f"Invalid selection_idx={selection_idx}")

    if chosen_row is not None:
        # Clear the selection so we don't re-use it next time
        state["selection_idx"] = None

        # Record that chosen place's g_place in our lists
        if "selected_place_g_places" not in state:
            state["selected_place_g_places"] = []
        state["selected_place_g_places"].append(int(chosen_row["g_place"]))

        # Also store the entire row if you want (as JSON), e.g. for debugging:
        # if "selected_places" not in state:
        #     state["selected_places"] = []
        # state["selected_places"].append(chosen_row.to_json())

        # Add a message about what was chosen
        msg = f"Selected place: {chosen_row['g_name']} (ID={chosen_row['g_place']})"
        state["messages"].append(AIMessage(content=msg))

        # Done with place i -> move to next place
        state["current_place_index"] = i + 1

        # If we haven't handled all places, we can raise an interrupt
        # just to “re-trigger” the same node until done
        if state["current_place_index"] < len(place_names):
            logger.info(
                "Not all places handled yet; raising interrupt to pick the next place.")
            state['interrupt_state'] = True
            raise NodeInterrupt(value={
                "message": f"Now let’s pick the next place: {place_names[state['current_place_index']]}",
                "current_place_index": state["current_place_index"],
                "selected_place_g_places": state["selected_place_g_places"]
            })

    # If we get here with no chosen_row, it means sub_df==1 but we do not get there logically.
    # Or sub_df>1 but we skip the interrupt logic.
    # Usually we either raise or pick a row. So we should be safe.

    logger.info("All place selections completed or advanced.")
    state["current_place_index"] = 0
    return state


def handle_user_place_selection(state: lg_State) -> lg_State:
    logger.info("Handling polygon selection for each chosen place...")

    # Suppose we already have a list of g_places in state["selected_place_g_places"]
    selected_g_places = state.get("selected_place_g_places", [])
    if not selected_g_places:
        logger.warning("No places selected yet.")
        state["messages"].append(AIMessage(content="No places selected yet."))
        return state

    selection_idx = state.get("selection_idx", None)
    
    # Track which place’s polygons we’re currently picking
    current_unit_index = state.get("current_unit_index", 0)
    if current_unit_index >= len(selected_g_places):
        # We’ve already assigned polygons to all places
        logger.info("All polygons assigned.")
        return state

    # For whichever place we’re on, get the unit_types:
    big_df = pd.read_json(state["multi_place_search_df"], orient="records")
    current_g_place = selected_g_places[current_unit_index]
    selected_place_df = big_df[big_df["g_place"] == current_g_place]
    
    # explode the g_unit_types column
    exploded_df = selected_place_df.explode(["g_unit", "g_unit_type"])
    exploded_df = exploded_df.dropna(subset=["g_unit"]).reset_index(drop=True).copy()
    exploded_df["g_unit"] = exploded_df["g_unit"].astype(int)
    logger.debug({"exploded_unit_data": exploded_df})
    
    if "selected_place_g_units" not in state:
        state["selected_place_g_units"] = []
    if "selected_place_g_unit_types" not in state:
        state["selected_place_g_unit_types"] = []
    
    if current_unit_index < len(selected_g_places) and selection_idx is None:
        if len(exploded_df) == 0:
            logger.warning("No valid g_units found")
            state["messages"].append(
                AIMessage(content="No valid g_unit was found for the selected place."))
            return state
        elif len(exploded_df) == 1:
            logger.info("Single unit found - processing automatically")
            single_row = exploded_df.iloc[0]
            state["selected_place_g_units"].append(int(single_row["g_unit"]))
            state["selected_place_g_unit_types"].append(single_row["g_unit_type"] or "MOD_DIST")

            logger.info("Raising NodeInterrupt for map selection")
            state['interrupt_state'] = True

            raise NodeInterrupt(value={
                "message": "map_selection",
                "current_unit_index": state["current_unit_index"],
                "selected_place_g_units": state["selected_place_g_units"],
                "selected_place_g_unit_types": state["selected_place_g_unit_types"]
            })
        elif len(exploded_df) > 1:
            logger.info(
                "Multiple units found - preparing selection options")
            button_options = []
            for i, row in exploded_df.iterrows():
                label = f"{row['g_unit_type']} (ID={row['g_unit']})"
                button_options.append({
                    "option_type": "unit_selection",
                    "label": label,
                    "value": i
                })
            logger.debug({"button_options": button_options})
            state['interrupt_state'] = True

            raise NodeInterrupt(value={
                "message": "Multiple (g_unit, g_unit_type) options found. Please select one.",
                "options": button_options
            })

    if selection_idx is not None:
        selected_unit = exploded_df.iloc[selection_idx]
        state["selected_place_g_units"].append(str(selected_unit["g_unit"]))
        state["selected_place_g_unit_types"].append(selected_unit["g_unit_type"])

        response_message = AIMessage(
            content=f"Unit selected: {selected_unit['g_unit_type']} (ID={selected_unit['g_unit']})"
        )
        state["messages"].append(response_message)

        state["current_unit_index"] = current_unit_index + 1

        if state["current_unit_index"] < len(selected_g_places):
            state['interrupt_state'] = True
        raise NodeInterrupt(value={
            "message": f"map_selection",
            "current_unit_index": state["current_unit_index"],
            "selected_place_g_units": state["selected_place_g_units"],
            "selected_place_g_unit_types": state["selected_place_g_unit_types"]
        })
    
    logger.debug({"updated_state": state})
    state["current_unit_index"] = 0
    state["selection_idx"] = None
    return state


def get_place_themes_node(state: lg_State) -> lg_State:
    """
    Retrieve available statistical themes for the selected place unit by querying the database.
    """
    logger.info("Starting theme retrieval for selected place...")
    logger.debug({"current_state": state})

    selected_place_g_units = state.get("selected_place_g_units")
    themes_df_list = []
    for selected_place_g_unit in selected_place_g_units:
        try:
            logger.info(f"Retrieving themes for unit ID: {selected_place_g_unit}")
            # Call the database tool to get themes for the given unit.
            selected_place_themes = find_themes_for_unit(str(selected_place_g_unit))
            logger.debug({"retrieved_themes": selected_place_themes})
            themes_df_list.append(selected_place_themes)
        except Exception as e:
            logger.error("Error retrieving themes", exc_info=True)
            response_message = AIMessage(
                content=f"Error retrieving themes: {str(e)}"
            )
            state["messages"].append(response_message)
    common_themes = pd.concat(
        themes_df_list, ignore_index=True, axis=0).drop_duplicates()
    state["selected_place_themes"] = common_themes.to_json(index=True)

    logger.debug({"updated_state": state})
    return state


def decide_next_node(state: lg_State) -> str:
    """
    Decide which node to move to next in the workflow based on the current state.
    If a valid postcode was extracted, proceed with postcode processing;
    otherwise, if a place name was extracted, proceed with the place lookup.
    """
    logger.info("Deciding next node based on current state...")
    logger.debug({"current_state": state})
    if state.get("is_postcode"):
        logger.info("Decision: Processing as postcode")
        return "postcode_tool_call"
    elif state.get("extracted_place_name"):
        logger.info("Decision: Processing as place name")
        return "multi_place_tool_call"
    else:
        logger.info("Decision: Needs place name extraction")
        return "multi_place_tool_call"


def get_place_themes_handler(state: lg_State) -> lg_State:
    """
    Process the themes retrieved for the selected place.
    If a theme was mentioned in the initial extraction, use the LLM to decide on the best match.
    Otherwise, if multiple themes exist, interrupt the workflow to allow the user to select one.
    """
    logger.info("Starting theme handler...")
    logger.debug({"current_state": state})
    try:
        # Convert JSON themes to a DataFrame.
        selected_place_themes = pd.read_json(state["selected_place_themes"])
        logger.debug({"theme_data": selected_place_themes})
        if selected_place_themes.empty:
            logger.warning("No themes found for selected place")
            response_message = AIMessage(
                content="No themes found for the selected place."
            )
            state["messages"].append(response_message)
            return state

        # If a theme was extracted from the user query, use the LLM to decide on a theme.
        if state.get("extracted_theme"):
            user_question = state["messages"][0].content
            decision = choose_theme_chain.invoke({"question": user_question})
            theme_code = decision.theme_code.strip()
            logger.info(f"LLM decided theme code: {theme_code}")

            # Verify that the chosen theme is among those available.
            available_theme_codes = selected_place_themes["ent_id"].unique().tolist()
            if theme_code in available_theme_codes:
                selected_theme = selected_place_themes[selected_place_themes["ent_id"] == theme_code].iloc[0:1]
                state["selected_theme"] = selected_theme.to_json(index=True)
                logger.info(f"Automatically selected theme: {state['selected_theme']}")
                return state
            else:
                logger.info("LLM-selected theme is not available for the selected place; falling back to user selection.")

        # If no theme is set, check if the user made a selection via interrupt.
        selection_idx = state.get("selection_idx")
        if selection_idx is not None:
            logger.info(f"Processing theme selection with index: {selection_idx}")
            selected_theme = selected_place_themes.iloc[int(selection_idx)].to_frame().T
            state["selected_theme"] = selected_theme.to_json(index=True)
        # If still no theme is set, prepare theme selection options for the user.
        if not state.get("selected_theme"):
            logger.info("Preparing theme selection options for the user")
            button_options = [
                {"option_type": "theme", "label": row["labl"], "value": index}
                for index, row in selected_place_themes.iterrows()
            ]
            logger.debug({"button_options": button_options})
            state['interrupt_state'] = True
            raise NodeInterrupt(value={
                "message": "Select a theme for the selected place.",
                "options": button_options
            })
    except NodeInterrupt:
        state['interrupt_state'] = True
        logger.info("Raising NodeInterrupt for theme selection")
        raise
    except Exception as e:
        logger.error("Error in theme handler", exc_info=True)
        state["messages"].append(
            AIMessage(content=f"Error processing themes: {str(e)}")
        )
    logger.debug({"updated_state": state})
    return state


def find_cubes_node(state: lg_State) -> lg_State:
    logger.info("Starting cube retrieval for multiple polygons...")
    logger.debug({"current_state": state})

    g_units = state.get("selected_place_g_units", [])
    if not g_units:
        state["messages"].append(
            AIMessage(content="No polygons (g_units) selected."))
        return state

    selected_theme = state.get("selected_theme")
    if not selected_theme:
        state["messages"].append(AIMessage(content="No theme selected."))
        return state

    # Suppose we parse out the theme code from the selected_theme JSON:
    selected_theme_df = pd.read_json(selected_theme)
    theme_id = str(selected_theme_df["ent_id"].values[0])

    min_year = state.get("min_year")
    max_year = state.get("max_year")

    all_cubes = []
    for g_unit in g_units:
        # Do your existing DB lookup for (g_unit, theme_id)
        cubes_df = find_cubes_for_unit_theme(
            {"g_unit": str(g_unit), "theme_id": theme_id})
        # Filter by year range if needed
        if "Start" in cubes_df and "End" in cubes_df:
            cubes_df["Start"] = pd.to_numeric(
                cubes_df["Start"], errors="coerce")
            cubes_df["End"] = pd.to_numeric(cubes_df["End"], errors="coerce")
            if min_year is not None:
                cubes_df = cubes_df[cubes_df["End"] >= min_year]
            if max_year is not None:
                cubes_df = cubes_df[cubes_df["Start"] <= max_year]

        # Label which g_unit this row belongs to, so we can combine them
        cubes_df["g_unit"] = g_unit
        all_cubes.append(cubes_df)

    if all_cubes:
        big_cubes = pd.concat(all_cubes, ignore_index=True)
        # store them if you like
        state["selected_cubes"] = big_cubes.to_json(orient="records")
        # Possibly raise interrupt to open a chart:
        raise NodeInterrupt(value={
            "message": "Here are the combined cubes for all selected places.",
            "cubes": big_cubes.to_dict("records")
        })
    else:
        state["messages"].append(
            AIMessage(content="No cubes found for the selected polygons."))

    return state


# ----------------------------------------------------------------------------------------
# MAP SELECTION NODES
# ----------------------------------------------------------------------------------------

def check_map_selection_node(state: lg_State) -> lg_State:
    """
    Check if the user has made a map selection (i.e. selected polygons).
    If so, use that selection to set the unit (g_unit) directly, bypassing other inputs.
    """
    logger.info("Checking for map selection...")
    logger.debug({"current_state": state})

    selected_polygons = state.get("selected_polygons") or []
    if len(selected_polygons) > 0:
        logger.info({"map_selection": {"g_unit": selected_polygons[0]}})
        state["selected_place_g_units"] = selected_polygons[0]
        msg = f"Map selection detected: using g_unit={selected_polygons[0]}"
        state["messages"].append(AIMessage(content=msg))
    else:
        logger.info("No map selection found")

    logger.debug({"updated_state": state})
    return state


def decide_if_map_selected(state: lg_State) -> str:
    """
    Decide the next node in the workflow based on whether the user has made a map selection.
    If a map selection is present, proceed directly to theme retrieval; otherwise, validate the user input.
    """
    logger.info("Deciding flow based on map selection...")
    logger.debug({"current_state": state})

    selected_polygons = state.get("selected_polygons") or []
    if len(selected_polygons) > 0:
        logger.info("Decision: Skip to themes due to map selection")
        return "get_place_themes_node"
    else:
        logger.info("Decision: Proceed with normal input flow")
        return "validate_user_input"

# ----------------------------------------------------------------------------------------
# WORKFLOW DEFINITION
# ----------------------------------------------------------------------------------------

def create_workflow(lg_state):
    """
    Create and compile the workflow graph.  
    This function adds all the nodes and edges to the LangGraph StateGraph,
    compiles the workflow, and generates a Mermaid diagram for visualization.
    """
    logger.info("Creating workflow graph...")
    workflow = StateGraph(lg_state)

    # Log the nodes being added for debugging purposes.
    logger.debug({"action": "adding_nodes", "nodes": [
        "extract_initial_query_node",
        "check_map_selection_node",
        "validate_user_input",
        "postcode_tool_call",
        "place_tool_call",
        "place_tool_handler",
        "handle_user_place_selection",
        "get_place_themes_node",
        "get_place_themes_handler",
        "find_cubes_node"
    ]})

    # Add each node with its corresponding function.
    workflow.add_node("extract_initial_query_node", extract_initial_query_node)
    workflow.add_node("check_map_selection_node", check_map_selection_node)
    workflow.add_node("validate_user_input", validate_user_input)
    workflow.add_node("postcode_tool_call", postcode_tool_call)
    workflow.add_node("multi_place_tool_call", multi_place_tool_call)
    workflow.add_node("multi_place_tool_handler", multi_place_tool_handler)
    workflow.add_node("handle_user_place_selection",
                      handle_user_place_selection)
    workflow.add_node("get_place_themes_node", get_place_themes_node)
    workflow.add_node("get_place_themes_handler", get_place_themes_handler)
    workflow.add_node("find_cubes_node", find_cubes_node)

    # Define the edges between nodes. The workflow starts at the extraction node.
    workflow.add_edge(START, "extract_initial_query_node")
    # Then, after extraction, check if there's a map selection.
    workflow.add_edge("extract_initial_query_node", "check_map_selection_node")

    # Conditional edges based on map selection:
    workflow.add_conditional_edges(
        "check_map_selection_node",
        decide_if_map_selected,
        {
            "get_place_themes_node": "get_place_themes_node",
            "validate_user_input": "validate_user_input",
        }
    )
    # Conditional edges based on whether the user input contains a postcode or a place name.
    workflow.add_conditional_edges(
        "validate_user_input",
        decide_next_node,
        {
            "postcode_tool_call": "postcode_tool_call",
            "multi_place_tool_call": "multi_place_tool_call",
        }
    )

    # Add the remaining edges to connect the nodes sequentially.
    workflow.add_edge("multi_place_tool_call", "multi_place_tool_handler")
    workflow.add_edge("multi_place_tool_handler",
                      "handle_user_place_selection")
    workflow.add_edge("handle_user_place_selection", "get_place_themes_node")
    workflow.add_edge("get_place_themes_node", "get_place_themes_handler")
    workflow.add_edge("get_place_themes_handler", "find_cubes_node")
    workflow.add_edge("find_cubes_node", END)

    # Compile the workflow; this prepares it for execution.
    logger.info("Compiling workflow...")
    try:
        compiled_workflow = workflow.compile(checkpointer=memory)
        logger.info("Workflow compilation successful")
    except Exception as e:
        logger.error("Error compiling workflow", exc_info=True)
        raise

    # Optional: Generate a Mermaid diagram for visualizing the workflow graph.
    logger.info("Generating Mermaid diagram...")
    try:
        logger.info(compiled_workflow.get_graph().draw_ascii())
    except Exception as e:
        logger.error("Error generating ascii Mermaid diagram", exc_info=True)
    try:
        compiled_workflow_image = compiled_workflow.get_graph().draw_mermaid_png(
            draw_method=MermaidDrawMethod.API,
        )
        with open("compiled_workflow.png", "wb") as png:
            png.write(compiled_workflow_image)
        logger.info("Successfully saved workflow diagram to compiled_workflow.png")
    except Exception as e:
        logger.error("Error generating workflow diagram", exc_info=True)

    logger.info("Workflow creation completed successfully")
    return compiled_workflow