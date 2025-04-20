# app/workflow.py

# -------------------------------
# Import standard libraries and type hints
# -------------------------------
from typing import Annotated, Optional, List
import io
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

# ----------------------------------------------------------------------------------------
# NODE DEFINITIONS
# ----------------------------------------------------------------------------------------
# These functions represent the individual steps (nodes) in the workflow graph.
# Each function takes the current state (lg_State) as input and returns the updated state
# or a Command to control graph flow (e.g., Command(goto=...)).


# def agent_node(state: lg_State) -> lg_State:
#     """
#     A general-purpose node that invokes the LLM assistant for conversational responses or clarifications.
#     It's often used as a fallback or when the workflow needs the LLM's reasoning capabilities beyond structured tasks.
#     It checks the last message and, if it's from the user, invokes the LLM with context.
#     """
#     logger.info("Agent node: consulting assistant...")
#     if state["messages"]:
#         last_msg = state["messages"][-1]
#         # Only invoke the LLM if the last message was from the user (not the AI)
#         if isinstance(last_msg, HumanMessage):
#             query = last_msg.content.strip()
#             logger.info(f"Assistant query detected: {query}")
#             # Invoke the LLM with a system message providing context about the application and the current state.
#             response = model.invoke([
#                     SystemMessage(
#                         content=f"""
#                         You are an expert assistant integrated within the DDME prototype application—a dashboard that combines a chat interface, data retrieval, and dynamic visualization(maps and charts). The overall aim of the program is to engage users in conversation, extract relevant information from their queries(such as place names, postcodes, themes, and year ranges), retrieve corresponding data from a database, and then display this data interactively on maps and visualizations.

#                         The application is structured as a workflow graph with multiple nodes, each responsible for a specific part of the process. You have full access to the global state variable `state`, which tracks conversation history, user selections, and workflow progress.

#                         **Important Points for Your Assistance:**
#                         - **State Awareness:** The `state` variable is central to the application’s logic, tracking everything from user messages to selections and workflow progress.
#                         - **User Interruptions:** Several nodes can raise a `interrupt` to pause the automated flow and request user input. When advising on or debugging the system, consider how these interruptions are managed and how user responses update the `state`. 
#                         - **Data Flow:** Each node builds on the previous ones—starting from query extraction to data retrieval and visualization. Understanding this flow is key to providing accurate recommendations.

#                         Your role is to use this context and the current `state` to offer detailed, actionable insights that help maintain a smooth conversation flow, correctly process user queries, and enhance the overall user experience.

#                         Here is the current state of the application:
#                         {state}
#                         """),
#                 HumanMessage(content=query) # Pass the user's query
#             ])

#             # Append the LLM's response to the message history in the state.
#             state["messages"].append(AIMessage(content=response.content))
#     # Return the potentially updated state.
#     return state


# def extract_initial_query_node(state: lg_State):
#     """
#     Uses the `initial_query_chain` (LLM with structured output) to extract places, counties,
#     theme, and year range from the latest user message. Updates the state with these findings.
#     Returns a Command to branch execution based on whether places were found.
#     """
#     logger.info("Extracting variables from the initial user query...")
#     # Record the current node name in the state (useful for resuming after interrupts)
#     state["current_node"] = "extract_initial_query_node"
#     # Get the last user message content.
#     user_message = state["messages"][-1].content if state["messages"] else ""
#     try:
#         # Invoke the extraction chain.
#         extraction: UserQuery = initial_query_chain.invoke({"text": user_message})
#         logger.debug(f"Extraction result: {extraction}")
#         # Update state with extracted information from the Pydantic model.
#         state["extracted_place_names"] = extraction.places
#         state["extracted_counties"] = extraction.counties
#         state["extracted_theme"] = extraction.theme
#         state["min_year"] = extraction.min_year
#         state["max_year"] = extraction.max_year
#     except Exception as e:
#         # Log errors during extraction.
#         logger.error("Error during initial query extraction", exc_info=True)
#         # Optionally add an error message for the user?
#         # state["messages"].append(AIMessage(content="Sorry, I had trouble understanding that request."))

#     # If place names were successfully extracted, proceed to the multi-place tool call.
#     if state.get("extracted_place_names"):
#         # Use Command to explicitly route to the next node and pass updated state values.
#         return Command(
#             goto="multi_place_tool_call",
#             update={ # Pass extracted values explicitly in the update part of the command
#                 "extracted_place_names": state["extracted_place_names"],
#                 "extracted_counties": state["extracted_counties"],
#                 "extracted_theme": state["extracted_theme"],
#                 "min_year": state["min_year"],
#                 "max_year": state["max_year"]
#             }
#         )
#     else:
#         # If no places were extracted, go to the general agent node for clarification.
#         return Command(
#             goto="agent_node",
#             update={"messages": state["messages"]} # Pass updated messages
#         )


def validate_user_input(state: lg_State) -> lg_State:
    """
    Checks the latest user message for a valid UK postcode using the defined regex pattern.
    Updates the state with `is_postcode` (boolean) and `extracted_postcode` (string) if found.
    """
    logger.info("Starting user input validation...")
    # Recording current node is optional here if it doesn't interrupt, but can be useful for debugging.
    # state["current_node"] = "validate_user_input"
    logger.debug({"current_state": state})
    # Get the content of the last message.
    if state["messages"]:
        user_input = state["messages"][-1].content
    else:
        user_input = "" # Handle case with no messages
    logger.debug({"user_input": user_input})

    # Search for the postcode pattern in the input.
    postcode_match = re.search(postcode_regex, user_input)
    if postcode_match:
        # If found, log it and update the state.
        found_postcode = postcode_match.group(0)
        logger.info(f"Valid postcode found: {found_postcode}")
        state["is_postcode"] = True
        state["extracted_postcode"] = found_postcode
    else:
        # If not found, log it and update the state.
        logger.info("No valid postcode found in input")
        state["is_postcode"] = False
        state["extracted_postcode"] = None

    logger.debug({"updated_state": state})
    # Return the updated state.
    return state


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
    Iterates through the list of `extracted_place_names` (and corresponding `extracted_counties`)
    and calls the `find_places_by_name` tool for each. Combines all results into a single
    DataFrame stored as JSON in `multi_place_search_df`. Initializes `current_place_index`.
    """
    logger.info("Starting multi-place tool call...")
    state["current_node"] = "multi_place_tool_call"
    # Retrieve the lists of place names and counties from the state.
    place_names = state.get("extracted_place_names", [])
    counties = state.get("extracted_counties", []) # May be empty or shorter than place_names

    all_results_dfs = [] # List to hold DataFrames for each place search
    # Iterate through each extracted place name.
    for i, place_name in enumerate(place_names):
        # Get the corresponding county if available, otherwise use a default value ("0" or adjust as needed).
        county = counties[i] if i < len(counties) else "0" # Handle cases where county list is shorter
        try:
            # Call the database tool to find matches for the place name (and county).
            df = pd.read_json(io.StringIO(find_places_by_name(
                {"place_name": place_name, "county": county})), orient="records")
            # Add a column to the results indicating which original requested place this result corresponds to.
            df["requested_place_index"] = i
            # Add the resulting DataFrame to the list.
            all_results_dfs.append(df)
        except Exception as e:
            # Handle errors during the search for a specific place.
            logger.error(
                f"Error searching for place '{place_name}'", exc_info=True)
            # Inform the user about the error for that specific place.
            state["messages"].append(
                AIMessage(
                    content=f"Sorry, there was an error searching for '{place_name}': {str(e)}")
            )

    # Combine all the individual DataFrames into one large DataFrame.
    if all_results_dfs:
        big_df = pd.concat(all_results_dfs, ignore_index=True)
    else:
        # If no results were found for any place, create an empty DataFrame.
        logger.warning("No results found for any extracted place names.")
        big_df = pd.DataFrame() # Ensure big_df exists even if empty

    # Store the combined DataFrame as a JSON string in the state.
    # 'orient="records"' stores it as a list of dictionaries, which is often convenient.
    state["multi_place_search_df"] = big_df.to_json(orient="records")
    # Initialize the index for processing these places one by one.
    # state["current_place_index"] = 0
    logger.debug(f"Combined place search results stored for {len(place_names)} places.")
    return state


def process_place_selection(state: lg_State) -> lg_State:
    """
    Processes the search results for the *current* place being handled (indicated by `current_place_index`).
    Filters `multi_place_search_df` for the current place.
    - If multiple matches are found, it issues an `interrupt` to ask the user (via buttons in the frontend) to choose the correct one.
    - If only one match exists, or after the user selects via interrupt (using `selection_idx`), it stores the chosen `g_place` ID in `selected_place_g_places`.
    - If no matches are found, it informs the user and moves to the next place index.
    """
    logger.info("Processing place selection...")
    state["current_node"] = "process_place_selection"
    
    # Load the combined search results DataFrame from the JSON stored in the state.
    try:
        big_df = pd.read_json(io.StringIO(state["multi_place_search_df"]), orient="records")
    except ValueError: # Handle case where JSON might be invalid or empty
        logger.error("Could not read multi_place_search_df from state.")
        state["messages"].append(AIMessage(content="Error loading place search results."))
        # Decide how to proceed - maybe end or go to agent? Here, we let it continue but it might fail later.
        big_df = pd.DataFrame()

    place_names = state.get("extracted_place_names", [])
    current_index = state.get("current_place_index", 0)

    # Check if all extracted places have been processed.
    if current_index >= len(place_names):
        logger.info("All places processed in process_place_selection.")
        # Optionally reset index if looping is desired, or just return.
        # state["current_place_index"] = 0 # Reset if needed
        return state # Move on from this node

    current_place_name = place_names[current_index]
    logger.info(f"Processing place: '{current_place_name}' (Index: {current_index})")

    # Filter the big DataFrame to get results only for the current place index.
    sub_df = big_df[big_df["requested_place_index"] == current_index].reset_index(drop=True)

    # Handle case where no database matches were found for this specific place name.
    if sub_df.empty:
        logger.warning(f"No DB matches found for place '{current_place_name}'")
        # Inform the user.
        state["messages"].append(
            AIMessage(content=f"I couldn't find any specific matches for '{current_place_name}'.")
        )
        # Increment the index to process the next place in the list.
        state["current_place_index"] = current_index + 1
        # Return the state to potentially loop back or move forward depending on the graph edges.
        return state

    # Check if the user has made a selection via an interrupt.
    selection_idx = state.get("selection_idx") # This comes from the frontend callback after button click

    already_waiting = (
        selection_idx is None               # user hasn’t answered
        and state.get("options")            # options are already stored
        and state.get("current_node") == "process_place_selection"
    )

    if already_waiting:
        logger.debug("Already waiting for a place choice – skip duplicate prompt.")
        interrupt(value={
            "options": state["options"],            # keep the buttons alive
            "current_node": "process_place_selection",
            "current_place_index": state.get("current_place_index", 0),
        })

    # If there's more than one match AND the user hasn't made a selection yet, interrupt.
    if selection_idx is None and len(sub_df) > 1:
        logger.info(f"Multiple matches for '{current_place_name}'; prompting user selection.")
        # Prepare the options for the buttons to be displayed in the frontend.
        button_options = [
            {
                "option_type": "place",  # Identifier for the type of choice
                "label": f"{row['g_name']}, {row['county_name']}", # Text on the button
                "color": "#333", # Optional color styling hint
                "value": i # The value sent back when clicked (the DataFrame index)
            }
            for i, row in sub_df.iterrows() # Create a button for each matching row
        ]
        # Issue an interrupt. This pauses the graph execution.
        # The `value` dictionary is sent to the frontend (`chat.py` callback).
        interrupt(value={
            "message": f"Multiple places found matching '{current_place_name}'. Please choose the correct one:", # Prompt message
            "options": button_options, # List of button definitions
            # Pass relevant current state context needed if resuming *this* node after selection
            "current_place_index": current_index,
            "selected_place_g_places": state.get("selected_place_g_places", []),
            "selected_place_g_units": state.get("selected_place_g_units", []),
            "selected_place_g_unit_types": state.get("selected_place_g_unit_types", []),
            "current_node": "process_place_selection" # The node that issued the interrupt
        })
        # IMPORTANT: After calling interrupt(), the node execution stops here for this run.
        # The workflow waits until the user interacts (e.g., clicks a button) which
        # updates the state (`selection_idx`) and potentially resumes the workflow.
        # The graph might re-enter this node, but now `selection_idx` will have a value.

    # Determine the chosen row:
    # - If selection_idx is set (user clicked a button from the interrupt): use that index.
    # - Otherwise (either only 1 match initially, or resuming failed): default to the first row.
    chosen_row_df = None
    if selection_idx is not None: # Check if selection_idx has a value (i.e. user selected)
        try:
            # Select the row corresponding to the user's choice index.
            chosen_row_df = pd.DataFrame([sub_df.loc[int(selection_idx)]])
            state["options"] = []
            state["selection_idx"] = None
            logger.info(f"User selected index {selection_idx}.")
        except (KeyError, ValueError, IndexError):
            logger.error(f"Invalid selection_idx={selection_idx} received for place selection. Defaulting to first option.")
            # Fallback to the first row if the index is invalid
            chosen_row_df = pd.DataFrame([sub_df.iloc[0]])
    elif not sub_df.empty: # If no selection needed (only 1 result) or fallback needed
        chosen_row_df = pd.DataFrame([sub_df.iloc[0]])
        logger.info("Defaulting to the first/only place match.")

    # If a row was successfully chosen (either by selection or default)
    if chosen_row_df is not None and not chosen_row_df.empty:
        # Extract the g_place identifier from the chosen row.
        g_place = int(chosen_row_df["g_place"].values[0])
        # Add the g_place ID to the list in the state, ensuring it's initialized and avoiding duplicates.
        state.setdefault("selected_place_g_places", [])
        if g_place not in state["selected_place_g_places"]:
            state["selected_place_g_places"].append(g_place)

        # Inform the user about the place that was automatically or manually selected.
        place_name = chosen_row_df['g_name'].values[0]
        county_name = chosen_row_df['county_name'].values[0]
        msg = f"Okay, proceeding with data for: {place_name} in {county_name}."
        state["messages"].append(AIMessage(content=msg))
        logger.info(f"Stored g_place: {g_place} for '{place_name}'")
    else:
        logger.error("Could not determine chosen row for place selection.")
        state["messages"].append(AIMessage("Sorry, I couldn't finalize the place selection."))
        # Increment index to avoid getting stuck? Or route to agent?
        state["current_place_index"] = current_index + 1

    # NOTE: We do NOT increment current_place_index here if a place was successfully selected.
    # The flow moves to the *next node* (process_unit_selection) for the *same place index*.
    # The index is only incremented if no match was found, or after unit processing is complete.

    return state


def process_unit_selection(state: lg_State) -> lg_State:
    """
    Processes unit selection for the *currently selected place* (`g_place` identified in the previous node
    for the `current_place_index`).
    - Filters the `multi_place_search_df` again for the chosen `g_place`.
    - Extracts available geographical units (`g_unit`) and their types (`g_unit_type`) for that place.
    - If multiple unit types are available, issues an `interrupt` to ask the user to choose one (e.g., LSOA vs. Ward).
    - Stores the selected `g_unit` and `g_unit_type` in the state lists.
    - Increments `current_place_index` after successfully processing the unit for the current place, allowing the workflow to loop back for the next place if needed.
    """
    logger.info("Processing unit selection...")
    state["current_node"] = "process_unit_selection"

    # Load the search results again.
    try:
        big_df = pd.read_json(io.StringIO(state["multi_place_search_df"]), orient="records")
    except ValueError:
        logger.error("Could not read multi_place_search_df from state.")
        state["messages"].append(AIMessage(content="Error loading place search results for unit selection."))
        return state # Exit node if data is missing

    place_names = state.get("extracted_place_names", [])
    # Use the *same* current_index as process_place_selection used.
    current_index = state.get("current_place_index", 0)

    # Safety check: Ensure we have processed places up to the current index.
    selected_places = state.get("selected_place_g_places", [])
    if current_index >= len(selected_places):
        logger.error(f"Cannot process unit selection: No selected g_place found for index {current_index}.")
        # Maybe try incrementing index or routing differently?
        # state["current_place_index"] = current_index + 1 # Attempt to skip
        return state # Exit node

    # Get the g_place ID that was selected in the previous node for this index.
    chosen_g_place = selected_places[current_index]
    current_place_name = place_names[current_index] # For logging/messages
    logger.info(f"Processing units for place '{current_place_name}' (g_place: {chosen_g_place}, Index: {current_index})")

    # Filter the big DataFrame first by the request index, then by the chosen g_place ID.
    # This isolates the row(s) corresponding to the specific place instance selected previously.
    # Note: In the DB schema used, g_unit/g_unit_type might be lists within the row if a place maps to multiple units.
    chosen_rows = big_df[big_df["g_place"] == chosen_g_place].reset_index(drop=True)

    if chosen_rows.empty:
        logger.error(f"Consistency Error: No matching rows found in multi_place_search_df for selected g_place {chosen_g_place} at index {current_index}.")
        state["messages"].append(AIMessage(content="An internal error occurred while looking up geographical units."))
        state["current_place_index"] = current_index + 1 # Try to recover by skipping
        return state

    # Handle potential list-like columns for units.
    # `explode` transforms each item in the list into a separate row, pairing it with the other columns.
    # This normalizes the data so each row represents one specific unit for the selected place.
    try:
        # Ensure 'g_unit' and 'g_unit_type' exist before exploding. Add default empty lists if missing?
        if 'g_unit' not in chosen_rows.columns: chosen_rows['g_unit'] = [[] for _ in range(len(chosen_rows))]
        if 'g_unit_type' not in chosen_rows.columns: chosen_rows['g_unit_type'] = [[] for _ in range(len(chosen_rows))]

        unit_df = chosen_rows.explode(["g_unit", "g_unit_type"]).dropna(
            subset=["g_unit"] # Remove rows where g_unit became NaN after exploding (e.g., from empty lists)
        ).reset_index(drop=True)
        # Ensure g_unit is integer type after exploding.
        unit_df["g_unit"] = pd.to_numeric(unit_df["g_unit"], errors='coerce').astype('Int64') # Use nullable Int64
        unit_df = unit_df.dropna(subset=["g_unit"]) # Drop rows where conversion failed
    except KeyError as e:
        logger.error(f"Missing expected column for explode: {e}. Data: {chosen_rows.to_dict()}")
        state["messages"].append(AIMessage(content="Error processing unit data structure."))
        state["current_place_index"] = current_index + 1
        return state

    if unit_df.empty:
        logger.warning(f"No valid units found for g_place {chosen_g_place} after processing.")
        state["messages"].append(AIMessage(content=f"No specific geographical units found for {current_place_name}."))
        state["current_place_index"] = current_index + 1 # Move to next place
        return state

    # Check again for user selection via interrupt.
    selection_idx = state.get("selection_idx")

    already_waiting = (
        selection_idx is None               # user hasn’t answered
        and state.get("options")            # options are already stored
        and state.get("current_node") == "process_unit_selection"
    )

    if already_waiting:
        logger.debug("Already waiting for a unit choice – skip duplicate prompt.")
        interrupt(value={
            "options": state["options"],            # keep the buttons alive
        })

    # If there's more than one unit option AND the user hasn't selected yet, interrupt.
    if selection_idx is None and len(unit_df) > 1:
        logger.info(f"Multiple unit options found for '{current_place_name}'; prompting user selection.")
        # Prepare button options using unit types and potentially colors from constants.
        button_options = [
            {
                "option_type": "unit", # Type identifier
                 # Use descriptive names from UNIT_TYPES map, fallback to the code.
                "label": f"{UNIT_TYPES.get(row['g_unit_type'], {}).get('long_name', row['g_unit_type'])}",
                 # Use color from UNIT_TYPES map, fallback to default.
                "color": UNIT_TYPES.get(row['g_unit_type'], {}).get('color', "#333"),
                "value": i # Send back the DataFrame index of the unit choice
            }
            for i, row in unit_df.iterrows()
        ]
        # Issue the interrupt.
        interrupt(value={
            "message": f"Several types of geographical units are available for '{current_place_name}'. Please choose one:",
            "options": button_options,
             # Pass context again
            "selected_place_g_places": state.get("selected_place_g_places", []),
            "selected_place_g_units": state.get("selected_place_g_units", []),
            "selected_place_g_unit_types": state.get("selected_place_g_unit_types", []),
            "current_place_index": current_index,
            "current_node": "process_unit_selection" # Identify the interrupting node
        })
        # Execution stops here for this run, waits for user selection.

    # Select the unit row (either by user selection, or default to the first row if only one/fallback).
    selected_unit_row = None
    if selection_idx is not None: # User made a choice
        try:
            selected_unit_row = unit_df.iloc[int(selection_idx)]
            logger.info(f"User selected unit index {selection_idx}.")
            state["options"] = []
            state["selection_idx"] = None
        except (ValueError, IndexError):
            logger.error(f"Invalid selection_idx={selection_idx} received for unit selection. Defaulting to first option.")
            selected_unit_row = unit_df.iloc[0]
    elif not unit_df.empty: # Only one option or fallback
        selected_unit_row = unit_df.iloc[0]
        logger.info("Defaulting to the first/only unit option.")

    # If a unit row was successfully selected.
    if selected_unit_row is not None:
        # Extract the g_unit ID and the unit type.
        selected_unit_id = int(selected_unit_row["g_unit"])
        # Provide a default unit type if it's missing/null in the data. Adjust 'MOD_DIST' as needed.
        unit_type = selected_unit_row["g_unit_type"] or "MOD_DIST" # Fallback unit type

        # Add the selected unit ID and type to the state lists, avoiding duplicates.
        state.setdefault("selected_place_g_units", [])
        state.setdefault("selected_place_g_unit_types", [])
        if selected_unit_id not in state["selected_place_g_units"]:
            state["selected_place_g_units"].append(selected_unit_id)
            state["selected_place_g_unit_types"].append(unit_type)
            logger.info(f"Stored g_unit: {selected_unit_id} (Type: {unit_type}) for index {current_index}")

            # Inform the user about the selected unit type.
            unit_long_name = UNIT_TYPES.get(unit_type, {}).get('long_name', unit_type) # Get descriptive name
            msg = f"Using {unit_long_name} data for '{current_place_name}'."
            state["messages"].append(AIMessage(content=msg))
        else:
             logger.warning(f"Unit {selected_unit_id} was already selected for index {current_index}?") # Should ideally not happen if logic is correct

    else:
        logger.error("Could not determine selected unit row.")
        state["messages"].append(AIMessage(content="Sorry, I couldn't finalize the geographical unit selection."))

    # --- Crucial Step for Looping ---
    # Increment the place index *after* successfully processing the unit for the current place.
    # This prepares the workflow to handle the *next* place name in the list
    # when/if the graph routes back to `process_place_selection`.
    state["current_place_index"] = current_index + 1
    logger.info(f"Incremented current_place_index to {state['current_place_index']}")

    return state


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


def get_place_themes_node(state: lg_State) -> lg_State:
    """
    Retrieves available statistical themes for *all* selected units.
    This includes units selected via the standard place/unit workflow (`selected_place_g_units`)
    *and* any units selected directly on the map (`selected_polygons`).
    It calls the `find_themes_for_unit` tool for each unique unit ID and combines
    the results, storing them as JSON in `selected_place_themes`.
    """
    logger.info("Retrieving available themes for all selected units...")
    state["current_node"] = "get_place_themes_node"
    logger.debug({"current_state": state})

    # Combine units selected through the standard workflow and those from map interaction.
    workflow_units = state.get("selected_place_g_units", [])
    map_selected_units_int = [int(p) for p in state.get("selected_polygons", []) if str(p).isdigit()] # Ensure integer IDs

    # Combine and find unique unit IDs.
    all_selected_unit_ids = list(set(workflow_units + map_selected_units_int))

    if not all_selected_unit_ids:
        logger.warning("No units selected (workflow or map). Cannot retrieve themes.")
        state["messages"].append(AIMessage(content="Please select a place or area on the map first."))
        state["selected_place_themes"] = None # Ensure it's cleared/set to None
        return state

    logger.info(f"Retrieving themes for unit IDs: {all_selected_unit_ids}")
    themes_df_list = [] # To store theme DataFrames for each unit
    # Iterate through each unique selected unit ID.
    for unit_id in all_selected_unit_ids:
        try:
            # Call the database tool to get themes available for this specific unit.
            themes_for_unit_df = pd.read_json(io.StringIO(find_themes_for_unit(str(unit_id))), orient="records")
            # Add the DataFrame to the list if it's not empty.
            if not themes_for_unit_df.empty:
                themes_df_list.append(themes_for_unit_df)
                logger.debug(f"Retrieved {len(themes_for_unit_df)} themes for unit {unit_id}")
            else:
                logger.debug(f"No themes found for unit {unit_id}")
        except Exception as e:
            # Handle errors during theme retrieval for a specific unit.
            logger.error(f"Error retrieving themes for unit ID {unit_id}", exc_info=True)
            state["messages"].append(
                AIMessage(content=f"Error retrieving themes for one of the selected areas: {str(e)}")
            )
            # Continue to try and get themes for other units.

    # Process the collected theme DataFrames.
    if themes_df_list:
        # Concatenate all theme DataFrames into one.
        combined_themes_df = pd.concat(themes_df_list, ignore_index=True)
        # Find the common themes available across *all* selected units.
        # This requires careful thought: Do we want intersection or union?
        # Current implementation implicitly takes the *union* and then removes duplicates.
        # To get the *intersection* (themes common to ALL units), more complex logic is needed:
        # e.g., group by theme ID and count occurrences, keeping only those matching len(all_selected_unit_ids).
        # Assuming UNION is desired for now:
        unique_themes_df = combined_themes_df.drop_duplicates(subset=['ent_id']) # Keep unique themes based on ID
        logger.info(f"Found {len(unique_themes_df)} unique themes across selected units.")
        # Store the unique themes DataFrame as JSON in the state.
        state["selected_place_themes"] = unique_themes_df.to_json(orient="records") # Store as list of dicts
    else:
        # If no themes were found for any selected unit.
        logger.warning("No themes found for any of the selected units.")
        state["messages"].append(
            AIMessage(content="No statistical themes seem to be available for the selected area(s).")
        )
        state["selected_place_themes"] = None # Ensure state reflects no themes found

    logger.debug({"updated_state": state})
    return state


def get_place_themes_handler(state: lg_State) -> lg_State:
    """
    Handles the selection of a specific theme after available themes have been retrieved.
    - Loads themes from `selected_place_themes`.
    - If a theme was extracted in the initial query (`extracted_theme`), it uses the `choose_theme_chain` (LLM) to pick the best matching theme code from the available ones.
    - If no initial theme was extracted or the LLM's choice isn't available:
        - If multiple themes are available, it issues an `interrupt` to ask the user to select one via buttons.
        - If only one theme is available, it selects it automatically.
    - Stores the finally selected theme as JSON in `selected_theme`.
    """
    logger.info("Handling theme selection…")
    state["current_node"] = "get_place_themes_handler"

    # keep any existing choice
    current_theme_json = state.get("selected_theme")
    if current_theme_json:
        try:
            cur_df   = pd.read_json(io.StringIO(current_theme_json), orient="records")
            cur_code = cur_df["ent_id"].iat[0]
        except Exception:
            cur_code = None
    else:
        cur_code = None

    # ------------------------------------------------------------------
    # load the freshly fetched theme list for **all** selected units
    # ------------------------------------------------------------------
    json_themes = state.get("selected_place_themes")
    if not json_themes:
        return state                               # nothing to do

    available_df = pd.read_json(io.StringIO(json_themes), orient="records")
    if available_df.empty:
        return state

    # if we already have a theme and it is still in the list → done
    if cur_code and cur_code in available_df["ent_id"].values:
        theme_lbl = cur_df["labl"].iat[0]
        logger.info(f"Theme ‘{theme_lbl}’ still valid – keeping it.")
        state["messages"].append(
            AIMessage(content=f"Keeping previously selected theme “{theme_lbl}”.")
        )
        return state 

    # Clear any previous theme selection before proceeding.
    state["selected_theme"] = None
    # Also clear interrupt flag possibly set by previous nodes if not handled.

    # Check if themes were successfully retrieved in the previous node.
    json_themes = state.get("selected_place_themes")
    if not json_themes:
        logger.warning("No themes available in state to handle.")
        # No themes found message already added in previous node.
        return state # Nothing to do here.

    # Load the available themes from JSON into a DataFrame.
    available_themes_df = pd.read_json(io.StringIO(json_themes), orient="records")
    logger.debug({"available_themes_df": available_themes_df})

    if available_themes_df.empty:
        logger.warning("Loaded theme data is empty.")
        # Message likely added previously.
        return state

    theme_selected = False # Flag to track if a theme has been selected

    # --- Attempt 1: Use LLM if an initial theme was extracted ---
    extracted_theme_query = state.get("extracted_theme")
    # Also check if the LLM hasn't already made this choice in a previous loop (if applicable)
    if extracted_theme_query and not state.get("selected_theme"):
        logger.info(f"Attempting to match extracted theme query: '{extracted_theme_query}' using LLM.")
        # Get the original user query that mentioned the theme. Use first message or concatenate?
        # Assuming first message is sufficient context here.
        user_question = state["messages"][0].content if state["messages"] else extracted_theme_query
        try:
            # Invoke the theme selection chain.
            decision = choose_theme_chain.invoke({"question": user_question})
            llm_theme_code = decision.theme_code.strip()
            logger.info(f"LLM suggested theme code: {llm_theme_code}")

            # Verify that the LLM's chosen theme code is actually available for the selected place(s).
            available_theme_codes = available_themes_df["ent_id"].unique().tolist()
            if llm_theme_code in available_theme_codes:
                # Filter the DataFrame to get the row for the selected theme.
                selected_theme_df = available_themes_df[available_themes_df["ent_id"] == llm_theme_code].iloc[0:1]
                # Store the selected theme DataFrame as JSON.
                state["selected_theme"] = selected_theme_df.to_json(orient="records")
                theme_label = selected_theme_df['labl'].values[0]
                logger.info(f"Theme automatically selected based on initial query: {theme_label} ({llm_theme_code})")
                state["messages"].append(AIMessage(content=f"Okay, looking for data on '{theme_label}'."))
                theme_selected = True
            else:
                logger.info(f"LLM-suggested theme '{llm_theme_code}' is not available in the retrieved themes for this area. Falling back.")
                # Add message to user? "I couldn't find '{llm_theme_code}' data, please choose from available options:"
                state["messages"].append(AIMessage(content=f"I couldn't find specific data matching '{extracted_theme_query}' for this area. Please choose from the available themes:"))


        except Exception as llm_exc:
            logger.error(f"Error during LLM theme decision: {llm_exc}", exc_info=True)
            state["messages"].append(AIMessage(content="Sorry, I had trouble choosing a theme automatically. Please select one:"))
            # Fall through to manual selection below.

    # --- Attempt 2: Use user selection if interrupt occurred ---
    selection_idx = state.get("selection_idx")
    
    already_waiting = (
        selection_idx is None               # user hasn’t answered
        and state.get("options")            # options are already stored
        and state.get("current_node") == "get_place_themes_handler"
    )

    if already_waiting:
        logger.debug("Already waiting for a theme choice – skip duplicate prompt.")
        interrupt(value={
            "options": state["options"],            # keep the buttons alive
            "current_node": "get_place_themes_handler",
            "current_place_index": state.get("current_place_index", 0),
        })
    
    if not theme_selected and selection_idx is not None:
        logger.info(f"Processing user theme selection with index: {selection_idx}")
        try:
            # Select the theme row based on the index provided by the user's button click.
            selected_theme_df = available_themes_df.iloc[[int(selection_idx)]] # Use double brackets to keep DataFrame structure
            state["selected_theme"] = selected_theme_df.to_json(orient="records")
            theme_label = selected_theme_df['labl'].values[0]
            theme_code = selected_theme_df['ent_id'].values[0]
            logger.info(f"Theme selected by user: {theme_label} ({theme_code})")
            state["messages"].append(AIMessage(content=f"Okay, proceeding with theme: '{theme_label}'."))
            theme_selected = True
            state["options"] = []
            state["selection_idx"] = None
        except (ValueError, IndexError):
            logger.error(f"Invalid selection_idx={selection_idx} received for theme selection.")
            state["messages"].append(AIMessage(content="Sorry, that selection wasn't valid. Please try again:"))
            # selection_idx should be cleared later, so next run might re-interrupt.


    # --- Attempt 3: Interrupt if multiple options and no selection yet ---
    if not theme_selected and len(available_themes_df) > 1:
        logger.info("Multiple themes available and none selected yet. Prompting user.")
        # Prepare button options for the user.
        button_options = [{
            "option_type": "theme", # Type identifier
            "label": row["labl"], # Theme label for the button text
            'color': '#333', # Optional styling
            "value": index # Send back the DataFrame index as the value
            }
            for index, row in available_themes_df.iterrows() # Create button for each available theme
        ]
        logger.debug({"theme_button_options": button_options})
        # Issue the interrupt.
        interrupt(value={
            "message": "Please select a statistical theme for the chosen area(s):",
            "options": button_options,
            "current_place_index": state.get("current_place_index"),
            "current_node": "get_place_themes_handler"
        })
        # Execution stops here, waits for user selection via button click.


    # --- Attempt 4: Auto-select if only one option ---
    elif not theme_selected and len(available_themes_df) == 1:
        logger.info("Only one theme available, selecting automatically.")
        selected_theme_df = available_themes_df.iloc[[0]] # Select the first (only) row
        state["selected_theme"] = selected_theme_df.to_json(orient="records")
        theme_label = selected_theme_df['labl'].values[0]
        theme_code = selected_theme_df['ent_id'].values[0]
        logger.info(f"Theme automatically selected (only option): {theme_label} ({theme_code})")
        state["messages"].append(AIMessage(content=f"Found data for theme: '{theme_label}'."))
        theme_selected = True

    logger.debug({"updated_state_after_theme_handling": state})
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
         # Ensure interrupt flag is set correctly if we are just passing through
         return state

    try:
        # Parse the selected theme JSON to get the theme ID (e.g., 'T_POP').
        selected_theme_df = pd.read_json(io.StringIO(selected_theme_json), orient="records")
        if selected_theme_df.empty or 'ent_id' not in selected_theme_df.columns:
            raise ValueError("Selected theme data is invalid or missing 'ent_id'.")
        theme_id = str(selected_theme_df["ent_id"].values[0])
        theme_label = selected_theme_df["labl"].values[0] # For messages
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
            "current_node": "find_cubes_node" # Identify the interrupting node
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

def should_continue_to_themes(state: lg_State) -> str:
    """
    Routing logic used *after* unit selection and the map interaction trigger (`select_unit_on_map`).
    Checks if all extracted places have had their units processed.
    - If YES (number of selected units matches number of extracted places) -> proceed to get themes (`get_place_themes_node`).
    - If NO (more places left to process) -> loop back to process the next place (`process_place_selection`).

    Returns:
        str: The name of the next node ("get_place_themes_node" or "process_place_selection").
    """
    logger.info("Routing: Checking if all places have units processed...")
    # Get the number of places initially extracted.
    num_extracted_places = len(state.get("extracted_place_names", []))
    # Get the number of units successfully selected so far (workflow only, map selections handled separately).
    num_selected_units = len(state.get("selected_place_g_units", []))
    # Get the current place index (which points to the *next* place to be processed).
    current_index = state.get("current_place_index", 0)

    # Decision: Have we processed a unit for every place name extracted?
    # Compare `num_selected_units` count to `num_extracted_places`.
    # Alternatively, check if `current_index` has reached the total number of places.
    if num_extracted_places > 0 and current_index >= num_extracted_places:
        logger.info("Router -> get_place_themes_node (All places processed, ready for themes)")
        return "get_place_themes_node" # All done, proceed to themes.
    else:
        logger.info(f"Router -> process_place_selection (More places to process. Index: {current_index}, Total: {num_extracted_places})")
        return "process_place_selection" # Loop back to handle the next place.


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
    # workflow.add_node("extract_initial_query_node", extract_initial_query_node)
    workflow.add_node("agent_node", agent_node) # General LLM agent
    # workflow.add_node("validate_user_input", validate_user_input) # Checks for postcode
    workflow.add_node("postcode_tool_call", postcode_tool_call) # Handles postcode search
    workflow.add_node("multi_place_tool_call", multi_place_tool_call) # Searches multiple places
    workflow.add_node("process_place_selection", process_place_selection) # Handles place disambiguation/selection
    workflow.add_node("process_unit_selection", process_unit_selection) # Handles unit disambiguation/selection
    workflow.add_node("select_unit_on_map", select_unit_on_map) # Triggers map interaction (interrupt)
    workflow.add_node("get_place_themes_node", get_place_themes_node) # Retrieves available themes
    workflow.add_node("get_place_themes_handler", get_place_themes_handler) # Handles theme selection (LLM/interrupt)
    workflow.add_node("find_cubes_node", find_cubes_node) # Retrieves final data cubes (interrupt)
    # workflow.add_node("interrupt_or_chat_decision", interrupt_or_chat_decision) # Example if needed

    workflow.add_node("ShowState_node", ShowState_node)
    workflow.add_node("ListThemesForSelection_node", ListThemesForSelection_node)
    workflow.add_node("ListAllThemes_node", ListAllThemes_node)
    workflow.add_node("Reset_node", Reset_node)
    workflow.add_node("AddPlace_node", AddPlace_node)
    workflow.add_node("RemovePlace_node", RemovePlace_node)
    workflow.add_node("AddTheme_node", AddTheme_node)
    workflow.add_node("RemoveTheme_node", RemoveTheme_node)

    # agent‑edge – single mapping
    workflow.add_conditional_edges(
        "agent_node",
        # router
        lambda s: (s.get("last_intent_payload") or {}).get("intent") or "NO_INTENT",
        # mapping
        {
            **{i.value: f"{i.value}_node"         # the existing intent routes
            for i in AssistantIntent if i is not AssistantIntent.CHAT},
            "NO_INTENT": END                      # ← fall through when there’s nothing to do
        }
    )


    for n in [
        "ShowState_node", "ListThemesForSelection_node", "ListAllThemes_node",
        "RemovePlace_node", "AddTheme_node", "RemoveTheme_node"
        ]:
        workflow.add_edge(n, "agent_node")

    # --- Define Edges (Workflow Logic) ---

    # START already goes straight to agent_node now
    workflow.add_edge(START, "agent_node")

    # AddPlace_node sometimes issues Command→multi_place_tool_call or →postcode_tool_call
    workflow.add_edge("multi_place_tool_call", "process_place_selection")
    workflow.add_edge("process_place_selection", "process_unit_selection")
    workflow.add_edge("process_unit_selection", "select_unit_on_map")

    workflow.add_conditional_edges(
        "select_unit_on_map",
        should_continue_to_themes,            # unchanged helper
        {
            "get_place_themes_node": "get_place_themes_node",
            "process_place_selection": "process_place_selection",
        },
    )
    
    

    workflow.add_edge("postcode_tool_call", "get_place_themes_node")
    workflow.add_edge("get_place_themes_node", "get_place_themes_handler")
    workflow.add_edge("get_place_themes_handler", "find_cubes_node")
    workflow.add_edge("find_cubes_node", "agent_node")

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