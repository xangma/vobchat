from typing import Annotated, Literal, Optional, List, Dict, Any
from pydantic import BaseModel, Field
from typing_extensions import TypedDict
from langgraph.graph import END, StateGraph, START
from langgraph.graph.message import AnyMessage, add_messages
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_openai import ChatOpenAI
from langchain_core.runnables import Runnable, RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import SystemMessage, AIMessage, ToolMessage, AnyMessage
import re
import pandas as pd
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from config import load_config, get_db
from tools import highlight_polygons_on_map, find_cubes_for_unit_theme, find_units_by_postcode, \
    find_themes_for_unit, find_places_by_name
from langchain_core.runnables.graph import MermaidDrawMethod

config = load_config()
db = get_db(config)

# Memory setup
memory = MemorySaver()



# Model
model = ChatOpenAI(model="gpt-4o-mini")

# Tools
toolkit = SQLDatabaseToolkit(db=db, llm=model)
tools = toolkit.get_tools()

list_tables_tool = next(
    tool for tool in tools if tool.name == "sql_db_list_tables")
get_schema_tool = next(tool for tool in tools if tool.name == "sql_db_schema")

# Regular expression for UK postcodes
postcode_regex = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})"


# Define the state for the agent
class lg_State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    selected_place: Optional[Any]
    selected_place_g_place: Optional[Any]
    selected_place_g_unit: Optional[Any]
    selected_place_gdf_id: Optional[Any]
    selected_place_themes: Optional[Any]
    multiple_places_returned: Optional[bool]
    is_postcode: bool
    extracted_postcode: Optional[str]  # Add field for the extracted postcode
    extracted_place_name: Optional[str]  # Add field for the extracted place name


# class Assistant:
#     def __init__(self, runnable: Runnable):
#         self.runnable = runnable

#     def __call__(self, state: State, config: RunnableConfig):
#         while True:
#             result = self.runnable.invoke(state)
#             # If the LLM happens to return an empty response, we will re-prompt it
#             # for an actual response.
#             if not result.tool_calls and (
#                 not result.content
#                 or isinstance(result.content, list)
#                 and not result.content[0].get("text")
#             ):
#                 messages = state["messages"] + \
#                     [("user", "Respond with a real output.")]
#                 state = {**state, "messages": messages}
#             else:
#                 break
#         return {"messages": result}


# Add a node for validating user input
def validate_user_input(state: lg_State) -> lg_State:
    print(f"Initial state: {state}")
    user_input = state['messages'][-1].content
    
    # If the state already has multiple places (i.e.) from a previous invocation, skip validation
    if state.get('multiple_places_returned'):
        return state
    
    postcode_match = re.search(postcode_regex, user_input)

    if postcode_match:
        # Extract the postcode from the user input
        extracted_postcode = postcode_match.group(0)
        state["is_postcode"] = True
        state["extracted_postcode"] = extracted_postcode
    else:
        state["is_postcode"] = False
        state["extracted_postcode"] = None

    print(f"Validated state: {state}")

    return state

def decide_next_node(state):
    if state.get('is_postcode'):
        return "postcode_tool_call"
    elif state.get('multiple_places'):
        return "place_tool_call"
    else:
        return "extract_place_name_node"

class Place(BaseModel):
    """Information about a place."""
    name: Optional[str] = Field(
        default=None, description="The name of the place")


class ExtractedData(BaseModel):
    """Extracted data about places."""
    places: List[Place]
    
# Define a custom prompt to provide instructions for extraction
place_extraction_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are an expert extraction algorithm. "
        "Only extract place names from the text. "
        "If you do not know the name of an attribute asked to extract, return null for the attribute's value."
    ),
    ("human", "{text}"),
])

place_extraction_runnable = place_extraction_prompt | model.with_structured_output(
    schema=ExtractedData)

# Define a new node to use the extraction chain


def extract_place_name_node(state: lg_State) -> lg_State:
    print(f"State at start of extract_place_name_node: {state}")
    text = state['messages'][-1].content
    
    extracted_data = place_extraction_runnable.invoke(
        {"text": text})

    if extracted_data and extracted_data.places:
        extracted_place = extracted_data.places[0].name
        state["extracted_place_name"] = extracted_place
    else:
        state["extracted_place_name"] = None

    print(f"State at end of extract_place_name_node: {state}")
    return state


def postcode_tool_call(state: lg_State) -> lg_State:
    extracted_postcode = state.get('extracted_postcode')
    if not extracted_postcode:
        state['messages'].append(AIMessage(content="No valid postcode was found."))
        return state

    # Perform tool call using the extracted postcode
    response = find_units_by_postcode(extracted_postcode)
    state['messages'].append(AIMessage(content=response.to_string()))
    state['selected_place'] = response.to_json(index=True)
    state['selected_place_g_unit'] = int(response['g_unit'].values[0])
    state['selected_place_g_place'] = int(response['g_place'].values[0])
    return state


def place_tool_call(state: lg_State) -> lg_State:
    print(f"State in place_tool_call: {state}")
    if state.get('multiple_places_returned'):
        text = state['messages'][-1].content
        returned_places = pd.read_json(state['selected_place'])
        returned_places = returned_places.iloc[int(text)].to_frame().T
        state['selected_place'] = returned_places.to_json(index=True)
        state['multiple_places_returned'] = False
    else:
        place_name = state['extracted_place_name']
        returned_places = find_places_by_name(place_name)
        state['selected_place'] = returned_places.to_json(index=True)
        print(
            f"Result from find_places_by_name: {returned_places}, num_results: {len(returned_places)}")
    num_results = len(returned_places)

    if num_results == 1:
        response_message = AIMessage(
            content=f"Place found: {returned_places.to_string()}")
        state['selected_place'] = returned_places.to_json(index=True)
        state['selected_place_g_place'] = int(
            returned_places['g_place'].values[0])
    elif num_results > 1:
        response_message = AIMessage(
            content=f"""Multiple places found:\n
            {returned_places[['g_name', 'county_name']]}
            \n\n
            Please specify which place you meant by number (e.g., '1').""")
        state['multiple_places_returned'] = True
    else:
        response_message = AIMessage(content="No places found with that name.")
    # update state with the message
    state['messages'].append(response_message)
    print(f"State at end of place_tool_call: {state}")
    return state


def handle_user_selection(state: lg_State) -> lg_State:
    if state.get('multiple_places_returned'):
        return state
    # Assume we get the user's response in `selected_place`
    selected_place = state.get('selected_place')
    selected_place_df = pd.read_json(selected_place)
    if not selected_place_df.empty:
        response_message = AIMessage(
            content=f"Place selected: {selected_place_df[['g_name', 'county_name']].to_string()}")
        # explode the selected place to get the g_unit
        selected_place_df = selected_place_df.explode('g_unit')
        # left join gdf['g_unit'] with selected_place_df['g_unit']
        selected_place_df = selected_place_df.dropna(subset='g_unit')
        selected_place_df = selected_place_df.astype({"g_unit": int})
        if len(selected_place_df) > 0:
            state['selected_place_g_unit'] = int(
                selected_place_df['g_unit'].values[0])
            gdf['gdf_index'] = gdf.index
            gdf_merged = gdf.merge(
                selected_place_df, on='g_unit', how='inner')
            if len(gdf_merged) > 0:
                state['selected_place_gdf_id'] = int(gdf_merged['gdf_index'].values[0])
    else:
        response_message = AIMessage(
            content="The selected place was not found.")
    # update state with the message
    state['messages'].append(response_message)
    return state


def get_place_themes_node(state: lg_State) -> lg_State:
    # Assume we get the user's response in `selected_place`
    if state.get('multiple_places_returned'):
        return state
    selected_place = state.get('selected_place')
    selected_place_df = pd.read_json(selected_place)
    selected_place_g_unit = state.get('selected_place_g_unit')
    if selected_place_g_unit:
        selected_place_themes = find_themes_for_unit(str(selected_place_g_unit))
        response_message = AIMessage(
            content=f"Themes available for selected Place {selected_place_df['g_name'].values[0]}: {selected_place_themes.to_string()}")
        state['selected_place_themes'] = selected_place_themes.to_json(index=True)
        
    else:
        response_message = AIMessage(
            content="The selected place was not found.")
    # update state with the message
    state['messages'].append(response_message)
    return state


def create_workflow(lg_State, gdf):
    # Define a new graph
    workflow = StateGraph(lg_State)




    # Add nodes to handle user input and provide responses

    # workflow.add_node("assistant", Assistant(model))
    workflow.add_node("validate_user_input", validate_user_input)
    workflow.add_node("extract_place_name_node", extract_place_name_node)
    # Adjust tool call based on the validation result
    workflow.add_node("postcode_tool_call", postcode_tool_call)
    workflow.add_node("place_tool_call", place_tool_call)
    workflow.add_node("handle_user_selection", handle_user_selection)
    workflow.add_node("get_place_themes_node", get_place_themes_node)

    # Decide what path to take based on the user input


    # workflow.add_edge(START, "assistant")
    workflow.add_edge(START, "validate_user_input")
    workflow.add_conditional_edges(
        "validate_user_input",
        decide_next_node,
        {
            "postcode_tool_call": "postcode_tool_call",
            "place_tool_call": "place_tool_call",
            "extract_place_name_node": "extract_place_name_node",
        }
    )
    workflow.add_edge("extract_place_name_node", "place_tool_call")
    workflow.add_edge("place_tool_call", "handle_user_selection")


    workflow.add_edge("postcode_tool_call", "handle_user_selection")
    workflow.add_edge("handle_user_selection", "get_place_themes_node")
    workflow.add_edge("get_place_themes_node", END)


    # Compile the workflow into a runnable
    compiled_workflow = workflow.compile(checkpointer=memory)

    compiled_workflow_image = compiled_workflow.get_graph().draw_mermaid_png(
    draw_method=MermaidDrawMethod.API,
    )

    # Save image
    with open("compiled_workflow.png", "wb") as png:
        png.write(compiled_workflow_image)
    return compiled_workflow
