from langgraph.prebuilt import ToolNode
from langchain_core.runnables import RunnableLambda
from langchain_core.messages import ToolMessage
from pydantic import BaseModel, Field
from langchain.tools import BaseTool, StructuredTool, tool
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
import pandas as pd
from typing import List, Annotated
from config import load_config, get_db
import io

config = load_config()
db = get_db(config)


def handle_tool_error(state) -> dict:
    error = state.get("error")
    tool_calls = state["messages"][-1].tool_calls
    return {
        "messages": [
            ToolMessage(
                content=f"Error: {repr(error)}\n please fix your mistakes.",
                tool_call_id=tc["id"],
            )
            for tc in tool_calls
        ]
    }


def create_tool_node_with_fallback(tools: list) -> dict:
    return ToolNode(tools).with_fallbacks(
        [RunnableLambda(handle_tool_error)], exception_key="error"
    )


def _print_event(event: dict, _printed: set, max_length=1500):
    current_state = event.get("dialog_state")
    if current_state:
        print("Currently in: ", current_state[-1])
    message = event.get("messages")
    if message:
        if isinstance(message, list):
            message = message[-1]
        if message.id not in _printed:
            msg_repr = message.pretty_repr(html=True)
            if len(msg_repr) > max_length:
                msg_repr = msg_repr[:max_length] + " ... (truncated)"
            print(msg_repr)
            _printed.add(message.id)


# MAP TOOLS

# LangChain Tool to extract g_unit numbers
# @tool("highlight_polygons_on_map")
# def highlight_polygons_on_map(
#     g_unit_numbers: Annotated[str, "String containing g_unit numbers to highlight on the map"],
#     ) -> List[str]:
#     """
#     LangChain tool to highlight polygons on the map based on provided g_unit numbers.
#     """
#     print(f"Received g_unit numbers: {g_unit_numbers}")
#     # Here we expect only numbers like '10032910' and return them as a list
#     g_unit_list = [num for num in g_unit_numbers.split() if num.isdigit()]
#     return g_unit_list


def calculate_center_and_zoom(gdf_filtered):
    """
    Calculate the center and zoom level for a GeoDataFrame of selected polygons.

    Args:
        gdf_filtered (GeoDataFrame): The filtered GeoDataFrame containing selected polygons.

    Returns:
        dict: A dictionary containing `center` (latitude and longitude) and `zoom` level.
    """
    if gdf_filtered.empty:
        return {"center": None, "zoom": None}

    # Calculate the bounding box
    bounds = gdf_filtered.total_bounds  # [minx, miny, maxx, maxy]
    min_lon, min_lat, max_lon, max_lat = bounds

    # Calculate center
    center_lon = (min_lon + max_lon) / 2
    center_lat = (min_lat + max_lat) / 2

    # Adjust zoom level (this logic can be customized)
    zoom = 10  # Default zoom level
    if max_lon - min_lon > 0 and max_lat - min_lat > 0:
        zoom = 12 - max(max_lon - min_lon, max_lat - min_lat)

    return {"center": {"lon": center_lon, "lat": center_lat}, "zoom": zoom}


# DATABASE TOOLS
@tool
def find_cubes_for_unit_theme(
    g_unit: Annotated[str, "unit identifier for the cube"],
    theme_id: Annotated[str, "theme id for the cube"],
    ) -> pd.DataFrame:
    """
    Find cubes for a given unit and theme.
    """
    query = f"""select ncube.ent_ID as Cube_ID,
    ncube.labl as Cube,
    min(data.end_date_decimal) as Start,
    max(data.end_date_decimal) as End,
    count(data.g_data) as Count
    from hgis.g_data data,
        hgis.g_data_map map,
        hgis.g_data_ent ncube
    where ncube.ent_id = map.ncuberef
        and data.cellref = map.cellref
        and data.g_unit = '{g_unit}'
        and ncube.theme_ID = '{theme_id}'
    group by ncube.ent_ID, ncube.labl
    order by ncube.labl;
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    return df


@tool
def find_units_by_postcode(
    postcode: Annotated[str, "postcode to search for"],
    ) -> pd.DataFrame:
    """
    Find units by postcode.
    """
    query = f"""select    distinct u.g_unit, gp.g_place, auo_util.get_unit_name(u.g_unit) as g_name, u.g_unit_type, gp.county_name, max(public.st_area(f.g_foot_ertslcc))
    from    hgis.g_unit u, hgis.g_foot f, gbhdb.codepoint_jul2023_gb post, hgis.g_name as gn, hgis.g_place as gp
    where    f.g_unit=u.g_unit 
	and gn.g_unit=u.g_unit
	and gn.g_place=gp.g_place
	and public.st_contains(f.g_foot_ertslcc, post.g_point_etrs) 
	and post.postcode='{postcode}'
	and u.g_unit_type='MOD_DIST'
    group    by u.g_unit, gp.g_place, u.g_unit_type, gp.county_name
    order    by max(public.st_area(f.g_foot_ertslcc))
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    return df


@tool
def find_places_by_name(
    place_name: Annotated[str, "Name of the place to search for"],
    county: Annotated[str, "County code, default is '0'"] = "0",
    nation: Annotated[str, "Nation code, default is '0'"] = "0",
    domain: Annotated[str, "Domain code, default is '0'"] = "0",
    state: Annotated[str, "State code, default is '0'"] = "0"
) -> pd.DataFrame:
    """
    Find place names by provided parameters.
    """
    query = f"""
        SELECT p.g_place, p.g_name, p.g_county, p.g_nation, p.g_domain, p.g_state, p.county_name, p.nation_name, p.domain_name, p.state_name, n.g_unit, g.g_unit_type
        FROM g_place p, g_name n, g_unit g
        WHERE p.g_place = n.g_place
        AND (g.g_unit_type = 'MOD_DIST' OR g.g_unit_type='MOD_REG')
        AND n.g_name = UPPER('{place_name}')
        AND ({county}::integer = 0 OR p.g_county = {county}::integer)
        AND ({nation}::integer = 0 OR p.g_nation = {nation}::integer)
        AND ({domain}::integer = 0 OR p.g_domain = {domain}::integer)
        AND ({state}::integer = 0 OR p.g_state = {state}::integer)
        GROUP BY p.g_place, p.g_name, p.g_county, p.g_nation, p.g_domain, p.g_state, p.county_name, p.nation_name, p.domain_name, p.state_name, n.g_unit, g.g_unit_type
        LIMIT 41;
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    df = df.drop(columns=['g_unit_type'])
    column_to_aggregate = 'g_unit'
    group_columns = [col for col in df.columns if col != column_to_aggregate]

    df = df.groupby(group_columns, dropna=False, as_index=False).agg(
        {column_to_aggregate: list})
    return df

@tool
def find_themes_for_unit(
    unit: Annotated[str, "unit identifier for the cube"],
    ) -> pd.DataFrame:
    """
    Find themes for a given unit.
    """
    query = f"""select    distinct theme.labl, theme.ent_ID
    from hgis.g_data data, hgis.g_data_map map, hgis.g_data_ent ncube, hgis.g_data_ent theme
    where    theme.ent_ID=ncube.theme_ID and
        ncube.ent_id=map.ncuberef and
        data.cellref=map.cellref and
        data.g_unit='{unit}';
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    return df


@tool
def data_query(
    unitname: Annotated[str, "unit name to search for"],
    ) -> pd.DataFrame:
    """
    Query data for a given unit name.
    """
    query=f"""select    d.g_unit, auo_util.get_unit_name(d.g_unit), u.g_unit_type, d.end_date_decimal, d.cellref, d.g_authority, d.g_auth_note, d.g_data
    from    hgis.g_data d, hgis.g_unit u
    where    u.g_unit = d.g_unit and
        auo_util.get_unit_name(d.g_unit) = '{unitname}' limit 20;
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    return df