# app/tools.py
from langchain_core.runnables import RunnableLambda
from langchain_core.messages import ToolMessage
from pydantic import BaseModel, Field
from langchain.tools import BaseTool, StructuredTool, tool
from langchain_community.tools import QuerySQLDataBaseTool
import pandas as pd
from typing import List, Annotated, Dict
from config import load_config, get_db
import io
from utils.constants import UNIT_TYPES
import logging
logger = logging.getLogger(__name__)


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


# def create_tool_node_with_fallback(tools: list) -> dict:
#     return ToolNode(tools).with_fallbacks(
#         [RunnableLambda(handle_tool_error)], exception_key="error"
#     )


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


def get_date_ranges_by_type() -> pd.DataFrame:
    """Fetch min and max dates for each unit type."""
    query = """
    SELECT 
        g_unit_type,
        MIN(util.get_start_year(g_duration)) as min_year,
        MAX(util.get_end_year(g_duration)) as max_year
    FROM hgis.g_foot 
    WHERE use_for_stat_map='Y'
    GROUP BY g_unit_type
    ORDER BY g_unit_type;
    """
    res = db.run(query, fetch="cursor")
    res = list(res.mappings())
    return pd.DataFrame(res)

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
    ) -> str:
    """
    Find cubes for a given unit and theme.
    """
    query = f"""select 
        ncube.ent_ID as cube_id,
        ncube.labl as cube,
        min(data.end_date_decimal) as start,
        max(data.end_date_decimal) as end,
        count(data.g_data) as count
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
    logger.debug(f"[find_cubes_for_unit_theme] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    # Convert column names to match what we expect
    df.columns = ['Cube_ID', 'Cube', 'Start', 'End', 'Count']
    logger.debug(f"[find_cubes_for_unit_theme] Query returned: \n\n{df}")
    return df.to_json(orient='records')


@tool
def find_units_by_postcode(
    postcode: Annotated[str, "postcode to search for"],
    ) -> str:
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
    logger.debug(f"[find_units_by_postcode] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    logger.debug(f"[find_units_by_postcode] Query returned: \n\n{df}")
    return df.to_json(orient='records')


@tool
def find_places_by_name(
    place_name: Annotated[str, "Name of the place to search for"],
    county: Annotated[str, "County code, default is '0'"] = "0",
    nation: Annotated[str, "Nation code, default is '0'"] = "0",
    domain: Annotated[str, "Domain code, default is '0'"] = "0",
    state: Annotated[str, "State code, default is '0'"] = "0"
) -> str:
    """
    Find place names by provided parameters.
    """
    types_tuple = tuple(UNIT_TYPES.keys())
    query = f"""
        SELECT 
            p.g_place, 
            p.g_name, 
            p.g_county, 
            p.g_nation, 
            p.g_domain, 
            p.g_state, 
            p.county_name, 
            p.nation_name, 
            p.domain_name, 
            p.state_name, 
            array_agg(n.g_unit) AS g_unit,
            array_agg(COALESCE(g.g_unit_type, 'NONE')) AS g_unit_type
        FROM 
            g_place p
        JOIN 
            g_name n ON p.g_place = n.g_place
        LEFT JOIN 
            g_unit g ON n.g_unit = g.g_unit
        WHERE
        (g.g_unit_type IS NULL OR g.g_unit_type in {types_tuple})
        AND n.g_name = UPPER('{place_name}')
        AND ({county}::integer = 0 OR p.g_county = {county}::integer)
        AND ({nation}::integer = 0 OR p.g_nation = {nation}::integer)
        AND ({domain}::integer = 0 OR p.g_domain = {domain}::integer)
        AND ({state}::integer = 0 OR p.g_state = {state}::integer)
        AND g.g_point_source = 'Own centroid'
        GROUP BY 
            p.g_place, 
            p.g_name, 
            p.g_county, 
            p.g_nation, 
            p.g_domain, 
            p.g_state, 
            p.county_name, 
            p.nation_name, 
            p.domain_name, 
            p.state_name
        LIMIT 41;
    """
    logger.debug(f"[find_places_by_name] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    logger.debug(f"[find_places_by_name] Query returned: \n\n{df}")
    return df.to_json(orient='records')

@tool
def find_themes_for_unit(
    unit: Annotated[str, "unit identifier for the cube"],
    ) -> str:
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
    logger.debug(f"[find_themes_for_unit] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    logger.debug(f"[find_themes_for_unit] Query returned: \n\n{df}")
    return df.to_json(orient='records')


@tool
def data_query(
    unitname: Annotated[str, "unit name to search for"],
    ) -> str:
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
    return df.to_json(orient='records')

@tool
def get_cube_data(
    cube_id: Annotated[str, "ID of the cube to fetch data for"]
) -> str:
    """
    Fetch the actual data for a given cube ID.
    """
    query = f"""
    SELECT 
        d.end_date_decimal as year,
        d.g_unit,
        d.g_data as value
    FROM 
        hgis.g_data d,
        hgis.g_data_map m
    WHERE 
        d.cellref = m.cellref
        AND m.ncuberef = '{cube_id}'
    ORDER BY 
        d.end_date_decimal;
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    return df.to_json(orient='records')

@tool
def get_all_cube_data(
    g_unit: Annotated[str, "unit identifier for the cube"],
    cube_ids: List[str]
) -> str:
    """
    Fetch data for multiple cubes at once.
    """
    cube_ids_str = "','".join(cube_ids)
    query = f"""
    SELECT 
        d.end_date_decimal as year,
        u.g_name,
        d.g_unit,
        d.cellref,
        d.g_data as value,
        m.ncuberef as cube_id,
        ncube.labl as cube_name
    FROM 
        hgis.g_data d
        JOIN hgis.g_data_map m ON d.cellref = m.cellref
        JOIN hgis.g_data_ent ncube ON m.ncuberef = ncube.ent_id
        JOIN hgis.g_unit u ON d.g_unit = u.g_unit
    WHERE 
        d.g_unit = '{g_unit}'
        AND m.ncuberef IN ('{cube_ids_str}')
    ORDER BY 
        d.end_date_decimal;
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    
    # Pivot the data to create columns for each cube
    pivot_df = df.pivot(index=['g_name', 'year'], columns='cellref', values='value').reset_index()
    return pivot_df.to_json(orient='records')


# tool to choose theme from sentence

@tool
def get_unit_details(unit_ids: List[str]) -> str:
    """
    Fetch details (name, type) for a list of g_unit IDs.
    Useful for listing currently selected units.
    """
    if not unit_ids:
        return pd.DataFrame(columns=['g_unit', 'unit_name', 'unit_type', 'long_name']).to_json(orient='records')

    # Ensure IDs are strings and handle potential SQL injection (though less likely with list)
    safe_unit_ids = [str(uid).replace("'", "''") for uid in unit_ids]
    unit_ids_str = "','".join(safe_unit_ids)

    query = f"""
    SELECT
        u.g_unit,
        COALESCE(auo_util.get_unit_name(u.g_unit), 'Unknown Name') as unit_name,
        u.g_unit_type
    FROM
        hgis.g_unit u
    WHERE
        u.g_unit IN ('{unit_ids_str}');
    """
    logger.debug(f"[get_unit_details] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    try:
        res = dbtool.db._execute(query)
        df = pd.DataFrame(res, columns=['g_unit', 'unit_name', 'unit_type'])
        # Add the long name for display
        df['long_name'] = df['unit_type'].apply(lambda ut: UNIT_TYPES.get(ut, {}).get('long_name', ut))
        logger.debug(f"[get_unit_details] Query returned: \n\n{df}")
        return df.to_json(orient='records')
    except Exception as e:
        logger.error(f"[get_unit_details] Error executing query: {e}", exc_info=True)
        return pd.DataFrame(columns=['g_unit', 'unit_name', 'unit_type', 'long_name']).to_json(orient='records')


# Make sure get_all_themes is robust
@tool
def get_all_themes() -> str:
    """
    Get all available statistical themes from the database.
    Renamed to avoid conflict with the internal function name.
    """
    query = f"""
    SELECT ent_id, labl, text FROM hgis.g_data_ent where ent_type='T'
    ORDER BY labl
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    try:
        res = dbtool.db._execute(query)
        df = pd.DataFrame(res, columns=['ent_id', 'labl', 'text'])
        logger.debug(f"[get_all_themes_tool] Query returned: \n\n{df}")
        return df.to_json(orient='records')
    except Exception as e:
        logger.error(f"[get_all_themes_tool] Error executing query: {e}", exc_info=True)
        return pd.DataFrame(columns=['ent_id', 'labl', 'text']).to_json(orient='records')


# ────────────────────────────────────────────────────────────────────────────
# fetch the long description of a theme
# ────────────────────────────────────────────────────────────────────────────
@tool
def get_theme_text(theme_code: Annotated[str, "Theme code e.g. T_POP"]):
    """Return labl + text for a theme (hgis.g_data_ent)."""
    query = f"""
        SELECT ent_id, labl, text
        FROM   hgis.g_data_ent
        WHERE  ent_id = '{theme_code}'
          AND  ent_type = 'T'
        LIMIT 1;
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res    = dbtool.db._execute(query)
    df     = pd.DataFrame(res, columns=["ent_id", "labl", "text"])
    if df.empty:
        return pd.DataFrame(columns=["ent_id", "labl", "text"]).to_json(orient="records")
    return df.to_json(orient="records")
# ─────────────────────────────────────────────────────────────────────────────
