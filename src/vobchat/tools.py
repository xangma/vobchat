# app/tools.py
from langchain_core.messages import ToolMessage
from langchain.tools import tool
from langchain_community.tools import QuerySQLDataBaseTool
import pandas as pd
from typing import List, Annotated, Dict
import json
from vobchat.config import load_config, get_db
from vobchat.utils.constants import UNIT_TYPES
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
        logger.debug("Currently in: ", current_state[-1])
    message = event.get("messages")
    if message:
        if isinstance(message, list):
            message = message[-1]
        if message.id not in _printed:
            msg_repr = message.pretty_repr(html=True)
            if len(msg_repr) > max_length:
                msg_repr = msg_repr[:max_length] + " ... (truncated)"
            logger.debug(msg_repr)
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
        ncube.theme_ID as theme_ID,
        ncube.ent_ID as cube_ID,
        ncube.labl as cube,
        min(ncube.text) as cube_text,
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

    # Retry logic for database connection issues
    max_retries = 3
    retry_delay = 0.5

    for attempt in range(max_retries):
        try:
            # Create a fresh database tool for each attempt
            from vobchat.config import load_config, get_db

            config = load_config()
            fresh_db = get_db(config)
            dbtool = QuerySQLDataBaseTool(db=fresh_db)

            res = dbtool.db._execute(query)
            # If no rows, return an empty, well-formed JSON array
            if not res:
                return pd.DataFrame(
                    columns=[
                        "Theme_ID",
                        "Cube_ID",
                        "Cube",
                        "Cube_Text",
                        "Start",
                        "End",
                        "Count",
                    ]
                ).to_json(orient="records", force_ascii=False, default_handler=str)

            # Build DataFrame robustly regardless of driver return shape (dicts or tuples)
            df = pd.DataFrame(res)
            if df.empty:
                return pd.DataFrame(
                    columns=[
                        "Theme_ID",
                        "Cube_ID",
                        "Cube",
                        "Cube_Text",
                        "Start",
                        "End",
                        "Count",
                    ]
                ).to_json(orient="records", force_ascii=False, default_handler=str)

            # Normalize column names case-insensitively; if unnamed (0..6), assign explicitly
            expected = [
                "Theme_ID",
                "Cube_ID",
                "Cube",
                "Cube_Text",
                "Start",
                "End",
                "Count",
            ]
            try:
                # If columns are numeric range (no names), set expected names
                if list(df.columns) == list(range(len(expected))):
                    df.columns = expected
                else:
                    # Lower all names and map to expected
                    lower_map = {str(c).lower(): c for c in df.columns}
                    rename_map = {}
                    for exp in expected:
                        key = exp.lower()
                        if key in lower_map:
                            rename_map[lower_map[key]] = exp
                    if rename_map:
                        df = df.rename(columns=rename_map)
                    # Ensure all expected columns exist even if missing in result
                    for col in expected:
                        if col not in df.columns:
                            df[col] = None
                    # Reorder columns to expected order
                    df = df[expected]
            except Exception:
                # Last resort: coerce to expected columns without failing
                for col in expected:
                    if col not in df.columns:
                        df[col] = None
                df = df[expected]
            logger.debug(
                f"[find_cubes_for_unit_theme] Query returned: \n\n{df}"
            )
            # Handle NaN values properly for JSON serialization
            return df.to_json(
                orient="records", force_ascii=False, default_handler=str
            )

        except Exception as e:
            logger.warning(
                f"[find_cubes_for_unit_theme] Database error for unit {g_unit}, theme {theme_id} (attempt {attempt + 1}/{max_retries}): {e}"
            )

            if attempt < max_retries - 1:
                import time

                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                logger.error(
                    f"[find_cubes_for_unit_theme] All retry attempts failed for unit {g_unit}, theme {theme_id}: {e}"
                )
                # Return empty result instead of crashing
                return "[]"


@tool
def find_units_by_postcode(
    postcode: Annotated[str, "postcode to search for"],
) -> str:
    """
    Find units by postcode.
    """

    user_lang = "eng"
    query = """
    WITH unit_name AS (
        SELECT g_unit,
            g_name,
            ROW_NUMBER() OVER (
                PARTITION BY g_unit
                ORDER BY
                    CASE
                        WHEN g_language IS NOT NULL
                                AND g_language = '{user_lang}' THEN 0
                        WHEN g_language = 'eng' THEN 1
                        ELSE 2
                    END
            ) AS rn
        FROM   hgis.g_name
        WHERE  g_name_status = 'P'
    )
    SELECT DISTINCT
        u.g_unit,
        gp.g_place,
        un.g_name              AS g_name,
        u.g_unit_type,
        gp.county_name,
        MAX(public.st_area(f.g_foot_ertslcc)) AS max_area
    FROM   hgis.g_unit   u
    JOIN   hgis.g_foot   f   ON f.g_unit = u.g_unit
    JOIN   hgis.g_name   gn  ON gn.g_unit = u.g_unit
    JOIN   hgis.g_place  gp  ON gn.g_place = gp.g_place
    JOIN   unit_name     un  ON un.g_unit = u.g_unit AND un.rn = 1
    JOIN   hgis.codepoint post
        ON public.st_contains(f.g_foot_ertslcc, post.g_point_etrs)
    WHERE  post.postcode   = {postcode}
    AND  u.g_unit_type   = 'MOD_DIST'
    GROUP  BY u.g_unit, gp.g_place, un.g_name, u.g_unit_type, gp.county_name
    ORDER  BY MAX(public.st_area(f.g_foot_ertslcc));
    """
    logger.debug(f"[find_units_by_postcode] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    logger.debug(f"[find_units_by_postcode] Query returned: \n\n{df}")
    return df.to_json(orient="records", force_ascii=False, default_handler=str)


@tool
def find_places_by_name(
    place_name: Annotated[str, "Name of the place to search for"],
    county: Annotated[str, "County code, default is '0'"] = "0",
    unit_type: Annotated[str, "Unit type code, default is '0'"] = "0",
    nation: Annotated[str, "Nation code, default is '0'"] = "0",
    domain: Annotated[str, "Domain code, default is '0'"] = "0",
    state: Annotated[str, "State code, default is '0'"] = "0",
) -> str:
    """
    Find place names by provided parameters.
    """
    types_tuple = (
        tuple(UNIT_TYPES.keys()) if unit_type == "0" else (f"('{unit_type}')")
    )
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
            p.x_uk,
            p.y_uk,
            public.ST_Y(public.ST_Transform(p.g_point, 4326)) AS lat,
            public.ST_X(public.ST_Transform(p.g_point, 4326)) AS lon,
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
        AND p.g_point IS NOT NULL
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
            p.state_name,
            p.x_uk,
            p.y_uk,
            p.g_point
        LIMIT 41;
    """
    logger.debug(f"[find_places_by_name] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    logger.debug(f"[find_places_by_name] Query returned: \n\n{df}")
    return df.to_json(orient="records", force_ascii=False, default_handler=str)


@tool
def find_places_by_name_like(
    place_name: Annotated[str, "Name (or part) of the place to search for"],
    county: Annotated[str, "County code, default is '0'"] = "0",
    unit_type: Annotated[str, "Unit type code, default is '0'"] = "0",
    nation: Annotated[str, "Nation code, default is '0'"] = "0",
    domain: Annotated[str, "Domain code, default is '0'"] = "0",
    state: Annotated[str, "State code, default is '0'"] = "0",
) -> str:
    """
    Fuzzy place lookup using ILIKE on place names. Returns similar columns as find_places_by_name.
    """
    types_tuple = (
        tuple(UNIT_TYPES.keys()) if unit_type == "0" else (f"('{unit_type}')")
    )
    safe = (place_name or "").replace("'", "''")
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
            p.x_uk,
            p.y_uk,
            public.ST_Y(public.ST_Transform(p.g_point, 4326)) AS lat,
            public.ST_X(public.ST_Transform(p.g_point, 4326)) AS lon,
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
        AND n.g_name ILIKE UPPER('%{safe}%')
        AND ({county}::integer = 0 OR p.g_county = {county}::integer)
        AND ({nation}::integer = 0 OR p.g_nation = {nation}::integer)
        AND ({domain}::integer = 0 OR p.g_domain = {domain}::integer)
        AND ({state}::integer = 0 OR p.g_state = {state}::integer)
        AND g.g_point_source = 'Own centroid'
        AND p.g_point IS NOT NULL
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
            p.state_name,
            p.x_uk,
            p.y_uk,
            p.g_point
        ORDER BY char_length(p.g_name) ASC
        LIMIT 41;
    """
    logger.debug(f"[find_places_by_name_like] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    logger.debug(f"[find_places_by_name_like] Query returned: \n\n{df}")
    return df.to_json(orient="records", force_ascii=False, default_handler=str)


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

    # Retry logic for database connection issues
    max_retries = 3
    retry_delay = 0.5

    for attempt in range(max_retries):
        try:
            # Create a fresh database tool for each attempt
            from vobchat.config import load_config, get_db

            config = load_config()
            fresh_db = get_db(config)
            dbtool = QuerySQLDataBaseTool(db=fresh_db)

            res = dbtool.db._execute(query)
            df = pd.DataFrame(res)
            logger.debug(f"[find_themes_for_unit] Query returned: \n\n{df}")
            return df.to_json(
                orient="records", force_ascii=False, default_handler=str
            )

        except Exception as e:
            logger.warning(
                f"[find_themes_for_unit] Database error for unit {unit} (attempt {attempt + 1}/{max_retries}): {e}"
            )

            if attempt < max_retries - 1:
                import time

                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                logger.error(
                    f"[find_themes_for_unit] All retry attempts failed for unit {unit}: {e}"
                )
                # Return empty result instead of crashing
                return "[]"


@tool
def data_query(
    unitname: Annotated[str, "unit name to search for"],
) -> str:
    """
    Query data for a given unit name.
    """

    user_lang = "eng"
    query = """
    WITH unit_name AS (
        SELECT g_unit,
            g_name,
            ROW_NUMBER() OVER (
                PARTITION BY g_unit
                ORDER BY
                    CASE
                        WHEN g_language IS NOT NULL
                                AND g_language = '{user_lang}' THEN 0
                        WHEN g_language = 'eng' THEN 1
                        ELSE 2
                    END
            ) AS rn
        FROM hgis.g_name
        WHERE g_name_status = 'P'
    )
    SELECT  d.g_unit,
            un.g_name           AS unit_name,
            u.g_unit_type,
            d.end_date_decimal,
            d.cellref,
            d.g_authority,
            d.g_auth_note,
            d.g_data
    FROM    hgis.g_data d
    JOIN    hgis.g_unit u   ON u.g_unit = d.g_unit
    JOIN    unit_name  un   ON un.g_unit = d.g_unit AND un.rn = 1
    WHERE   un.g_name = %(unitname)s
    LIMIT 20;
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res)
    return df.to_json(orient="records", force_ascii=False, default_handler=str)


@tool
def get_cube_data(
    cube_id: Annotated[str, "ID of the cube to fetch data for"],
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
    # Handle NaN values properly for JSON serialization
    return df.to_json(orient="records", force_ascii=False, default_handler=str)


@tool
def get_all_cube_data(
    g_unit: Annotated[str, "unit identifier for the cube"], cube_ids: List[str]
) -> str:
    """
    Fetch data for multiple cubes at once.
    """
    if not cube_ids:
        logger.warning(
            f"[get_all_cube_data] No cube_ids provided for unit {g_unit}"
        )
        return "[]"

    cube_ids_str = "','".join(cube_ids)
    query = f"""
    SELECT
        d.end_date_decimal as year,
        u.g_name,
        d.g_unit,
        d.cellref,
        d.g_data as value,
        m.ncuberef as cube_id,
        ncube.labl as cube_name,
        ncube.text as cube_text
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

    try:
        logger.debug(
            f"[get_all_cube_data] Running query for unit {g_unit} with {len(cube_ids)} cubes"
        )
        dbtool = QuerySQLDataBaseTool(db=db)
        res = dbtool.db._execute(query)
        df = pd.DataFrame(res)

        if df.empty:
            logger.warning(
                f"[get_all_cube_data] No data found for unit {g_unit}"
            )
            return "[]"

        # Pivot the data to create columns for each cube
        # Keep g_unit in the index so downstream code can map unit_type
        pivot_df = df.pivot(
            index=["g_unit", "g_name", "year"], columns="cellref", values="value"
        ).reset_index()
        logger.debug(
            f"[get_all_cube_data] Returning {len(pivot_df)} rows for unit {g_unit}"
        )
        # Handle NaN values properly for JSON serialization
        return pivot_df.to_json(
            orient="records", force_ascii=False, default_handler=str
        )
    except Exception as e:
        logger.error(
            f"[get_all_cube_data] Database error for unit {g_unit}: {e}"
        )
        # Return empty result instead of crashing
    return "[]"


# tool to choose theme from sentence


@tool
def get_unit_details(unit_ids: List[str]) -> str:
    """
    Fetch details (name, type) for a list of g_unit IDs.
    Useful for listing currently selected units.
    """
    if not unit_ids:
        return pd.DataFrame(
            columns=["g_unit", "unit_name", "unit_type", "long_name"]
        ).to_json(orient="records", force_ascii=False, default_handler=str)

    # Ensure IDs are strings and handle potential SQL injection (though less likely with list)
    safe_unit_ids = [str(uid).replace("'", "''") for uid in unit_ids]
    unit_ids_str = "','".join(safe_unit_ids)
    user_lang = "eng"
    query = f"""
    WITH unit_name AS (
        SELECT  g_unit,
                g_name,
                ROW_NUMBER() OVER (
                    PARTITION BY g_unit
                    ORDER BY
                        CASE
                            WHEN g_language IS NOT NULL
                                AND g_language = '{user_lang}' THEN 0
                            WHEN g_language = 'eng' THEN 1
                            ELSE 2
                        END
                ) AS rn
        FROM   hgis.g_name
        WHERE  g_name_status = 'P'
    )
    SELECT  u.g_unit,
            COALESCE(un.g_name, 'Unknown Name') AS unit_name,
            u.g_unit_type
    FROM    hgis.g_unit u
    LEFT JOIN unit_name un
        ON un.g_unit = u.g_unit
        AND un.rn     = 1
    WHERE   u.g_unit IN ('{unit_ids_str}');
    """
    logger.debug(f"[get_unit_details] Running query:\n{query}")
    dbtool = QuerySQLDataBaseTool(db=db)
    try:
        res = dbtool.db._execute(query)
        df = pd.DataFrame(res, columns=["g_unit", "unit_name", "unit_type"])
        # Add the long name for display
        df["long_name"] = df["unit_type"].apply(
            lambda ut: UNIT_TYPES.get(ut, {}).get("long_name", ut)
        )
        logger.debug(f"[get_unit_details] Query returned: \n\n{df}")
        return df.to_json(
            orient="records", force_ascii=False, default_handler=str
        )
    except Exception as e:
        logger.error(
            f"[get_unit_details] Error executing query: {e}", exc_info=True
        )
        return pd.DataFrame(
            columns=["g_unit", "unit_name", "unit_type", "long_name"]
        ).to_json(orient="records", force_ascii=False, default_handler=str)


# Make sure get_all_themes is robust
@tool
def get_all_themes() -> str:
    """
    Get all available statistical themes from the database.
    Renamed to avoid conflict with the internal function name.
    """
    query = """
    SELECT ent_id, labl, text FROM hgis.g_data_ent where ent_type='T'
    ORDER BY labl
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    try:
        res = dbtool.db._execute(query)
        df = pd.DataFrame(res, columns=["ent_id", "labl", "text"])
        logger.debug(f"[get_all_themes_tool] Query returned: \n\n{df}")
        return df.to_json(
            orient="records", force_ascii=False, default_handler=str
        )
    except Exception as e:
        logger.error(
            f"[get_all_themes_tool] Error executing query: {e}", exc_info=True
        )
        return pd.DataFrame(columns=["ent_id", "labl", "text"]).to_json(
            orient="records", force_ascii=False, default_handler=str
        )


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
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res, columns=["ent_id", "labl", "text"])
    if df.empty:
        return pd.DataFrame(columns=["ent_id", "labl", "text"]).to_json(
            orient="records", force_ascii=False, default_handler=str
        )
    return df.to_json(orient="records", force_ascii=False, default_handler=str)


# ────────────────────────────────────────────────────────────────────────────
# get key findings for a place
# ────────────────────────────────────────────────────────────────────────────
@tool
def get_place_information(
    g_place: Annotated[int, "Place identifier (g_place) for the place"],
):
    """Return detailed information about a place from g_place table, matching the original Vision of Britain place page display."""
    query = f"""
        SELECT
            g_name,
            g_container as county,
            is_county,
            is_district,
            is_nation,
            is_state,
            is_domain,
            nation_name,
            state_name,
            domain_name,
            county_name,
            g_county,
            g_nation,
            g_state,
            g_domain,
            mod_dist,
            district_name,
            district_type,
            dg_text_auth,
            dg_text,
            has_multiple_names,
            see_also_place,
            x_uk as lon,
            y_uk as lat,
            notes
        FROM hgis.g_place
        WHERE g_place = {g_place}
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)

    if not res:
        return pd.DataFrame().to_json(
            orient="records", force_ascii=False, default_handler=str
        )

    # Convert to dict for easier handling
    place_data = dict(res[0])

    # If there's a see_also_place, get its name
    if place_data.get("see_also_place"):
        see_also_query = f"""
            SELECT g_name
            FROM hgis.g_place
            WHERE g_place = {place_data["see_also_place"]}
        """
        see_also_res = dbtool.db._execute(see_also_query)
        if see_also_res:
            place_data["see_also_place_name"] = see_also_res[0]["g_name"]

    df = pd.DataFrame([place_data])
    return df.to_json(orient="records", force_ascii=False, default_handler=str)


def get_place_key_findings(
    g_unit: Annotated[int, "Unit identifier for the place"],
):
    """Return key findings for a place from g_unit_key_findings table."""
    query = f"""
        SELECT
            g_url,
            g_label,
            g_text
        FROM
            hgis.g_unit_key_findings
        WHERE
            g_unit = {g_unit}
        ORDER BY g_seq
        LIMIT 8;
    """
    dbtool = QuerySQLDataBaseTool(db=db)
    res = dbtool.db._execute(query)
    df = pd.DataFrame(res, columns=["g_url", "g_label", "g_text"])
    if df.empty:
        return pd.DataFrame(columns=["g_url", "g_label", "g_text"]).to_json(
            orient="records", force_ascii=False, default_handler=str
        )
    return df.to_json(orient="records", force_ascii=False, default_handler=str)


# ─────────────────────────────────────────────────────────────────────────────


# ────────────────────────────────────────────────────────────────────────────
# Unit type information (definition page) – DB-backed to avoid hallucinations
# ────────────────────────────────────────────────────────────────────────────
@tool
def get_unit_type_info(
    unit_type: Annotated[
        str,
        "Unit type code (e.g., 'LG_DIST') or label (e.g., 'Local Government District')",
    ],
) -> str:
    """Return structured details about a unit type (label, id, level, ADL feature type, descriptions, counts, relations, statuses).

    Returns JSON with shape:
      {
        "identifier": str,
        "label": str,
        "level": int,
        "level_label": str,
        "adl_feature_type": str | null,
        "description": str | null,
        "full_description": str | null,
        "unit_count": int,
        "may_be_part_of": [{"unit_type": str, "label": str}],
        "may_have_parts":  [{"unit_type": str, "label": str}],
        "may_have_succeeded": [{"unit_type": str, "label": str}],
        "may_have_preceded":  [{"unit_type": str, "label": str}],
        "statuses": [{"code": str, "label": str}]
      }
    """
    raw = (unit_type or "").strip()
    if not raw:
        return json.dumps({})
    safe = raw.replace("'", "''")

    dbtool = QuerySQLDataBaseTool(db=db)

    base_query_by_code = f"""
        SELECT t.g_unit_type, t.g_type_label, t.g_type_level, ll.g_label, l.g_adl_ft,
               t.g_description, t.g_full_description, t.notes
        FROM   g_unit_type t
        JOIN   g_type_level l       ON l.g_type_level = t.g_type_level
        JOIN   g_type_level_label ll ON ll.g_type_level = l.g_type_level
        WHERE  t.g_unit_type = '{safe}'
        LIMIT 1;
    """
    base_query_by_label = f"""
        SELECT t.g_unit_type, t.g_type_label, t.g_type_level, ll.g_label, l.g_adl_ft,
               t.g_description, t.g_full_description, t.notes
        FROM   g_unit_type t
        JOIN   g_type_level l       ON l.g_type_level = t.g_type_level
        JOIN   g_type_level_label ll ON ll.g_type_level = l.g_type_level
        WHERE  LOWER(t.g_type_label) = LOWER('{safe}')
        LIMIT 1;
    """

    # Normalize known labels to codes using constants to avoid relying on exact DB label text
    candidate_code = None
    try:
        raw_upper = raw.upper()
        if raw_upper in UNIT_TYPES:
            candidate_code = raw_upper
        else:
            raw_lower = raw.lower()
            # Exact long_name match (case-insensitive)
            for code_key, meta in UNIT_TYPES.items():
                long_name = (meta.get("long_name") or "").strip()
                if long_name and long_name.lower() == raw_lower:
                    candidate_code = code_key
                    break
            # Simple plural-to-singular fallback (e.g., "Modern Districts" → "Modern District")
            if not candidate_code and raw_lower.endswith("s"):
                singular = raw_lower[:-1]
                for code_key, meta in UNIT_TYPES.items():
                    long_name = (meta.get("long_name") or "").strip()
                    if long_name and long_name.lower() == singular:
                        candidate_code = code_key
                        break
    except Exception:
        candidate_code = None

    try:
        base_df = pd.DataFrame()
        # Prefer lookup by canonical code if we can map it
        if candidate_code:
            safe_code = candidate_code.replace("'", "''")
            q = base_query_by_code.replace(safe, safe_code)
            res = dbtool.db._execute(q)
            base_df = pd.DataFrame(
                res,
                columns=[
                    "g_unit_type",
                    "g_type_label",
                    "g_type_level",
                    "level_label",
                    "g_adl_ft",
                    "g_description",
                    "g_full_description",
                    "notes",
                ],
            )
        # Fallback: try as provided (code), then as label
        if base_df.empty:
            res = dbtool.db._execute(base_query_by_code)
            base_df = pd.DataFrame(
                res,
                columns=[
                    "g_unit_type",
                    "g_type_label",
                    "g_type_level",
                    "level_label",
                    "g_adl_ft",
                    "g_description",
                    "g_full_description",
                    "notes",
                ],
            )
        if base_df.empty:
            res = dbtool.db._execute(base_query_by_label)
            base_df = pd.DataFrame(
                res,
                columns=[
                    "g_unit_type",
                    "g_type_label",
                    "g_type_level",
                    "level_label",
                    "g_adl_ft",
                    "g_description",
                    "g_full_description",
                    "notes",
                ],
            )
        if base_df.empty:
            return json.dumps({})
        row = base_df.iloc[0]
        code = str(row["g_unit_type"])  # canonical code
    except Exception as e:
        logger.error(
            f"[get_unit_type_info] Error fetching base info: {e}", exc_info=True
        )
        return json.dumps({})

    # Count units
    try:
        count_query = f"SELECT count(g_unit) as unit_count FROM g_unit WHERE g_unit_type = '{code}';"
        res = dbtool.db._execute(count_query)
        count_df = (
            pd.DataFrame(res, columns=["unit_count"])
            if res is not None
            else pd.DataFrame()
        )
        unit_count = (
            int(count_df.iloc[0]["unit_count"]) if not count_df.empty else 0
        )
    except Exception as e:
        logger.warning(
            f"[get_unit_type_info] Count query failed for {code}: {e}"
        )
        unit_count = 0

    def rel_query(
        sql_tmpl_no_filter: str, sql_tmpl_with_filter: str, cols: List[str]
    ) -> List[Dict[str, str]]:
        try:
            res = dbtool.db._execute(sql_tmpl_with_filter)
            df = pd.DataFrame(res, columns=cols)
            if df.empty:
                res = dbtool.db._execute(sql_tmpl_no_filter)
                df = pd.DataFrame(res, columns=cols)
        except Exception:
            try:
                res = dbtool.db._execute(sql_tmpl_no_filter)
                df = pd.DataFrame(res, columns=cols)
            except Exception:
                df = pd.DataFrame(columns=cols)
        out = []
        for _, r in df.iterrows():
            try:
                out.append(
                    {"unit_type": str(r[cols[0]]), "label": str(r[cols[1]])}
                )
            except Exception:
                continue
        return out

    above_no_filter = f"""
        SELECT r.g_rel_unit_type, t.g_type_label
        FROM   g_legal_rel r
        JOIN   g_unit_type t ON t.g_unit_type = r.g_rel_unit_type
        WHERE  r.g_rel_type = 'IsPartOf' AND r.g_unit_type = '{code}';
    """
    above_with_filter = f"""
        SELECT r.g_rel_unit_type, t.g_type_label
        FROM   g_legal_rel r
        JOIN   g_unit_type t ON t.g_unit_type = r.g_rel_unit_type
        WHERE  r.g_rel_type = 'IsPartOf' AND r.g_unit_type = '{code}'
          AND  t.g_jurisdiction = 'GBHGIS';
    """
    above = rel_query(
        above_no_filter, above_with_filter, ["g_rel_unit_type", "g_type_label"]
    )

    below_no_filter = f"""
        SELECT r.g_unit_type, t.g_type_label
        FROM   g_legal_rel r
        JOIN   g_unit_type t ON t.g_unit_type = r.g_unit_type
        WHERE  r.g_rel_type = 'IsPartOf' AND r.g_rel_unit_type = '{code}';
    """
    below_with_filter = f"""
        SELECT r.g_unit_type, t.g_type_label
        FROM   g_legal_rel r
        JOIN   g_unit_type t ON t.g_unit_type = r.g_unit_type
        WHERE  r.g_rel_type = 'IsPartOf' AND r.g_rel_unit_type = '{code}'
          AND  t.g_jurisdiction = 'GBHGIS';
    """
    below = rel_query(
        below_no_filter, below_with_filter, ["g_unit_type", "g_type_label"]
    )

    before_q = f"""
        SELECT r.g_unit_type, t.g_type_label
        FROM   g_legal_rel r
        JOIN   g_unit_type t ON t.g_unit_type = r.g_unit_type
        WHERE  r.g_rel_type = 'SucceededBy' AND r.g_rel_unit_type = '{code}';
    """
    before = rel_query(before_q, before_q, ["g_unit_type", "g_type_label"])

    after_q = f"""
        SELECT r.g_rel_unit_type, t.g_type_label
        FROM   g_legal_rel r
        JOIN   g_unit_type t ON t.g_unit_type = r.g_rel_unit_type
        WHERE  r.g_rel_type = 'SucceededBy' AND r.g_unit_type = '{code}';
    """
    after = rel_query(after_q, after_q, ["g_rel_unit_type", "g_type_label"])

    # Status values
    try:
        status_q = f"SELECT g_status, g_label FROM g_status_type WHERE g_unit_type = '{code}';"
        res = dbtool.db._execute(status_q)
        status_df = (
            pd.DataFrame(res, columns=["g_status", "g_label"])
            if res is not None
            else pd.DataFrame()
        )
        statuses = []
        for _, r in status_df.iterrows():
            statuses.append(
                {"code": str(r["g_status"]), "label": str(r["g_label"])}
            )
    except Exception as e:
        logger.warning(
            f"[get_unit_type_info] Status query failed for {code}: {e}"
        )
        statuses = []

    out = {
        "identifier": code,
        "label": str(row.get("g_type_label", "")),
        "level": (
            int(row.get("g_type_level"))
            if row.get("g_type_level") is not None
            else None
        ),
        "level_label": (
            str(row.get("level_label"))
            if row.get("level_label") is not None
            else None
        ),
        "adl_feature_type": (
            str(row.get("g_adl_ft"))
            if row.get("g_adl_ft") is not None
            else None
        ),
        "description": (
            str(row.get("g_description"))
            if row.get("g_description") is not None
            else None
        ),
        "full_description": (
            str(row.get("g_full_description"))
            if row.get("g_full_description") is not None
            else None
        ),
        "unit_count": unit_count,
        "may_be_part_of": above,
        "may_have_parts": below,
        "may_have_succeeded": before,
        "may_have_preceded": after,
        "statuses": statuses,
    }
    return json.dumps(out)


@tool
def find_data_entity_id(query: Annotated[str, "Data entity code (e.g., N_*) or label to look up"]) -> str:
    """
    Resolve a data entity by code or by (case-insensitive) label.

    Returns JSON like {"ent_id": "N_SOCIAL_GRADE_TOT_M", "labl": "Social Grade Total", "ent_type": "N"}
    or {} if not found.
    """
    try:
        q = (query or "").strip()
        if not q:
            return json.dumps({})

        dbtool = QuerySQLDataBaseTool(db=db)
        safe = q.replace("'", "''")

        # 1) Try by ent_id (case-insensitive exact)
        q_id_variants = [
            f"SELECT ent_id, labl, ent_type FROM hgis.g_data_ent WHERE UPPER(ent_id) = UPPER('{safe}') LIMIT 1;",
            f"SELECT ent_id, labl, ent_type FROM g_data_ent WHERE UPPER(ent_id) = UPPER('{safe}') LIMIT 1;",
        ]
        for sql in q_id_variants:
            try:
                res = dbtool.db._execute(sql)
                df = pd.DataFrame(res)
                if not df.empty:
                    row = df.iloc[0]
                    return json.dumps(
                        {
                            "ent_id": str(row.get("ent_id") or row.get(0)),
                            "labl": str(row.get("labl") or row.get(1) or ""),
                            "ent_type": str(row.get("ent_type") or row.get(2) or ""),
                        }
                    )
            except Exception:
                continue

        # 2) Try by label (case-insensitive exact)
        q_label_exact = [
            f"SELECT ent_id, labl, ent_type FROM hgis.g_data_ent WHERE LOWER(labl) = LOWER('{safe}') LIMIT 1;",
            f"SELECT ent_id, labl, ent_type FROM g_data_ent WHERE LOWER(labl) = LOWER('{safe}') LIMIT 1;",
        ]
        for sql in q_label_exact:
            try:
                res = dbtool.db._execute(sql)
                df = pd.DataFrame(res)
                if not df.empty:
                    row = df.iloc[0]
                    return json.dumps(
                        {
                            "ent_id": str(row.get("ent_id") or row.get(0)),
                            "labl": str(row.get("labl") or row.get(1) or ""),
                            "ent_type": str(row.get("ent_type") or row.get(2) or ""),
                        }
                    )
            except Exception:
                continue

        # 3) Try ILIKE (prefix/substring), prefer shorter labels and nCubes
        q_label_like = [
            f"""
            SELECT ent_id, labl, ent_type
            FROM hgis.g_data_ent
            WHERE labl ILIKE '%{safe}%'
            ORDER BY (CASE WHEN ent_type='N' THEN 0 ELSE 1 END), char_length(labl) ASC
            LIMIT 1;
            """,
            f"""
            SELECT ent_id, labl, ent_type
            FROM g_data_ent
            WHERE labl ILIKE '%{safe}%'
            ORDER BY (CASE WHEN ent_type='N' THEN 0 ELSE 1 END), char_length(labl) ASC
            LIMIT 1;
            """,
        ]
        for sql in q_label_like:
            try:
                res = dbtool.db._execute(sql)
                df = pd.DataFrame(res)
                if not df.empty:
                    row = df.iloc[0]
                    return json.dumps(
                        {
                            "ent_id": str(row.get("ent_id") or row.get(0)),
                            "labl": str(row.get("labl") or row.get(1) or ""),
                            "ent_type": str(row.get("ent_type") or row.get(2) or ""),
                        }
                    )
            except Exception:
                continue

        return json.dumps({})
    except Exception as e:
        logger.error(f"[find_data_entity_id] Error: {e}", exc_info=True)
        return json.dumps({})


@tool
def get_data_entity_info(entity_id: Annotated[str, "ID of the data entity (e.g., N_..., T_..., U_..., V_...)"]) -> str:
    """
    Fetch core information for a data entity (from g_data_ent and g_data_ent_type),
    including higher/lower related entities.

    Returns JSON with keys: entity, higher_entities, lower_entities.
    """
    try:
        dbtool = QuerySQLDataBaseTool(db=db)

        def exec_df(sql_list: list[str]) -> pd.DataFrame:
            for q in sql_list:
                try:
                    res = dbtool.db._execute(q)
                    df = pd.DataFrame(res)
                    if df is not None and not df.empty:
                        return df
                except Exception:
                    continue
            return pd.DataFrame()

        safe_id = str(entity_id).replace("'", "''")

        # Step 1: fetch entity row (without type join, for robustness)
        q_entity_variants = [
            f"""
            SELECT e.ent_type              AS ent_type,
                   e.labl                  AS ent_name,
                   e.short_labl            AS ent_short_name,
                   e.text                  AS ent_text,
                   e.additivity            AS ent_additivity,
                   e.continuous            AS rate_continuous,
                   e.top_ID                AS rate_top,
                   e.bottom_ID             AS rate_bottom,
                   e.mult                  AS rate_mult,
                   e.root_unit             AS cube_root_unit,
                   e.root_name             AS cube_root_name,
                   e.theme_id              AS theme_id,
                   e.rate_type             AS rate_type,
                   e.cube_display          AS cube_display,
                   e.cube_download         AS cube_download
            FROM   hgis.g_data_ent e
            WHERE  e.ent_ID = '{safe_id}'
            LIMIT 1;
            """,
            f"""
            SELECT e.ent_type,
                   e.labl          AS ent_name,
                   e.short_labl    AS ent_short_name,
                   e.text          AS ent_text,
                   e.additivity    AS ent_additivity,
                   e.continuous    AS rate_continuous,
                   e.top_ID        AS rate_top,
                   e.bottom_ID     AS rate_bottom,
                   e.mult          AS rate_mult,
                   e.root_unit     AS cube_root_unit,
                   e.root_name     AS cube_root_name,
                   e.theme_id      AS theme_id,
                   e.rate_type     AS rate_type,
                   e.cube_display  AS cube_display,
                   e.cube_download AS cube_download
            FROM   g_data_ent e
            WHERE  UPPER(e.ent_ID) = UPPER('{safe_id}')
            LIMIT 1;
            """,
        ]
        df_entity = exec_df(q_entity_variants)

        entity: dict = {}
        ent_type_code = None
        if not df_entity.empty:
            row0 = df_entity.iloc[0].to_dict()
            entity = {k: row0.get(k) for k in [
                "ent_type",
                "ent_name",
                "ent_short_name",
                "ent_text",
                "ent_additivity",
                "rate_continuous",
                "rate_top",
                "rate_bottom",
                "rate_mult",
                "cube_root_unit",
                "cube_root_name",
                "theme_id",
                "rate_type",
                "cube_display",
                "cube_download",
            ]}
            ent_type_code = entity.get("ent_type")

        # Step 2: add type metadata from ent_type table
        type_name = None
        type_text = None
        if ent_type_code:
            q_type_variants = [
                f"SELECT labl AS type_name, text AS type_text FROM hgis.g_data_ent_type WHERE ent_type = '{ent_type_code}' LIMIT 1;",
                f"SELECT labl AS type_name, text AS type_text FROM g_data_ent_type WHERE ent_type = '{ent_type_code}' LIMIT 1;",
            ]
            df_type = exec_df(q_type_variants)
            if not df_type.empty:
                t0 = df_type.iloc[0].to_dict()
                type_name = t0.get("type_name")
                type_text = t0.get("type_text")
        if entity is None:
            entity = {}
        entity["ent_type"] = ent_type_code
        if type_name is not None:
            entity["type_name"] = type_name
        if type_text is not None:
            entity["type_text"] = type_text

        # Step 3: relationships (higher/lower)
        q_higher_variants = [
            f"""
            SELECT e.ent_ID as higher_id, e.labl as higher_name, e.ent_type as higher_type
            FROM   hgis.g_data_ent e, hgis.g_data_rel r
            WHERE  r.rel_ID = e.ent_ID AND r.ent_ID = '{safe_id}'
            ORDER BY r.rel_seq;
            """,
            f"""
            SELECT e.ent_ID as higher_id, e.labl as higher_name, e.ent_type as higher_type
            FROM   g_data_ent e, g_data_rel r
            WHERE  r.rel_ID = e.ent_ID AND r.ent_ID = '{safe_id}'
            ORDER BY r.rel_seq;
            """,
        ]
        df_hi = exec_df(q_higher_variants)
        higher = []
        if not df_hi.empty:
            for _, r in df_hi.iterrows():
                higher.append(
                    {
                        "id": str(r.get("higher_id") if "higher_id" in df_hi.columns else r.get(0) or ""),
                        "name": str(r.get("higher_name") if "higher_name" in df_hi.columns else r.get(1) or ""),
                        "type": str(r.get("higher_type") if "higher_type" in df_hi.columns else r.get(2) or ""),
                    }
                )

        q_lower_variants = [
            f"""
            SELECT e.ent_ID as lower_id, e.labl as lower_name, e.ent_type as lower_type
            FROM   hgis.g_data_ent e, hgis.g_data_rel r
            WHERE  r.ent_ID = e.ent_ID AND r.rel_ID = '{safe_id}'
            ORDER BY r.rel_seq;
            """,
            f"""
            SELECT e.ent_ID as lower_id, e.labl as lower_name, e.ent_type as lower_type
            FROM   g_data_ent e, g_data_rel r
            WHERE  r.ent_ID = e.ent_ID AND r.rel_ID = '{safe_id}'
            ORDER BY r.rel_seq;
            """,
        ]
        df_lo = exec_df(q_lower_variants)
        lower = []
        if not df_lo.empty:
            for _, r in df_lo.iterrows():
                lower.append(
                    {
                        "id": str(r.get("lower_id") if "lower_id" in df_lo.columns else r.get(0) or ""),
                        "name": str(r.get("lower_name") if "lower_name" in df_lo.columns else r.get(1) or ""),
                        "type": str(r.get("lower_type") if "lower_type" in df_lo.columns else r.get(2) or ""),
                    }
                )

        out = {"entity": entity or {}, "higher_entities": higher, "lower_entities": lower}
        return json.dumps(out, ensure_ascii=False)
    except Exception as e:
        logger.error(f"[get_data_entity_info] Error for {entity_id}: {e}", exc_info=True)
        return json.dumps({})
