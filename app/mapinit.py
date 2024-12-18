
from config import load_config, get_db
import os
import pandas as pd
import geopandas as gpd
import json
import shapely
from pyproj import CRS
from utils.polygon_cache import polygon_cache
from typing import Optional, Tuple

config = load_config()
db = get_db(config)


def get_polygons_by_type(unit_type: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> gpd.GeoDataFrame:
    """
    Get polygons for a specific unit type, optionally filtered by year range.
    Uses the polygon cache to reduce database load.
    """
    return polygon_cache.get_polygons(unit_type, start_year, end_year)

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

def get_mapinit_polygons() -> Tuple[gpd.GeoDataFrame, dict]:
    """
    Get initial polygons for map initialization.
    Uses the polygon cache for the GeoDataFrame.
    """
    gdf = get_polygons_by_type('MOD_REG')
    geojson = json.loads(gdf.to_json())
    return gdf, geojson