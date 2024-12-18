# app/utils/helpers.py
import geopandas as gpd
from typing import Dict, Optional, Union, Tuple

def calculate_center_and_zoom(gdf_filtered):
    """Helper function to calculate map center and zoom level."""
    if gdf_filtered.empty:
        return {"center": None, "zoom": None}
    
    bounds = gdf_filtered.total_bounds
    min_lon, min_lat, max_lon, max_lat = bounds
    
    center_lon = (min_lon + max_lon) / 2
    center_lat = (min_lat + max_lat) / 2
    
    zoom = 10
    if max_lon - min_lon > 0 and max_lat - min_lat > 0:
        zoom = 12 - max(max_lon - min_lon, max_lat - min_lat)
    
    return {
        "center": {"lon": center_lon, "lat": center_lat},
        "zoom": zoom
    }