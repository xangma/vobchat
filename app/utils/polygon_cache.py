from typing import Optional, Dict, Tuple
import geopandas as gpd
from datetime import datetime
import hashlib
import json
from utils.constants import UNIT_TYPES, TIMELESS_UNIT_TYPES
from config import load_config, get_db
import shapely
import pandas as pd

class PolygonCache:
    def __init__(self):
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
        self._expiry_time = 3600  # Cache expires after 1 hour
        self.config = load_config()
        self.db = get_db(self.config)

    def _generate_cache_key(self, unit_type: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> str:
        """Generate a unique cache key based on the query parameters."""
        key_parts = [unit_type]
        if start_year is not None:
            key_parts.append(str(start_year))
        if end_year is not None:
            key_parts.append(str(end_year))
        key_string = '_'.join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()

    def _is_cache_valid(self, timestamp: datetime) -> bool:
        """Check if the cached data is still valid."""
        return (datetime.now() - timestamp).total_seconds() < self._expiry_time

    def _convert_to_gdf(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        """Convert a pandas DataFrame to a GeoDataFrame with proper projections."""
        if df.empty:
            return gpd.GeoDataFrame()
            
        # Convert WKB to geometry
        df['geometry'] = df['g_foot_ertslcc'].apply(lambda x: shapely.from_wkb(x))
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        
        # Set the index and CRS
        gdf.set_index('g_unit', inplace=True)
        gdf.set_crs(epsg=3034, inplace=True)
        gdf = gdf.to_crs(epsg=4326)
        
        return gdf

    def get_polygons(self, unit_type: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> gpd.GeoDataFrame:
        """
        Get polygons from cache or database for the specified unit type and year range.
        Returns a GeoDataFrame with the properly formatted geometries.
        """
        cache_key = self._generate_cache_key(unit_type, start_year, end_year)
        
        # Check if we have a valid cached version
        if cache_key in self._cache:
            df, timestamp = self._cache[cache_key]
            if self._is_cache_valid(timestamp):
                return self._convert_to_gdf(df)

        # Build the date filter if applicable
        date_filter = ""
        if start_year is not None and end_year is not None and unit_type not in TIMELESS_UNIT_TYPES:
            date_filter = f"""
            AND util.get_start_year(g_duration) <= {end_year}
            AND util.get_end_year(g_duration) >= {start_year}
            """
        
        query = f"""
        SELECT 
            g_unit, 
            g_foot_ertslcc,
            g_unit_type,
            auo_util.get_unit_name(g_unit) as unit_name, 
            util.get_start_year(g_duration) as start_year, 
            util.get_end_year(g_duration) as end_year
        FROM hgis.g_foot 
        WHERE g_unit_type='{unit_type}'
        AND use_for_stat_map='Y'
        {date_filter};
        """
        
        # Execute query and create DataFrame
        res = self.db.run(query, fetch="cursor")
        res = list(res.mappings())
        df = pd.DataFrame(res)
        
        # Cache the raw DataFrame
        if not df.empty:
            self._cache[cache_key] = (df, datetime.now())
        
        # Return as GeoDataFrame
        return self._convert_to_gdf(df)

    def clear_cache(self):
        """Clear the entire cache."""
        self._cache.clear()

    def remove_from_cache(self, unit_type: str, start_year: Optional[int] = None, end_year: Optional[int] = None):
        """Remove specific entry from cache."""
        cache_key = self._generate_cache_key(unit_type, start_year, end_year)
        self._cache.pop(cache_key, None)
# Create a global instance of the cache
polygon_cache = PolygonCache()