#app/utils/polygon_cache.py
from typing import Optional, Dict, Tuple, List, Set
import geopandas as gpd
from datetime import datetime
import hashlib
import json
from .constants import UNIT_TYPES
from ..config import load_config, get_db
import shapely
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

class PolygonCache:
    def __init__(self):
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
        self._expiry_time = 3600  # Cache expires after 1 hour
        self.config = load_config()
        self.db = get_db(self.config)

        # Directory for saving/loading polygon files
        self.disk_cache_dir = "./polygon_cache"
        os.makedirs(self.disk_cache_dir, exist_ok=True)

    def _generate_cache_key(self, unit_type: str, year: Optional[int] = None) -> str:
        """Generate a unique cache key for a unit type and specific year."""
        if year is not None:
            return f"{unit_type}_{year}"
        return unit_type

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
        gdf['id'] = gdf.index

        return gdf

    def _disk_file_path(self, cache_key: str) -> str:
        """Return the file path for a given cache key."""
        return os.path.join(self.disk_cache_dir, f"{cache_key}.geojson")

    def _load_from_disk(self, cache_key: str) -> Optional[gpd.GeoDataFrame]:
        """Attempt to load a GeoDataFrame from disk if it exists."""
        file_path = self._disk_file_path(cache_key)
        if os.path.exists(file_path):
            # Load from disk
            try:
                return gpd.read_file(file_path)
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")
        return None

    def _save_to_disk(self, gdf: gpd.GeoDataFrame, cache_key: str) -> None:
        """Save a GeoDataFrame to disk as GeoJSON."""
        file_path = self._disk_file_path(cache_key)
        try:
            gdf.to_file(file_path, driver="GeoJSON")
        except Exception as e:
            logger.error(f"Error saving file {file_path}: {e}")

    def _query_database_by_year(self, unit_type: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Query the database for polygons of a specific unit type and year.
        If year is None, get all polygons for that unit type.
        """
        # Build the date filter if applicable
        date_filter = ""
        timeless_unit_types = [k for k, v in UNIT_TYPES.items() if v['timeless']]
        
        if year is not None and unit_type not in timeless_unit_types:
            date_filter = f"""
            AND util.get_start_year(g_duration) <= {year}
            AND util.get_end_year(g_duration) >= {year}
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
        return pd.DataFrame(res)

    def get_polygons(self, unit_type: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> gpd.GeoDataFrame:
        """
        Get polygons from cache or database for the specified unit type and year range.
        For year ranges, data is now cached by individual years for more efficient reuse.
        Returns a GeoDataFrame with the properly formatted geometries.
        """
        # Case 1: Unit type is timeless or no year range specified
        timeless_unit_types = [k for k, v in UNIT_TYPES.items() if v['timeless']]
        if unit_type in timeless_unit_types or (start_year is None and end_year is None):
            cache_key = self._generate_cache_key(unit_type)
            
            # Try to load from disk first if applicable
            if unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
                gdf_from_disk = self._load_from_disk(cache_key)
                if gdf_from_disk is not None:
                    if 'g_unit' in gdf_from_disk.columns:
                        gdf_from_disk.set_index('g_unit', inplace=True, drop=False)
                    return gdf_from_disk

            # Check memory cache
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key][1]):
                return self._convert_to_gdf(self._cache[cache_key][0])
            
            # Query database if not in cache
            df = self._query_database_by_year(unit_type)
            
            if not df.empty:
                self._cache[cache_key] = (df, datetime.now())
                
                # Convert to GeoDataFrame and save to disk if applicable
                gdf = self._convert_to_gdf(df)
                if unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
                    self._save_to_disk(gdf, cache_key)
                
                return gdf
            return gpd.GeoDataFrame()
            
        # Case 2: Year range specified - we'll cache by individual years
        if start_year is not None and end_year is not None:
            # For large ranges or testing, limit the range to something reasonable
            if end_year - start_year > 100:
                logger.warning(f"Year range too large ({start_year}-{end_year}), limiting to 100 years")
                end_year = start_year + 100
                
            year_range = range(start_year, end_year + 1)
            
            # Determine which years we need to fetch from the database
            years_to_fetch = []
            cached_year_dfs = []
            
            for year in year_range:
                year_cache_key = self._generate_cache_key(unit_type, year)
                
                # Try disk cache first
                if unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
                    gdf_from_disk = self._load_from_disk(year_cache_key)
                    if gdf_from_disk is not None:
                        if 'g_unit' in gdf_from_disk.columns:
                            gdf_from_disk.set_index('g_unit', inplace=True, drop=False)
                        cached_year_dfs.append(gdf_from_disk)
                        continue
                
                # Check memory cache
                if year_cache_key in self._cache and self._is_cache_valid(self._cache[year_cache_key][1]):
                    cached_year_dfs.append(self._convert_to_gdf(self._cache[year_cache_key][0]))
                else:
                    years_to_fetch.append(year)
            
            # Fetch missing years from database
            if years_to_fetch:
                logger.info(f"Fetching {len(years_to_fetch)} missing years for {unit_type}: {years_to_fetch}")
                
                # Optimize by fetching all missing years in fewer queries
                # Group consecutive years for efficiency
                year_groups = []
                current_group = []
                
                for year in sorted(years_to_fetch):
                    if not current_group or year == current_group[-1] + 1:
                        current_group.append(year)
                    else:
                        year_groups.append(current_group)
                        current_group = [year]
                
                if current_group:
                    year_groups.append(current_group)
                
                for group in year_groups:
                    # For efficiency, query the database once for the entire group
                    group_start = group[0]
                    group_end = group[-1]
                    
                    # Build the date filter for the group
                    date_filter = f"""
                    AND util.get_start_year(g_duration) <= {group_end}
                    AND util.get_end_year(g_duration) >= {group_start}
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
                    group_df = pd.DataFrame(res)
                    
                    if not group_df.empty:
                        # Process each year in the group separately for caching
                        for year in group:
                            # Filter the result for this specific year
                            year_df = group_df[
                                (group_df['start_year'].fillna(0) <= year) & 
                                (group_df['end_year'].fillna(3000) >= year)
                            ].copy()
                            
                            # Cache the result
                            year_cache_key = self._generate_cache_key(unit_type, year)
                            self._cache[year_cache_key] = (year_df, datetime.now())
                            
                            # Convert to GeoDataFrame
                            year_gdf = self._convert_to_gdf(year_df)
                            
                            # Save to disk if applicable
                            if unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
                                self._save_to_disk(year_gdf, year_cache_key)
                            
                            cached_year_dfs.append(year_gdf)
            
            # Combine all GeoDataFrames
            if cached_year_dfs:
                combined_gdf = pd.concat(cached_year_dfs).drop_duplicates()
                if not combined_gdf.empty:
                    return combined_gdf
            
            return gpd.GeoDataFrame()
            
        # Fallback for any other case
        return gpd.GeoDataFrame()

    def clear_cache(self):
        """Clear the entire in-memory cache."""
        self._cache.clear()
        # Optionally, clear all disk-based caches as well:
        # for filename in os.listdir(self.disk_cache_dir):
        #     if filename.endswith(".geojson"):
        #         os.remove(os.path.join(self.disk_cache_dir, filename))

    def remove_from_cache(self, unit_type: str, year: Optional[int] = None):
        """Remove specific entry from cache (both in memory and on disk if applicable)."""
        cache_key = self._generate_cache_key(unit_type, year)

        # Remove from in-memory cache
        self._cache.pop(cache_key, None)

        # If disk-based caching is used, remove the file too
        if unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
            file_path = self._disk_file_path(cache_key)
            if os.path.exists(file_path):
                os.remove(file_path)

# Create a global instance of the cache
polygon_cache = PolygonCache()