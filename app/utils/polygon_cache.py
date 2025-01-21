#app/utils/polygon_cache.py
from typing import Optional, Dict, Tuple
import geopandas as gpd
from datetime import datetime
import hashlib
import json
from utils.constants import UNIT_TYPES, TIMELESS_UNIT_TYPES, UNIT_TYPES_DISK
from config import load_config, get_db
import shapely
import pandas as pd
import os

class PolygonCache:
    def __init__(self):
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
        self._expiry_time = 3600  # Cache expires after 1 hour
        self.config = load_config()
        self.db = get_db(self.config)

        # Directory for saving/loading polygon files
        self.disk_cache_dir = "./polygon_cache"
        os.makedirs(self.disk_cache_dir, exist_ok=True)

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
                print(f"Error reading file {file_path}: {e}")
        return None

    def _save_to_disk(self, gdf: gpd.GeoDataFrame, cache_key: str) -> None:
        """Save a GeoDataFrame to disk as GeoJSON."""
        file_path = self._disk_file_path(cache_key)
        try:
            gdf.to_file(file_path, driver="GeoJSON")
        except Exception as e:
            print(f"Error saving file {file_path}: {e}")

    def get_polygons(self, unit_type: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> gpd.GeoDataFrame:
        """
        Get polygons from cache or database for the specified unit type and year range.
        Returns a GeoDataFrame with the properly formatted geometries.
        """
        cache_key = self._generate_cache_key(unit_type, start_year, end_year)

        # 1) If this unit_type is designated for disk caching, try to load from disk first
        if unit_type in UNIT_TYPES_DISK:
            gdf_from_disk = self._load_from_disk(cache_key)
            if gdf_from_disk is not None:
                gdf_from_disk.set_index('g_unit', inplace=True, drop=False)
                return gdf_from_disk

        # 2) Otherwise, check the in-memory cache
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

        # Cache the raw DataFrame in memory if not empty
        if not df.empty:
            self._cache[cache_key] = (df, datetime.now())

        # Convert to GeoDataFrame
        gdf = self._convert_to_gdf(df)

        # If this is a disk-based unit type, save to disk
        if unit_type in UNIT_TYPES_DISK and not gdf.empty:
            self._save_to_disk(gdf, cache_key)

        return gdf

    def clear_cache(self):
        """Clear the entire in-memory cache."""
        self._cache.clear()
        # Optionally, clear all disk-based caches as well:
        # for filename in os.listdir(self.disk_cache_dir):
        #     if filename.endswith(".geojson"):
        #         os.remove(os.path.join(self.disk_cache_dir, filename))

    def remove_from_cache(self, unit_type: str, start_year: Optional[int] = None, end_year: Optional[int] = None):
        """Remove specific entry from cache (both in memory and on disk if applicable)."""
        cache_key = self._generate_cache_key(unit_type, start_year, end_year)

        # Remove from in-memory cache
        self._cache.pop(cache_key, None)

        # If disk-based caching is used, remove the file too
        if unit_type in UNIT_TYPES_DISK:
            file_path = self._disk_file_path(cache_key)
            if os.path.exists(file_path):
                os.remove(file_path)

# Create a global instance of the cache
polygon_cache = PolygonCache()