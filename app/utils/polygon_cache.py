# app/utils/polygon_cache.py (updated with advanced spatial caching)
from typing import Optional, Dict, Tuple, List, Set, Any
import geopandas as gpd
from datetime import datetime
import hashlib
import json
from shapely.geometry import box, Polygon, MultiPolygon
from shapely.ops import unary_union
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
        self._bbox_cache: Dict[str, Dict[str, Tuple[gpd.GeoDataFrame, Polygon, datetime]]] = {}
        self._expiry_time = 3600  # Cache expires after 1 hour
        self._bbox_expiry_time = 300  # Bounding box cache expires after 5 minutes
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

    def _generate_bbox_cache_key(self, unit_type: str, bbox_geom: Polygon, year_range: Optional[Tuple[int, int]] = None) -> str:
        """Generate a unique cache key for a unit type, bounding box, and year range."""
        # Round bbox coordinates to reduce small variations in cache keys
        minx, miny, maxx, maxy = [round(coord, 4) for coord in bbox_geom.bounds]
        
        bbox_str = f"{minx}_{miny}_{maxx}_{maxy}"
        
        if year_range:
            return f"{unit_type}_{bbox_str}_{year_range[0]}_{year_range[1]}"
        return f"{unit_type}_{bbox_str}"

    def _is_cache_valid(self, timestamp: datetime, is_bbox: bool = False) -> bool:
        """Check if the cached data is still valid."""
        expiry_time = self._bbox_expiry_time if is_bbox else self._expiry_time
        return (datetime.now() - timestamp).total_seconds() < expiry_time

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

    def _query_database_by_bbox(self, unit_type: str, bbox_geom: Polygon, start_year: Optional[int] = None, end_year: Optional[int] = None) -> pd.DataFrame:
        """
        Query the database for polygons of a specific unit type within a bounding box.
        Optionally filter by year range for time-dependent unit types.
        """
        # Convert bbox to database projection (ETRS-LAEA, EPSG:3034)
        bbox_geom_proj = gpd.GeoSeries([bbox_geom], crs=4326).to_crs(epsg=3034)[0]
        
        # Build the date filter if applicable
        date_filter = ""
        timeless_unit_types = [k for k, v in UNIT_TYPES.items() if v['timeless']]
        
        if unit_type not in timeless_unit_types:
            if start_year is not None and end_year is not None:
                date_filter = f"""
                AND util.get_start_year(g_duration) <= {end_year}
                AND util.get_end_year(g_duration) >= {start_year}
                """
            elif start_year is not None:
                date_filter = f"""
                AND util.get_end_year(g_duration) >= {start_year}
                """
            elif end_year is not None:
                date_filter = f"""
                AND util.get_start_year(g_duration) <= {end_year}
                """

        # Create a WKT representation of the bounding box for the spatial filter
        bbox_wkt = bbox_geom_proj.wkt
        
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
        AND public.ST_Intersects(g_foot_ertslcc, public.ST_GeomFromText('{bbox_wkt}', 3034))
        {date_filter};
        """

        # Execute query and create DataFrame
        try:
            logger.debug(f"Executing bbox query for {unit_type}: {query}")
            res = self.db.run(query, fetch="cursor")
            res = list(res.mappings())
            return pd.DataFrame(res)
        except Exception as e:
            logger.error(f"Error executing bbox query: {e}", exc_info=True)
            return pd.DataFrame()

    def get_uncovered_regions(self, unit_type: str, request_bbox: Polygon, year_range: Optional[Tuple[int, int]] = None) -> List[Polygon]:
        """
        Determine which parts of the requested bounding box aren't covered by existing cache.
        
        Args:
            unit_type: The unit type to check
            request_bbox: The bounding box being requested
            year_range: Optional year range for time-dependent units
            
        Returns:
            List of polygons representing areas not covered by cache
        """
        # If this unit type hasn't been cached yet, the entire bbox is uncovered
        if unit_type not in self._bbox_cache:
            return [request_bbox]
            
        # Get all cached regions for this unit type
        cached_regions = []
        for key, (gdf, region, timestamp) in self._bbox_cache[unit_type].items():
            # Only include valid cache entries
            if self._is_cache_valid(timestamp, is_bbox=True):
                # If year range is specified, only include matching entries
                if year_range:
                    if key.endswith(f"_{year_range[0]}_{year_range[1]}"):
                        cached_regions.append(region)
                else:
                    # For non-year specific requests, include entries without year suffix
                    if "_" not in key.split(unit_type + "_")[1]:
                        cached_regions.append(region)
                        
        # If no cached regions, return the entire bbox
        if not cached_regions:
            return [request_bbox]
            
        # Combine all cached regions into a single geometry
        union_cached = unary_union(cached_regions)
        
        # Get the difference between requested bbox and cached regions
        uncovered = request_bbox.difference(union_cached)
        
        # If nothing is uncovered, return empty list
        if uncovered.is_empty:
            return []
            
        # If the result is a single polygon, return it as a list
        if isinstance(uncovered, Polygon):
            return [uncovered]
            
        # If the result is a multipolygon, return list of constituent polygons
        if isinstance(uncovered, MultiPolygon):
            return list(uncovered.geoms)
            
        # Fallback: just return the entire request_bbox
        return [request_bbox]

    def get_polygons_by_bbox(self, unit_type: str, bbox_geom: Polygon, start_year: Optional[int] = None, end_year: Optional[int] = None) -> gpd.GeoDataFrame:
            """
            Get polygons within a bounding box, filtered by unit type and optional year range.
            Uses smart caching to reduce database load for frequently requested areas.
            
            Args:
                unit_type (str): Type of unit to fetch (e.g., 'MOD_REG', 'MOD_DIST')
                bbox_geom (Polygon): Shapely polygon representing the bounding box
                start_year (int, optional): Start year for time-dependent units
                end_year (int, optional): End year for time-dependent units
                
            Returns:
                gpd.GeoDataFrame: GeoDataFrame containing polygons within the bounding box
            """
            # Create a year range tuple for cache key if years are provided
            year_range = None
            if start_year is not None and end_year is not None:
                year_range = (start_year, end_year)
            
            # Initialize the unit type's cache dict if it doesn't exist
            if unit_type not in self._bbox_cache:
                self._bbox_cache[unit_type] = {}
                logger.debug(f"CACHE: Initialized empty cache for unit_type {unit_type}")
            
            # Generate cache key for this request
            cache_key = self._generate_bbox_cache_key(unit_type, bbox_geom, year_range)
            logger.debug(f"CACHE: Generated cache key {cache_key}")
            
            # Check if we have this exact request in cache
            if cache_key in self._bbox_cache[unit_type]:
                cached_entry = self._bbox_cache[unit_type][cache_key]
                cache_time = cached_entry[2]
                is_valid = self._is_cache_valid(cache_time, is_bbox=True)
                logger.debug(f"CACHE: Found existing cache entry for {cache_key}, age={datetime.now() - cache_time}, valid={is_valid}")
                
                if is_valid:
                    cached_gdf = cached_entry[0]
                    logger.info(f"CACHE: Using exact cached polygons for bbox request: {cache_key}, with {len(cached_gdf)} polygons")
                    return cached_gdf
                else:
                    logger.debug(f"CACHE: Entry expired, will refresh")
            else:
                logger.debug(f"CACHE: No exact match found for {cache_key}")
            
            # Find uncovered regions (parts of this bbox not in cache)
            uncovered_regions = self.get_uncovered_regions(unit_type, bbox_geom, year_range)
            logger.debug(f"CACHE: Found {len(uncovered_regions)} uncovered regions")
            for i, region in enumerate(uncovered_regions):
                logger.debug(f"CACHE: Uncovered region {i} bounds: {region.bounds}")
            
            if not uncovered_regions:
                logger.debug("CACHE: No uncovered regions, will combine from existing cache")
                # If there are no uncovered regions, we can combine existing cache entries
                all_features = []
                intersecting_regions = []
                
                for key, (gdf, region, timestamp) in self._bbox_cache[unit_type].items():
                    if self._is_cache_valid(timestamp, is_bbox=True):
                        # Check if this cached region intersects our request
                        if region.intersects(bbox_geom):
                            intersecting_regions.append(key)
                            # Filter GeoDataFrame to only include features that intersect our bbox
                            gdf_filtered = gdf[gdf.intersects(bbox_geom)]
                            logger.debug(f"CACHE: Adding {len(gdf_filtered)} features from cached region {key}")
                            all_features.append(gdf_filtered)
                
                logger.debug(f"CACHE: Found {len(intersecting_regions)} intersecting cached regions: {intersecting_regions}")
                
                # Combine all intersecting features
                if all_features:
                    combined_gdf = pd.concat(all_features).drop_duplicates(subset=['id'])
                    logger.info(f"CACHE: Combined {len(combined_gdf)} polygons from {len(all_features)} existing cache regions")
                    
                    # Cache this combined result
                    self._bbox_cache[unit_type][cache_key] = (combined_gdf, bbox_geom, datetime.now())
                    logger.debug(f"CACHE: Stored combined result under key {cache_key}")
                    
                    return combined_gdf
                
                # Fallback - query database for the whole bbox
                logger.debug("CACHE: No usable cached data found despite no uncovered regions, falling back to DB query for whole bbox")
                uncovered_regions = [bbox_geom]
            
            # Query database for each uncovered region and combine results
            all_new_features = []
            
            for i, region in enumerate(uncovered_regions):
                logger.debug(f"CACHE: Querying DB for uncovered region {i} with bounds {region.bounds}")
                df = self._query_database_by_bbox(unit_type, region, start_year, end_year)
                
                if not df.empty:
                    logger.debug(f"CACHE: Got {len(df)} rows from DB for region {i}")
                    # Convert to GeoDataFrame
                    gdf = self._convert_to_gdf(df)
                    
                    # Store in bbox cache with this region
                    region_key = self._generate_bbox_cache_key(unit_type, region, year_range)
                    self._bbox_cache[unit_type][region_key] = (gdf, region, datetime.now())
                    logger.debug(f"CACHE: Cached {len(gdf)} new polygons under key {region_key}")
                    
                    all_new_features.append(gdf)
                else:
                    logger.debug(f"CACHE: No data returned from DB for region {i}")
            
            logger.debug(f"CACHE: Got {len(all_new_features)} new GeoDataFrames from DB queries")
            
            # Combine all results (both from cache and new queries)
            all_cached_features = []
            
            # Add features from existing cache that intersect our bbox
            cache_entries_used = []
            for key, (gdf, region, timestamp) in self._bbox_cache[unit_type].items():
                if self._is_cache_valid(timestamp, is_bbox=True):
                    if region.intersects(bbox_geom):
                        cache_entries_used.append(key)
                        gdf_filtered = gdf[gdf.intersects(bbox_geom)]
                        logger.debug(f"CACHE: Adding {len(gdf_filtered)} features from cached region {key}")
                        all_cached_features.append(gdf_filtered)
            
            logger.debug(f"CACHE: Used {len(cache_entries_used)} existing cache entries: {cache_entries_used}")
            
            # Combine new features with cached ones
            all_features = all_cached_features + all_new_features
            
            if all_features:
                logger.debug(f"CACHE: Combining {len(all_cached_features)} cached GDFs and {len(all_new_features)} new GDFs")
                # Combine and remove duplicates
                combined_gdf = pd.concat(all_features).drop_duplicates(subset=['id'])
                
                # Cache this combined result
                self._bbox_cache[unit_type][cache_key] = (combined_gdf, bbox_geom, datetime.now())
                logger.debug(f"CACHE: Stored final combined result ({len(combined_gdf)} polygons) under key {cache_key}")
                
                logger.info(f"CACHE: Returning {len(combined_gdf)} polygons for bbox request (combined from {len(all_features)} sources)")
                return combined_gdf
            
            logger.info("CACHE: No features found for this request")
            # Return empty GeoDataFrame if no results
            return gpd.GeoDataFrame()


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
        # (Rest of the method remains unchanged)
        # Existing implementation...

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

    def clear_cache(self):
        """Clear the entire in-memory cache."""
        self._cache.clear()
        self._bbox_cache.clear()

    def remove_from_cache(self, unit_type: str, year: Optional[int] = None):
        """Remove specific entry from cache (both in memory and on disk if applicable)."""
        cache_key = self._generate_cache_key(unit_type, year)

        # Remove from in-memory cache
        self._cache.pop(cache_key, None)
        
        # Remove any bbox cache entries for this unit type
        if unit_type in self._bbox_cache:
            self._bbox_cache.pop(unit_type, None)

        # If disk-based caching is used, remove the file too
        if unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
            file_path = self._disk_file_path(cache_key)
            if os.path.exists(file_path):
                os.remove(file_path)

# Create a global instance of the cache
polygon_cache = PolygonCache()