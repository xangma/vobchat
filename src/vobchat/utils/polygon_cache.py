# app/utils/polygon_cache.py
from typing import Optional, Dict, Tuple, List, Set, Any
import geopandas as gpd
from datetime import datetime
import hashlib
import json
from shapely.geometry import box, Polygon, MultiPolygon
from shapely.ops import unary_union
from vobchat.utils.constants import UNIT_TYPES
from vobchat.config import load_config, get_db
import shapely
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

class PolygonCache:
    def __init__(self):
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
        self._features_by_id: Dict[str, gpd.GeoDataFrame] = {}  # Cache by feature ID
        self._features_by_unit_type: Dict[str, Set[str]] = {}  # Index of IDs by unit type
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

    def _query_database_by_id(
        self,
        unit_type: str,
        feature_ids: List[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> pd.DataFrame:
        """
            Query the database for polygons of a specific unit type and feature IDs.
            Optionally filter by year range for time-dependent unit types.
        """
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

        # Create a comma-separated list of feature IDs for SQL IN clause
        id_list = ", ".join([str(id) for id in feature_ids])

        id_filter = f"AND g.g_unit IN ({id_list})"
        logger.debug(f"Added ID filter to include {len(feature_ids)} IDs")
        user_lang = 'eng'
        # Build the SQL query
        query = f"""
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
        SELECT  g.g_unit,
                g.g_foot_ertslcc,
                g.g_unit_type,
                un.g_name                      AS unit_name,
                util.get_start_year(g.g_duration) AS start_year,
                util.get_end_year(g.g_duration)   AS end_year
        FROM    hgis.g_foot g
        JOIN    unit_name  un
            ON un.g_unit = g.g_unit AND un.rn = 1
        AND   g.use_for_stat_map = 'Y'
        WHERE g.g_unit_type='{unit_type}'
        {id_filter}
        {date_filter};
        """
        # Execute the query and create a DataFrame
        try:
            logger.debug(f"Executing ID query for {unit_type}: {query}")
            res = self.db.run(query, fetch="cursor")
            res = list(res.mappings())
            return pd.DataFrame(res)
        except Exception as e:
            logger.error(f"Error executing ID query: {e}", exc_info=True)
            return pd.DataFrame()


    def _query_database_by_bbox(
        self,
        unit_type: str,
        bbox_geom: Polygon,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        exclude_ids: Optional[List[str]] = None,
        theme_id: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query the database for polygons of a specific unit type within a bounding box.
        Optionally filter by year range for time-dependent unit types and exclude specific IDs.
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

        # Add ID exclusion filter if needed
        id_filter = ""
        if exclude_ids and len(exclude_ids) > 0:
            id_list = "', '".join([str(id).replace("'", "''") for id in exclude_ids])
            id_filter = f"AND g.g_unit NOT IN ('{id_list}')"
            logger.debug(f"Added ID filter to exclude {len(exclude_ids)} IDs")

        # Create a WKT representation of the bounding box for the spatial filter
        bbox_wkt = bbox_geom_proj.wkt
        user_lang = 'eng'
        # Theme availability expression: mark units that have any data for the theme
        theme_select = "0 AS has_theme"
        if theme_id:
            safe_theme = str(theme_id).replace("'", "''")
            theme_select = f"""
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM hgis.g_data d
                    JOIN hgis.g_data_map m ON d.cellref = m.cellref
                    JOIN hgis.g_data_ent nc ON nc.ent_id = m.ncuberef
                    WHERE d.g_unit = g.g_unit
                      AND nc.theme_ID = '{safe_theme}'
                ) THEN 1 ELSE 0 END AS has_theme
            """

        query = f"""
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
        SELECT  g.g_unit,
                g.g_foot_ertslcc,
                g.g_unit_type,
                un.g_name                      AS unit_name,
                util.get_start_year(g.g_duration) AS start_year,
                util.get_end_year(g.g_duration)   AS end_year,
                {theme_select}
        FROM    hgis.g_foot g
        JOIN    unit_name  un
            ON un.g_unit = g.g_unit AND un.rn = 1
        AND   g.use_for_stat_map = 'Y'
        WHERE g.g_unit_type='{unit_type}'
        AND public.ST_Intersects(g_foot_ertslcc, public.ST_GeomFromText('{bbox_wkt}', 3034))
        {date_filter}
        {id_filter};
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

    def get_polygons_by_bbox(
        self,
        unit_type: str,
        bbox_geom: Polygon,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        exclude_ids: Optional[List[str]] = None,
        theme_id: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        Get polygons within a bounding box, filtered by unit type and optional year range.
        Uses feature ID-based caching to only request new features.

        Args:
            unit_type (str): Type of unit to fetch (e.g., 'MOD_REG', 'MOD_DIST')
            bbox_geom (Polygon): Shapely polygon representing the bounding box
            start_year (int, optional): Start year for time-dependent units
            end_year (int, optional): End year for time-dependent units
            exclude_ids (List[str], optional): List of feature IDs to exclude from results

        Returns:
            gpd.GeoDataFrame: GeoDataFrame containing polygons within the bounding box
        """
        # Query database for features in this bbox, excluding ones the client already has
        df = self._query_database_by_bbox(unit_type, bbox_geom, start_year, end_year, exclude_ids, theme_id)

        if df.empty:
            logger.info(f"No features found for {unit_type} in the specified bounding box")
            return gpd.GeoDataFrame()

        # Convert to GeoDataFrame
        gdf = self._convert_to_gdf(df)

        # Update feature ID cache
        if not unit_type in self._features_by_unit_type:
            self._features_by_unit_type[unit_type] = set()

        # Cache each feature by ID
        for feature_id, row in gdf.iterrows():
            str_id = str(feature_id)
            self._features_by_id[str_id] = row
            self._features_by_unit_type[unit_type].add(str_id)

        logger.info(f"Cached {len(gdf)} features for {unit_type}, total cached: {len(self._features_by_unit_type[unit_type])}")

        return gdf

    def get_polygons(self, unit_type: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> gpd.GeoDataFrame:
        """
        Get polygons from cache or database for the specified unit type and year range.
        For year ranges, data is now cached by individual years for more efficient reuse.
        Returns a GeoDataFrame with the properly formatted geometries.
        """
        # Generate cache key
        cache_key = self._generate_cache_key(unit_type, start_year)

        # Try to load from disk first if applicable for timeless unit types
        timeless_unit_types = [k for k, v in UNIT_TYPES.items() if v['timeless']]
        if unit_type in timeless_unit_types and unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
            gdf_from_disk = self._load_from_disk(cache_key)
            if gdf_from_disk is not None:
                if 'g_unit' in gdf_from_disk.columns:
                    gdf_from_disk.set_index('g_unit', inplace=True, drop=False)
                return gdf_from_disk

        # Check memory cache
        if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key][1]):
            return self._convert_to_gdf(self._cache[cache_key][0])

        # Query database if not in cache
        df = self._query_database_by_year(unit_type, start_year, end_year)

        if not df.empty:
            self._cache[cache_key] = (df, datetime.now())

            # Convert to GeoDataFrame and save to disk if applicable
            gdf = self._convert_to_gdf(df)
            if unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
                self._save_to_disk(gdf, cache_key)

            # Also cache by feature ID
            if not unit_type in self._features_by_unit_type:
                self._features_by_unit_type[unit_type] = set()

            # Cache each feature by ID
            for feature_id, row in gdf.iterrows():
                str_id = str(feature_id)
                self._features_by_id[str_id] = row
                self._features_by_unit_type[unit_type].add(str_id)

            return gdf
        return gpd.GeoDataFrame()

    def get_polygons_by_ids(
        self,
        unit_type: str,
        feature_ids: List[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> gpd.GeoDataFrame:
        """
        Get polygons by feature IDs for a specific unit type.
        Uses feature ID-based caching to only request new features.
        """
        try:

            if unit_type not in UNIT_TYPES:
                logger.error(f"Invalid unit type: {unit_type}")
                return gpd.GeoDataFrame()

            # Generate cache key
            cache_key = self._generate_cache_key(unit_type, start_year)
            # Check if the cache is valid
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key][1]):
                return self._convert_to_gdf(self._cache[cache_key][0])
            # Check if the feature IDs are already cached (string keys)
            if all(str(fid) in self._features_by_id for fid in feature_ids):
                # Retrieve from cache using string keys consistently
                rows = [self._features_by_id[str(fid)] for fid in feature_ids]
                gdf = gpd.GeoDataFrame(rows)
                gdf.set_index('g_unit', inplace=True, drop=False)
                return gdf

            # Query the database for the specified feature IDs
            df = self._query_database_by_id(unit_type, feature_ids, start_year, end_year)
            if df.empty:
                logger.info(f"No features found for {unit_type} with the specified IDs")
                return gpd.GeoDataFrame()

            if not df.empty:
                self._cache[cache_key] = (df, datetime.now())

                # Convert to GeoDataFrame and save to disk if applicable
                gdf = self._convert_to_gdf(df)
                if unit_type in [k for k, v in UNIT_TYPES.items() if v['cache_disk']]:
                    self._save_to_disk(gdf, cache_key)

                # Also cache by feature ID
                if not unit_type in self._features_by_unit_type:
                    self._features_by_unit_type[unit_type] = set()

                # Cache each feature by ID
                for feature_id, row in gdf.iterrows():
                    str_id = str(feature_id)
                    self._features_by_id[str_id] = row
                    self._features_by_unit_type[unit_type].add(str_id)

                return gdf

        except Exception as e:
            logger.error(f"Error retrieving polygons by IDs for {unit_type}: {str(e)}", exc_info=True)
            return gpd.GeoDataFrame()


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
            # Ensure the directory exists at save time as well (robust against CWD changes)
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
            gdf.to_file(file_path, driver="GeoJSON")
        except Exception as e:
            logger.error(f"Error saving file {file_path}: {e}")

    def _query_database_by_year(
        self,
        unit_type: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query the database for polygons of a specific unit type and year.
        If year is None, get all polygons for that unit type.
        """
        # Build the date filter if applicable
        date_filter = ""
        timeless_unit_types = [k for k, v in UNIT_TYPES.items() if v['timeless']]

        if start_year is not None and end_year is not None and unit_type not in timeless_unit_types:
            date_filter = f"""
            AND util.get_start_year(g_duration) <= {end_year}
            AND util.get_end_year(g_duration) >= {start_year}
            """
        elif start_year is not None and unit_type not in timeless_unit_types:
            date_filter = f"""
            AND util.get_end_year(g_duration) >= {start_year}
            """
        elif end_year is not None and unit_type not in timeless_unit_types:
            date_filter = f"""
            AND util.get_start_year(g_duration) <= {end_year}
            """
        user_lang = 'eng'
        query = f"""
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
        SELECT  g.g_unit,
                g.g_foot_ertslcc,
                g.g_unit_type,
                un.g_name                      AS unit_name,
                util.get_start_year(g.g_duration) AS start_year,
                util.get_end_year(g.g_duration)   AS end_year
        FROM    hgis.g_foot g
        JOIN    unit_name  un
            ON un.g_unit = g.g_unit AND un.rn = 1
        AND   g.use_for_stat_map = 'Y'
        WHERE g.g_unit_type='{unit_type}'
        {date_filter};
        """

        # Execute query and create DataFrame
        res = self.db.run(query, fetch="cursor")
        res = list(res.mappings())
        return pd.DataFrame(res)

    def clear_cache(self):
        """Clear the entire in-memory cache."""
        self._cache.clear()
        self._features_by_id.clear()
        self._features_by_unit_type.clear()

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

        # We don't remove the feature ID cache here, as that would affect
        # operations that might rely on previously cached features.
        # The feature ID cache is separate from the unit type + year cache.

# Create a global instance of the cache
polygon_cache = PolygonCache()
