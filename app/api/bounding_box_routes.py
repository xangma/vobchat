# app/api/bounding_box_routes.py

from flask import jsonify, request
from typing import Optional, Dict, List, Tuple
import logging
import json
import geopandas as gpd
import shapely
from shapely.geometry import box
import pandas as pd

# Import polygon cache and other utilities
from ..utils.polygon_cache import polygon_cache
from ..utils.constants import UNIT_TYPES

logger = logging.getLogger(__name__)

def register_bounding_box_routes(server):
    """Register API routes for polygon retrieval using bounding box filters."""
    
    @server.route('/api/polygons/bbox', methods=['GET'])
    def get_polygons_by_bbox():
        """
        API endpoint to fetch polygons within a specified bounding box.
        
        Query Parameters:
            unit_types (str): Comma-separated list of unit types to fetch (e.g., 'MOD_REG,MOD_DIST')
            minX (float): Minimum X coordinate (longitude) of bounding box
            minY (float): Minimum Y coordinate (latitude) of bounding box
            maxX (float): Maximum X coordinate (longitude) of bounding box
            maxY (float): Maximum Y coordinate (latitude) of bounding box
            start_year (int, optional): Start year for time-dependent units
            end_year (int, optional): End year for time-dependent units
            
        Returns:
            JSON: GeoJSON representation of the polygons within the bounding box
        """
        try:
            # Get required parameters
            unit_types_str = request.args.get('unit_types')
            min_x = request.args.get('minX')
            min_y = request.args.get('minY')
            max_x = request.args.get('maxX')
            max_y = request.args.get('maxY')
            
            # Validate required parameters
            if not all([unit_types_str, min_x, min_y, max_x, max_y]):
                return jsonify({"error": "Missing required parameters. Required: unit_types, minX, minY, maxX, maxY"}), 400
                
            # Parse parameters
            unit_types = unit_types_str.split(',')
            bbox = {
                'minX': float(min_x),
                'minY': float(min_y),
                'maxX': float(max_x),
                'maxY': float(max_y)
            }
            
            # Get optional year range parameters
            start_year = request.args.get('start_year')
            end_year = request.args.get('end_year')
            
            # Convert to integers if provided
            start_year = int(start_year) if start_year else None
            end_year = int(end_year) if end_year else None
            
            # Create the bounding box geometry
            bbox_geom = box(bbox['minX'], bbox['minY'], bbox['maxX'], bbox['maxY'])
            
            # Add a client_cache parameter to track if client already has data
            client_has_data = request.args.get('client_has_data', 'false').lower() == 'true'
            request_id = request.args.get('request_id', '')
            
            # Log the request
            logger.info(f"Bbox request: {request_id} for {unit_types_str} bbox={bbox} client_has_data={client_has_data}")
            
            # Check if this request can be short-circuited
            if client_has_data:
                # Simply return a success response with no features
                # The client will use its cached data
                logger.info(f"Request {request_id}: Client has data, returning empty response")
                return jsonify({"type": "FeatureCollection", "features": [], "useCachedFeatures": True})
            
            # Initialize the result GeoJSON
            result_geojson = {"type": "FeatureCollection", "features": []}
            total_features = 0
            
            # Get polygons for each unit type and filter by bounding box
            for unit_type in unit_types:
                # Validate unit type
                if unit_type not in UNIT_TYPES:
                    logger.warning(f"Invalid unit type: {unit_type}")
                    continue
                    
                # Get polygons from the cache with bounding box filter
                gdf = polygon_cache.get_polygons_by_bbox(unit_type, bbox_geom, start_year, end_year)
                
                # Skip if no polygons found for this unit type
                if gdf.empty:
                    continue
                    
                # Convert to GeoJSON and merge with result
                geojson = json.loads(gdf.to_json())
                result_geojson["features"].extend(geojson["features"])
                total_features += len(geojson["features"])
                
            logger.info(f"Request {request_id}: Returned {total_features} polygons within bounding box")
            
            return jsonify(result_geojson)
            
        except Exception as e:
            logger.error(f"Error retrieving polygons by bounding box: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500