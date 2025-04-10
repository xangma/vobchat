# app/api/bounding_box_routes.py

from flask import jsonify, request
from typing import Optional, Dict, List, Tuple, Union
import logging
import json
import geopandas as gpd
import shapely
from shapely.geometry import box
import pandas as pd

# Import polygon cache and other utilities
from utils.polygon_cache import polygon_cache
from utils.constants import UNIT_TYPES

logger = logging.getLogger(__name__)

def register_bounding_box_routes(server):
    """Register API routes for polygon retrieval using bounding box filters."""
    
    def process_bbox_request(
        unit_types: List[str],
        bbox: Dict[str, float],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        exclude_ids: Optional[List[str]] = None,
        client_has_data: bool = False,
        request_id: str = ""
    ):
        """
        Process a bounding box request regardless of HTTP method (GET or POST).
        
        Args:
            unit_types: List of unit types to fetch
            bbox: Dictionary with minX, minY, maxX, maxY coordinates
            start_year: Optional start year for time-dependent units
            end_year: Optional end year for time-dependent units
            exclude_ids: Optional list of feature IDs to exclude
            client_has_data: Flag indicating if client already has data
            request_id: Optional request ID for tracking
            
        Returns:
            GeoJSON representation of the polygons within the bounding box
        """
        try:
            # Create the bounding box geometry
            bbox_geom = box(bbox['minX'], bbox['minY'], bbox['maxX'], bbox['maxY'])
            
            # Log the request
            logger.info(f"Bbox request: {request_id} for {','.join(unit_types)} bbox={bbox} client_has_data={client_has_data} exclude_ids_count={len(exclude_ids) if exclude_ids else 0}")
            
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
                gdf = polygon_cache.get_polygons_by_bbox(unit_type, bbox_geom, start_year, end_year, exclude_ids)
                
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
    
    @server.route('/api/polygons/bbox', methods=['GET', 'POST'])
    def get_polygons_by_bbox():
        """
        API endpoint to fetch polygons within a specified bounding box.
        Supports both GET and POST methods.
        
        GET Query Parameters:
            unit_types (str): Comma-separated list of unit types to fetch (e.g., 'MOD_REG,MOD_DIST')
            minX (float): Minimum X coordinate (longitude) of bounding box
            minY (float): Minimum Y coordinate (latitude) of bounding box
            maxX (float): Maximum X coordinate (longitude) of bounding box
            maxY (float): Maximum Y coordinate (latitude) of bounding box
            start_year (int, optional): Start year for time-dependent units
            end_year (int, optional): End year for time-dependent units
            exclude_ids (str, optional): Comma-separated list of feature IDs to exclude from results
            client_has_data (bool, optional): Flag indicating if the client already has data
            
        POST JSON Body:
            unit_types (List[str]): List of unit types to fetch
            bounds (Dict): Dictionary with minX, minY, maxX, maxY coordinates
            exclude_ids (List[str], optional): List of feature IDs to exclude
            start_year (int, optional): Start year for time-dependent units
            end_year (int, optional): End year for time-dependent units
            request_id (str, optional): Request ID for tracking
            
        Returns:
            JSON: GeoJSON representation of the polygons within the bounding box
        """
        # Handle differently based on HTTP method
        if request.method == 'POST':
            # Process POST request with JSON body
            try:
                data = request.get_json()
                
                # Get required parameters
                unit_types = data.get('unit_types')
                bounds = data.get('bounds')
                
                # Validate required parameters
                if not unit_types or not bounds:
                    return jsonify({"error": "Missing required parameters. Required: unit_types, bounds"}), 400
                
                # Extract bounding box coordinates
                bbox = {
                    'minX': float(bounds.get('minX')),
                    'minY': float(bounds.get('minY')),
                    'maxX': float(bounds.get('maxX')),
                    'maxY': float(bounds.get('maxY'))
                }
                
                # Get optional parameters
                start_year = data.get('start_year')
                end_year = data.get('end_year')
                exclude_ids = data.get('exclude_ids', [])
                request_id = data.get('request_id', '')
                client_has_data = data.get('client_has_data', False)
                
                # Process the request using the shared function
                return process_bbox_request(
                    unit_types=unit_types,
                    bbox=bbox,
                    start_year=start_year,
                    end_year=end_year,
                    exclude_ids=exclude_ids,
                    client_has_data=client_has_data,
                    request_id=request_id
                )
                
            except Exception as e:
                logger.error(f"Error processing POST request: {str(e)}", exc_info=True)
                return jsonify({"error": str(e)}), 400
        else:
            # Process GET request with URL parameters
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
                
                # Get exclude_ids parameter - these are IDs the client already has
                exclude_ids_str = request.args.get('exclude_ids')
                exclude_ids = exclude_ids_str.split(',') if exclude_ids_str else []
                
                # Add a client_cache parameter to track if client already has data
                client_has_data = request.args.get('client_has_data', 'false').lower() == 'true'
                request_id = request.args.get('request_id', '')
                
                # Process the request using the shared function
                return process_bbox_request(
                    unit_types=unit_types,
                    bbox=bbox,
                    start_year=start_year,
                    end_year=end_year,
                    exclude_ids=exclude_ids,
                    client_has_data=client_has_data,
                    request_id=request_id
                )
                
            except Exception as e:
                logger.error(f"Error processing GET request: {str(e)}", exc_info=True)
                return jsonify({"error": str(e)}), 400