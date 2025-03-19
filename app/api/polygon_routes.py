# app/api/polygon_routes.py

from flask import jsonify, request
from typing import Optional
import logging

# Import polygon cache and other utilities
from ..utils.polygon_cache import polygon_cache
from ..utils.constants import UNIT_TYPES

logger = logging.getLogger(__name__)

def register_polygon_routes(server):
    """Register API routes for polygon retrieval."""
    
    @server.route('/api/polygons/<string:unit_type>', methods=['GET'])
    def get_polygons(unit_type: str):
        """
        API endpoint to fetch polygons for a specific unit type.
        
        Args:
            unit_type (str): The unit type to fetch polygons for (e.g., 'MOD_REG', 'MOD_DIST')
            
        Query Parameters:
            start_year (int, optional): Start year for time-dependent units
            end_year (int, optional): End year for time-dependent units
            
        Returns:
            JSON: GeoJSON representation of the polygons
        """
        try:
            # Validate unit type
            if unit_type not in UNIT_TYPES:
                return jsonify({"error": f"Invalid unit type: {unit_type}"}), 400
                
            # Get optional year range parameters
            start_year = request.args.get('start_year')
            end_year = request.args.get('end_year')
            
            # Convert to integers if provided
            start_year = int(start_year) if start_year else None
            end_year = int(end_year) if end_year else None
            
            # Get polygons from the cache
            gdf = polygon_cache.get_polygons(unit_type, start_year, end_year)
            
            # Convert to GeoJSON
            if gdf.empty:
                geojson = {"type": "FeatureCollection", "features": []}
            else:
                geojson = gdf.__geo_interface__
                
            logger.info(f"Returned {len(geojson['features'])} polygons for {unit_type}")
            
            return jsonify(geojson)
            
        except Exception as e:
            logger.error(f"Error retrieving polygons for {unit_type}: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500