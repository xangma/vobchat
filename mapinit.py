
from config import load_config, get_db
import os
import pandas as pd
import geopandas as gpd
import json
import shapely
from pyproj import CRS

config = load_config()
db = get_db(config)

# Query to get the polygons
poly_query = """
select 
g_unit, 
g_foot_ertslcc,
auo_util.get_unit_name(g_unit), 
util.get_start_year(g_duration), 
util.get_end_year(g_duration)
from hgis.g_foot 
where g_unit_type='MOD_DIST'
and use_for_stat_map='Y';
"""

gdf_filename = 'polygons_gdf.geojson'

def get_mapinit_polygons():
    if not os.path.exists(gdf_filename):
        res = db.run(poly_query, fetch="cursor")
        res = list(res.mappings())
        df = pd.DataFrame(res)

        # convert column from wkb to geometry
        df['g_foot_ertslcc'] = df['g_foot_ertslcc'].apply(
            lambda x: shapely.from_wkb(x))
        # df.drop(columns=['g_foot'], inplace=True)
        
        gdf = gpd.GeoDataFrame(df, geometry='g_foot_ertslcc')
        # gdf = gdf.explode("g_foot_ertslcc", index_parts=True)
        # gdf.reset_index(inplace=True, drop=True)

        gdf.set_index('g_unit', inplace=True)
        gdf['id'] = gdf.index
        
        # Save gdf to file
        gdf.to_file(gdf_filename, driver='GeoJSON')
        gdf = gdf.set_crs(CRS.from_epsg(3034), allow_override=True)
        # reproject to WGS84
        gdf = gdf.to_crs(epsg=4326)
        
        # Convert GeoDataFrame to GeoJSON
        geojson = json.loads(gdf.to_json())
    else:
        # Load GeoJSON from file
        gdf = gpd.read_file(gdf_filename)
        
        gdf = gdf.set_crs(CRS.from_epsg(3034), allow_override=True)
        # reproject to WGS84
        gdf = gdf.to_crs(epsg=4326)
        # Convert GeoDataFrame to GeoJSON
        geojson = json.loads(gdf.to_json())
    return gdf, geojson