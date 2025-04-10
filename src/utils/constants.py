# app/utils/constants.py
UNIT_TYPES = {
              'CONSTITUENCY': {'long_name': 'Constituency', 'color': 'green', 'timeless': False, 'cache_disk': False},
              'LG_DIST': {'long_name': 'Local Government District', 'color': 'orange', 'timeless': False, 'cache_disk': False},
              'MOD_CNTY': {'long_name': 'Modern County', 'color': 'purple', 'timeless': True, 'cache_disk': True},
              'MOD_DIST': {'long_name': 'Modern District', 'color': 'brown', 'timeless': True, 'cache_disk': True},
              'MOD_REG': {'long_name': 'Modern Region', 'color': 'blue', 'timeless': True, 'cache_disk': True},
              'MOD_WARD': {'long_name': 'Modern Ward', 'color': 'darkgreen', 'timeless': False, 'cache_disk': False},
              }

UNIT_THEMES = {"T_LAND": "Agriculture & Land Use", 
               "T_HOUS": "Housing", 
               "T_IND": "Industry", 
               "T_LEARN": "Learning & Language", 
               "T_VITAL": "Life & Death", 
               "T_POL": "Political Life", 
               "T_POP": "Population", 
               "T_REL": "Roots & Religion", 
               "T_SOC": "Social Structure", 
               "T_WK": "Work & Poverty"}
