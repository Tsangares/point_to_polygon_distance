import os
import logging, math, random, os, time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats
import geopandas as gpd
from database import *
from dotenv import load_dotenv
load_dotenv()

RAW_DATA_PATH = os.path.join('../data/raw')
DIST_DATA_PATH = os.path.join('../data/dist')

SHAPES_PATH = os.path.join('../data/raw/townct_37800_0000_2010_s100_census_1_shp/wgs84/townct_37800_0000_2010_s100_census_1_shp_wgs84.shp')

# Read traffic stop data
def read_traffic_stops():
    temp_file = os.path.join(RAW_DATA_PATH, 'traffic_stops_10_000.csv')
    if not os.path.isfile(temp_file):
        df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'ct_trafficstop_2021-2022.csv'))
        df.sample(10_000).to_csv(temp_file, index=False)
    else:
        df = pd.read_csv(temp_file)
    return df

# Get Connecticut Shape
def get_ct_shape():
    ct_shape = gpd.read_file(SHAPES_PATH)
    ct_shape = ct_shape.to_crs(epsg=4326)
    return ct_shape

# Convert to geojson that can be loaded to mongodb
def to_geojson(gdf,path):
    gdf.to_file(path, driver="GeoJSON") 
    #Then load it into mongodb...
    #TODO: Write function
    pass 


# Show map of CT Shape File
def show_shape(shape):
    shape.plot()
    plt.show()


if __name__=="__main__":
    df = read_traffic_stops()
    client = get_mongo_client()
    db = client.connecticut
    traffic_stops = db.traffic_stops
    cities = db.city_polygons
    traffic_stop = traffic_stops.find_one()
    print(traffic_stop)
    intersection = lambda traffic_stop: {
        'geometry': {
            '$geoIntersects': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': traffic_stop['coord']
                }
            }
        }
    }
    city = cities.find_one(intersection(traffic_stop))
    print(city['properties']['NAME10'])