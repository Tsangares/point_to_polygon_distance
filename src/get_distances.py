import os
from pandarallel import pandarallel
import logging, math, random, os, time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats
import geopandas as gpd
from dotenv import load_dotenv
load_dotenv()
pandarallel.initialize(progress_bar=True,nb_workers=6)
logging.basicConfig(level=logging.INFO)

RAW_DATA_PATH = os.path.join('../data/raw')
DIST_DATA_PATH = os.path.join('../data/dist')

SHAPES_PATH = os.path.join('../data/raw/townct_37800_0000_2010_s100_census_1_shp/wgs84/townct_37800_0000_2010_s100_census_1_shp_wgs84.shp')

# Read traffic stop data
# Option for getting a sample for testing
def get_traffic_stops(sample=True):
    # Column (2) ProfileNo is a mix of float and int, ignore warning.
    temp_file = os.path.join(RAW_DATA_PATH, 'traffic_stops_10_000.csv')
    
    if sample and not os.path.isfile(temp_file):
        df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'ct_trafficstop_2021-2022.csv'))
        df.sample(10_000).to_csv(temp_file, index=False)
    elif sample:
        df = pd.read_csv(temp_file)
    else:
        df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'ct_trafficstop_2021-2022.csv'))
    
    # Convert the pandas dataframe to Geopandas DataFrame with lng lat at the point geometry
    # crs=EPSG:4326 referts to being on a ellipsoidal manifold
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lng, df.lat), crs="EPSG:4326")

# Get Connecticut Shape
def get_ct_shape():
    return gpd.read_file(SHAPES_PATH)

# Show map of CT Shape File
def show_shape(shape):
    shape.plot()
    plt.show()

# Spacial Join Using GeoPandas
# Return dataframe with traffic stop's containing polygon 
def spacial_match(traffic_stops,shape):
    # If Traffic Stop is within shape poly, data is attributed.
    total = len(traffic_stops)
    gdf = traffic_stops.sjoin(ct_shape,how="left",predicate="within")
    gdf = gdf[gdf['index_right'].notna()]
    #gdf.dropna(axis=0,subset=['index_right',])
    logging.info(f"{total-len(gdf)} Observations dropped for being outside shape file.")
    return gdf

#http://wiki.gis.com/wiki/index.php/NAD83#:~:text=NAD83%20is%20an%20acronym%20for,Reference%20System%20ellipsoid%20(GRS80).
if __name__=="__main__":
    # Takes in traffic stops and shape file
    # Will write an output matrix that matches indicies
    #  between traffic stop and poly that describes distance.
    traffic_stops = get_traffic_stops(sample=False)
    ct_shape = get_ct_shape()

    # Match Traffic Stops Within Shapefile
    traffic_stops = spacial_match(traffic_stops, ct_shape)

    # Write 
    logging.info(f"Writing poly and traffic stops to disk. Indicies are used to match distances.")
    ct_shape.to_csv(os.path.join(DIST_DATA_PATH, 'connecticuit_poly.csv'))
    traffic_stops.to_csv(os.path.join(DIST_DATA_PATH, "traffic_stops_poly.csv"))
    
    # Project from Spherical to flat manifold
    # This is so we can do linear distance instead of spherical geodesics
    ct_shape.to_crs(epsg=26956,inplace=True)
    traffic_stops.to_crs(epsg=26956,inplace=True)

    # Compute Distance Matrix in parallel
    start_time = time.time()
    logging.info('Starting to find the distance of every point to the convex hull of every poly.')
    distances = ct_shape.geometry.boundary.parallel_apply(lambda g: traffic_stops.distance(g))
    duration = time.time()-start_time
    logging.info(f"Took {duration:0.02f} secconds to proceen {len(traffic_stops)} samples.")

    # Transpose to make rows traffic stops and columns polygons 
    distances.transpose

    # Write to disk
    distances.to_parquet(os.path.join(DIST_DATA_PATH,'all_distances.parquet'))
    
    