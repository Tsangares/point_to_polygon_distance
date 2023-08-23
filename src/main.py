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
pandarallel.initialize(progress_bar=True,nb_workers=8)

RAW_DATA_PATH = os.path.join('../data/raw')
DIST_DATA_PATH = os.path.join('../data/dist')

SHAPES_PATH = os.path.join('../data/raw/townct_37800_0000_2010_s100_census_1_shp/wgs84/townct_37800_0000_2010_s100_census_1_shp_wgs84.shp')

# Read traffic stop data
def read_traffic_stops(all=False):
    if all:
        return pd.read_csv(os.path.join(RAW_DATA_PATH, 'ct_trafficstop_2021-2022.csv'))
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
    return ct_shape

# Show map of CT Shape File
def show_shape(shape):
    shape.plot()
    plt.show()


if __name__=="__main__":
    df = read_traffic_stops(all=True)
    ct_shape = get_ct_shape()
    traffic_stops = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lng, df.lat), crs="EPSG:4326")
    gdf = traffic_stops.sjoin(ct_shape,how="left",predicate="within")
    gdf[gdf['index_right'].notna()]

    #http://wiki.gis.com/wiki/index.php/NAD83#:~:text=NAD83%20is%20an%20acronym%20for,Reference%20System%20ellipsoid%20(GRS80).
    ct_shape.to_csv(os.path.join(DIST_DATA_PATH, 'connecticuit_poly.csv'))
    gdf.to_csv(os.path.join(DIST_DATA_PATH, "traffic_stops_poly.csv"))
    quit()
    #ct_shape.to_crs(epsg=26956,inplace=True)
    #gdf.to_crs(epsg=26956,inplace=True)
    start_time = time.time()
    print('Starting')
    distances = ct_shape.geometry.boundary.parallel_apply(lambda g: gdf.distance(g))
    duration = time.time()-start_time
    print(f"Took {duration:0.02f} secconds to proceen {len(gdf)} samples.")
    start_time = time.time()
    distances.transpose
    duration = time.time()-start_time
    print(f"Took {duration:0.02f} secconds to proceen {len(gdf)} samples.")
    distances.to_csv(os.path.join(DIST_DATA_PATH,'all_distances.csv'))
    #sample = gdf.sample(1)
    #print(get_distance_to_edges(sample,ct_shape))

    # Swap to mercader projection on just Connecticut
    #ax = geometry.plot()
    #point.plot(ax=ax,marker='o',color='red')
    #plt.show()
    pass
    
    