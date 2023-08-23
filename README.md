# Summary

This code will take coordinates and polygons and produce a matrix of distances from each coordinate to the convex hull of the polygon. This method is quick but loses some precision on extremly non-convex polygons in the input shape file.

# Quickstart

Navigate to the working directory and make a virtual environment.

    python virtualenv env

Activate the virtual env

    . env/bin/activate
	
I use fish so,

    . env/bin/activate.fish

Then install the pip dependencies,

    pip install -r requirements.txt
	
Navigate to `./src`

    cd src

Then run `get_distances.py`

    python `get_distances.py

After about 4 min for 600,000 samples with 7 processes, it will produce a parquet in `./data/dist/` that is a matrix of distances from point to convex hull of the polygons in the given shape file. To match it back to original samples, load it into memory then match its rows to points and columns to polygons. The associated points and polygon files will be csv files also in `./data/dist`.

To test on a sample change `sample` to true on the line:

    traffic_stops = get_traffic_stops(sample=False)

To make:

    traffic_stops = get_traffic_stops(sample=True)
	
And the program will run on a sample of 10,000.
