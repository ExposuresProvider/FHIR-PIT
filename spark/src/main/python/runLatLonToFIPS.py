import sys
from utils import run

cache_dir = sys.argv[1]
lat = sys.argv[2]
lon = sys.argv[3]
year = sys.argv[4]

run("datatrans.LatLon2FIPS",
       lat,
       lon,
       year)


