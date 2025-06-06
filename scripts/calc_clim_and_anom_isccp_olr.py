import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *

import filter_mode

year_start = int(os.environ.get("YEAR_START"))
year_end = int(os.environ.get("YEAR_END"))
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
diro = os.environ.get("DIRO")

var_name = "txuptp"
spd = 8

years = np.arange(year_start, year_end + 1, 1, dtype=int)

files = []

for year in years:
    file_year = sorted(glob(f"{ISCCP_OLR_DIR}/{var_name}.{year:04d}.nc"))
    
    if len(file_year) == 0:
        sys.exit(f"No .nc file exists for year {year:04d}")
    
    files += file_year

if lat_min < 0:
    lat_min_string = f"{np.abs(lat_min):.2f}S"
else:
    lat_min_string = f"{lat_min:.2f}N"

if lat_max < 0:
    lat_max_string = f"{np.abs(lat_max):.2f}S"
else:
    lat_max_string = f"{lat_max:.2f}N"

fileo = f"{diro}{var_name}.clim.and.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}.nc"

def _preprocess(ds):
    return ds.sel(latitude=slice(lat_min, lat_max))

ds = xr.open_mfdataset(files, preprocess=_preprocess)

x = ds[var_name].compute()

print(x)

clim = filter_mode.clim.calcClimTLL(x, spd=spd)
clim = filter_mode.clim.smthClimTLL(clim, spd=spd, nsmth=4)
anom = filter_mode.anom.calcAnomTLL(x, clim, spd=spd)

print(clim)
print(anom)

dso = xr.Dataset()
dso[f"{var_name}_clim"] = clim
dso[f"{var_name}_anom"] = anom

print(dso)

dso.to_netcdf(path=fileo, format="NETCDF4")
