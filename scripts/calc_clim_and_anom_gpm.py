import numpy as np
import pandas as pd
import xarray as xr
import os
from glob import glob

import sys

from config import *
from util import *

import filter_mode

diri = os.environ.get("DIRI")
file_name = os.environ.get("FILE_NAME")
var_name = os.environ.get("VAR_NAME")
year_start = int(os.environ.get("YEAR_START"))
year_end = int(os.environ.get("YEAR_END"))
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
spd = int(os.environ.get("SPD"))
diro = os.environ.get("DIRO")

if lat_min < 0:
    lat_min_string = f"{np.abs(lat_min):.2f}S"
else:
    lat_min_string = f"{lat_min:.2f}N"

if lat_max < 0:
    lat_max_string = f"{np.abs(lat_max):.2f}S"
else:
    lat_max_string = f"{lat_max:.2f}N"

def _preprocess(ds):
    return ds.sel(lat=slice(lat_min, lat_max))

years = np.arange(year_start, year_end + 1, 1, dtype=int)

files = []

for year in years:
    file_year = sorted(glob(f"{diri}{file_name}.{year}.nc"))
    
    if len(file_year) == 0:
        sys.exit(f"No .nc file is found for {diri}{file_name}.{year}")
    else:
        files += file_year

ds = xr.open_mfdataset(files, preprocess=_preprocess)

x = ds[var_name].compute()

print(x)

if len(x.dims) == 3:
    clim = filter_mode.clim.calcClimTLL(x, spd=spd)
    clim = filter_mode.clim.smthClimTLL(clim, spd=spd, nsmth=4)
    anom = filter_mode.anom.calcAnomTLL(x, clim, spd=spd)
elif len(x.dims) == 4:
    clim = filter_mode.clim.calcClimTLLL(x, spd=spd)
    clim = filter_mode.clim.smthClimTLLL(clim, spd=spd, nsmth=4)
    anom = filter_mode.anom.calcAnomTLLL(x, clim, spd=spd)
else:
    sys.exit("Only 3D or 4D arrays are accepted!")

print(clim)
print(anom)

dso = xr.Dataset()
dso[f"{var_name}_clim"] = clim
dso[f"{var_name}_anom"] = anom

print(dso)

fileo = f"{diro}{file_name}.clim.and.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
