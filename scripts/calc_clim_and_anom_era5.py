import numpy as np
import pandas as pd
import xarray as xr
import os
from glob import glob

import sys

from config import *
from util import *

import filter_mode

folder_name = os.environ.get("FOLDER_NAME")
var_name = os.environ.get("VAR_NAME")
level_type = os.environ.get("LEVEL_TYPE")
year_start = int(os.environ.get("YEAR_START"))
year_end = int(os.environ.get("YEAR_END"))
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
spd = int(os.environ.get("SPD"))
diro = os.environ.get("DIRO")

years = np.arange(year_start, year_end + 1, 1, dtype=int)

files = []

for year in years:
    files += sorted(glob(f"{ERA5_RT52_DIR}/{level_type}/reanalysis/{folder_name}/{year:04d}/*.nc"))

if len(files) == 0:
    sys.exit(f"No .nc files are found in {ERA5_RT52_DIR}/{level_type}/reanalysis/{folder_name}/")

if lat_min < 0:
    lat_min_string = f"{np.abs(lat_min):.2f}S"
else:
    lat_min_string = f"{lat_min:.2f}N"

if lat_max < 0:
    lat_max_string = f"{np.abs(lat_max):.2f}S"
else:
    lat_max_string = f"{lat_max:.2f}N"

if level_type == "pressure-levels":
    if "LEVEL" in os.environ:
        level = int(os.environ.get("LEVEL"))
        
        fileo = f"{diro}{folder_name}.{level}.clim.and.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}.nc"
        
        #x0 = open_era5_TLLL_per_level(folder_name, var_name, level, year_start, year_end, spd, lat_min=lat_min, lat_max=lat_max)
        
        def _preprocess(ds):
            return ds.sel(level=slice(level, level), latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
    else:
        fileo = f"{diro}{folder_name}.clim.and.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}.nc"
        
        #x0 = open_era5_TLLL(folder_name, var_name, year_start, year_end, spd, lat_min=lat_min, lat_max=lat_max)
        
        def _preprocess(ds):
            return ds.sel(latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
elif level_type == "single-levels":
    fileo = f"{diro}{folder_name}.clim.and.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}.nc"
    
    #x0 = open_era5_TLL(folder_name, var_name, year_start, year_end, spd, lat_min=lat_min, lat_max=lat_max)
    
    def _preprocess(ds):
        return ds.sel(latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
else:
    sys.exit("LEVEL_TYPE unknown!")

ds = xr.open_mfdataset(files, preprocess=_preprocess)

x0 = ds[var_name][..., ::-1, :].compute()

print(x0)

x = convert_lon_180_to_360(x0, "longitude")

del x0

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

del x

dso = xr.Dataset()
dso[f"{var_name}_clim"] = clim
dso[f"{var_name}_anom"] = anom

print(dso)

dso.to_netcdf(path=fileo, format="NETCDF4")
