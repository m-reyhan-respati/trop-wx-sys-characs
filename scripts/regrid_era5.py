import numpy as np
import pandas as pd
import xarray as xr
import os
from glob import glob

import sys

from config import *
from util import *

folder_name = os.environ.get("FOLDER_NAME")
var_name = os.environ.get("VAR_NAME")
level_type = os.environ.get("LEVEL_TYPE")
year = int(os.environ.get("YEAR"))
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
lon_min = float(os.environ.get("LON_MIN"))
lon_max = float(os.environ.get("LON_MAX"))
spd = int(os.environ.get("SPD"))
res_new = float(os.environ.get("RES_NEW"))
diro = os.environ.get("DIRO")

if level_type == "pressure-levels":
    if "LEVEL" in os.environ:
        level = int(os.environ.get("LEVEL"))

        fileo = f"{diro}{folder_name}.{level}.{year:04d}.nc"

        x = regrid_era5_TLLL_per_level_per_year(folder_name, var_name, level, year, spd, res_new, lat_min, lat_max, lon_min, lon_max, periodic=True)
    else:
        fileo = f"{diro}{folder_name}.{year:04d}.nc"

        x = regrid_era5_TLLL_per_year(folder_name, var_name, year, spd, res_new, lat_min, lat_max, lon_min, lon_max, periodic=True)
elif level_type == "single-levels":
    fileo = f"{diro}{folder_name}.{year:04d}.nc"

    x = regrid_era5_TLL_per_year(folder_name, var_name, year, spd, res_new, lat_min, lat_max, lon_min, lon_max, periodic=True)
else:
    sys.exit("LEVEL_TYPE unknown!")

print(x)

dso = xr.Dataset()
dso[var_name] = x

dso.to_netcdf(path=fileo, format="NETCDF4")
