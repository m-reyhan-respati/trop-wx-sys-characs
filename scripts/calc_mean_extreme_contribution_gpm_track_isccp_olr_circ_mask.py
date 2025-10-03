import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

diri = os.environ.get("DIRI")
file_name = os.environ.get("FILE_NAME")
var_name = os.environ.get("VAR_NAME")
percentile = int(os.environ.get("PERCENTILE"))
radius = float(os.environ.get("RADIUS"))
mode = os.environ.get("MODE")
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
diro = os.environ.get("DIRO")

modes = {"Moisture Mode": "moisture_mode", "Mixed System": "mixed_system", "IG Wave": "ig_wave", "Eastward Moisture Mode": "eastward_moisture_mode", "Eastward Mixed System": "eastward_mixed_system", "Eastward IG Wave": "eastward_ig_wave", "Westward Moisture Mode": "westward_moisture_mode", "Westward Mixed System": "westward_mixed_system", "Westward IG Wave": "westward_ig_wave", "Tropical Cyclone": "tc"}

mode_dict = {"Moisture Mode": "moisture.mode", "Mixed System": "mixed.system", "IG Wave": "ig.wave", "Eastward Moisture Mode": "eastward.moisture.mode", "Eastward Mixed System": "eastward.mixed.system", "Eastward IG Wave": "eastward.ig.wave", "Westward Moisture Mode": "westward.moisture.mode", "Westward Mixed System": "westward.mixed.system", "Westward IG Wave": "westward.ig.wave", "Tropical Cyclone": "tc"}

if lat_min < 0:
    lat_min_string = f"{np.abs(lat_min + 0.05):.2f}S"
else:
    lat_min_string = f"{lat_min + 0.05:.2f}N"

if lat_max < 0:
    lat_max_string = f"{np.abs(lat_max - 0.05):.2f}S"
else:
    lat_max_string = f"{lat_max - 0.05:.2f}N"

fileo = f"{diro}mean.extreme.contribution.{percentile:d}p.{file_name}.{mode_dict[mode]}.circ.mask.{radius:.1f}km.{lat_min_string}-{lat_max_string}.nc"

def _preprocess(ds):
    return ds.sel(lat=slice(lat_min + 0.01, lat_max - 0.01))

files = sorted(glob(f"{diri}extreme.contribution.{percentile:d}p.{file_name}.*.circ.mask.{radius:.1f}km.nc"))

ds = xr.open_mfdataset(files, preprocess=_preprocess)

x_all = ds[f"all_extreme_contribution_{var_name}_{modes[mode]}"].mean(dim="time").compute()

dso = xr.Dataset()
dso[f"mean_extreme_contribution_{var_name}_{modes[mode]}_ALL"] = x_all

seasons = ["DJF", "MAM", "JJA", "SON"]

x = ds[f"season_extreme_contribution_{var_name}_{modes[mode]}"]

for season in seasons:
    x_season = x[x["time"].dt.season.values == season, :, :].mean(dim="time").compute()
    
    dso[f"mean_extreme_contribution_{var_name}_{modes[mode]}_{season}"] = x_season

print(dso)

dso.to_netcdf(path=fileo, format="NETCDF4")