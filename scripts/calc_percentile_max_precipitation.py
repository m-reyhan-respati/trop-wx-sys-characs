import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *

percentile = int(os.environ.get("PERCENTILE"))
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
diro = os.environ.get("DIRO")

if (percentile < 0) | (percentile > 100):
    sys.exit("Percentile must be between 0 and 100, inclusive")

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

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_max_precipitation/max_precipitation.*.nc"))

ds = xr.open_mfdataset(files, preprocess=_preprocess)

max_precipitation = ds["max_precipitation"].compute()
max_precipitation = xr.where(max_precipitation > 0.0, max_precipitation, np.nan)

max_precipitation_ALL_percentile = max_precipitation.quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
max_precipitation_DJF_percentile = max_precipitation[max_precipitation["time"].dt.season == "DJF", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
max_precipitation_MAM_percentile = max_precipitation[max_precipitation["time"].dt.season == "MAM", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
max_precipitation_JJA_percentile = max_precipitation[max_precipitation["time"].dt.season == "JJA", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
max_precipitation_SON_percentile = max_precipitation[max_precipitation["time"].dt.season == "SON", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)

max_precipitation_ALL_percentile = max_precipitation_ALL_percentile.assign_attrs({"long_name": f"{percentile}th percentile of maximum precipitation rate within 3-hour window"})
max_precipitation_DJF_percentile = max_precipitation_DJF_percentile.assign_attrs({"long_name": f"{percentile}th percentile of maximum precipitation rate within 3-hour window in DJF"})
max_precipitation_MAM_percentile = max_precipitation_MAM_percentile.assign_attrs({"long_name": f"{percentile}th percentile of maximum precipitation rate within 3-hour window in MAM"})
max_precipitation_JJA_percentile = max_precipitation_JJA_percentile.assign_attrs({"long_name": f"{percentile}th percentile of maximum precipitation rate within 3-hour window in JJA"})
max_precipitation_SON_percentile = max_precipitation_SON_percentile.assign_attrs({"long_name": f"{percentile}th percentile of maximum precipitation rate within 3-hour window in SON"})

print(max_precipitation_ALL_percentile)
print(max_precipitation_DJF_percentile)
print(max_precipitation_MAM_percentile)
print(max_precipitation_JJA_percentile)
print(max_precipitation_SON_percentile)

dso = xr.Dataset()
dso[f"max_precipitation_ALL_{percentile}p"] = max_precipitation_ALL_percentile
dso[f"max_precipitation_DJF_{percentile}p"] = max_precipitation_DJF_percentile
dso[f"max_precipitation_MAM_{percentile}p"] = max_precipitation_MAM_percentile
dso[f"max_precipitation_JJA_{percentile}p"] = max_precipitation_JJA_percentile
dso[f"max_precipitation_SON_{percentile}p"] = max_precipitation_SON_percentile

fileo = f"{diro}max_precipitation.{percentile}p.{lat_min_string}_{lat_max_string}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
