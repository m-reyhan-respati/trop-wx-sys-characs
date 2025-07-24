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

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_mean_precipitation/precipitation.*.nc"))

ds = xr.open_mfdataset(files, preprocess=_preprocess)

precipitation = ds["precipitation"].compute()

precipitation *= 3.0
precipitation = xr.where(precipitation > 0.0, precipitation, np.nan)
precipitation = precipitation.assign_attrs({"units": "mm"})

precipitation_ALL_percentile = precipitation.quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
precipitation_DJF_percentile = precipitation[precipitation["time"].dt.season == "DJF", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
precipitation_MAM_percentile = precipitation[precipitation["time"].dt.season == "MAM", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
precipitation_JJA_percentile = precipitation[precipitation["time"].dt.season == "JJA", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
precipitation_SON_percentile = precipitation[precipitation["time"].dt.season == "SON", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)

precipitation_ALL_percentile = precipitation_ALL_percentile.assign_attrs({"long_name": f"{percentile}th percentile of 3-hourly precipitation accumulation"})
precipitation_DJF_percentile = precipitation_DJF_percentile.assign_attrs({"long_name": f"{percentile}th percentile of 3-hourly precipitation accumulation in DJF"})
precipitation_MAM_percentile = precipitation_MAM_percentile.assign_attrs({"long_name": f"{percentile}th percentile of 3-hourly precipitation accumulation in MAM"})
precipitation_JJA_percentile = precipitation_JJA_percentile.assign_attrs({"long_name": f"{percentile}th percentile of 3-hourly precipitation accumulation in JJA"})
precipitation_SON_percentile = precipitation_SON_percentile.assign_attrs({"long_name": f"{percentile}th percentile of 3-hourly precipitation accumulation in SON"})

print(precipitation_ALL_percentile)
print(precipitation_DJF_percentile)
print(precipitation_MAM_percentile)
print(precipitation_JJA_percentile)
print(precipitation_SON_percentile)

dso = xr.Dataset()
dso[f"precipitation_ALL_{percentile}p"] = precipitation_ALL_percentile
dso[f"precipitation_DJF_{percentile}p"] = precipitation_DJF_percentile
dso[f"precipitation_MAM_{percentile}p"] = precipitation_MAM_percentile
dso[f"precipitation_JJA_{percentile}p"] = precipitation_JJA_percentile
dso[f"precipitation_SON_{percentile}p"] = precipitation_SON_percentile

fileo = f"{diro}precipitation.{percentile}p.{lat_min_string}_{lat_max_string}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
