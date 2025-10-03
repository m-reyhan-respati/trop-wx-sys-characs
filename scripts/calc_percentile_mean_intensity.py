import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *

percentile = int(os.environ.get("PERCENTILE"))
raining = int(os.environ.get("RAINING"))
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
diro = os.environ.get("DIRO")

if (percentile < 0) | (percentile > 100):
    sys.exit("Percentile must be between 0 and 100, inclusive")

if lat_min < 0:
    lat_min_string = f"{np.abs(lat_min + 0.05):.2f}S"
else:
    lat_min_string = f"{lat_min + 0.05:.2f}N"

if lat_max < 0:
    lat_max_string = f"{np.abs(lat_max - 0.05):.2f}S"
else:
    lat_max_string = f"{lat_max - 0.05:.2f}N"

def _preprocess(ds):
    return ds.sel(lat=slice(lat_min, lat_max))

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_mean_intensity/mean_intensity.*.nc"))

ds = xr.open_mfdataset(files, preprocess=_preprocess)

precipitation = ds["mean_intensity"].compute()

if raining == 1:
    precipitation = xr.where(precipitation > 0.0, precipitation, np.nan, keep_attrs=True)

precipitation_ALL_percentile = precipitation.quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
precipitation_DJF_percentile = precipitation[precipitation["time"].dt.season == "DJF", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
precipitation_MAM_percentile = precipitation[precipitation["time"].dt.season == "MAM", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
precipitation_JJA_percentile = precipitation[precipitation["time"].dt.season == "JJA", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
precipitation_SON_percentile = precipitation[precipitation["time"].dt.season == "SON", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)

precipitation_ALL_percentile = precipitation_ALL_percentile.assign_attrs({"long_name": f"{percentile}th percentile of mean intensity of precipitation within 3-hour time window"})
precipitation_DJF_percentile = precipitation_DJF_percentile.assign_attrs({"long_name": f"{percentile}th percentile of mean intensity of precipitation within 3-hour time window in DJF"})
precipitation_MAM_percentile = precipitation_MAM_percentile.assign_attrs({"long_name": f"{percentile}th percentile of mean intensity of precipitation within 3-hour time window in MAM"})
precipitation_JJA_percentile = precipitation_JJA_percentile.assign_attrs({"long_name": f"{percentile}th percentile of mean intensity of precipitation within 3-hour time window in JJA"})
precipitation_SON_percentile = precipitation_SON_percentile.assign_attrs({"long_name": f"{percentile}th percentile of mean intensity of precipitation within 3-hour time window in SON"})

print(precipitation_ALL_percentile)
print(precipitation_DJF_percentile)
print(precipitation_MAM_percentile)
print(precipitation_JJA_percentile)
print(precipitation_SON_percentile)

dso = xr.Dataset()
dso[f"mean_intensity_ALL_{percentile}p"] = precipitation_ALL_percentile
dso[f"mean_intensity_DJF_{percentile}p"] = precipitation_DJF_percentile
dso[f"mean_intensity_MAM_{percentile}p"] = precipitation_MAM_percentile
dso[f"mean_intensity_JJA_{percentile}p"] = precipitation_JJA_percentile
dso[f"mean_intensity_SON_{percentile}p"] = precipitation_SON_percentile

if raining == 1:
    fileo = f"{diro}raining/mean_intensity.{percentile}p.{lat_min_string}_{lat_max_string}.nc"
else:
    fileo = f"{diro}mean_intensity.{percentile}p.{lat_min_string}_{lat_max_string}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
