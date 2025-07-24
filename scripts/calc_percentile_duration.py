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

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_duration/duration.*.nc"))

ds = xr.open_mfdataset(files, preprocess=_preprocess)

duration = ds["duration"].compute()
duration = xr.where(duration > 0.0, duration, np.nan)

duration_ALL_percentile = duration.quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
duration_DJF_percentile = duration[duration["time"].dt.season == "DJF", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
duration_MAM_percentile = duration[duration["time"].dt.season == "MAM", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
duration_JJA_percentile = duration[duration["time"].dt.season == "JJA", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)
duration_SON_percentile = duration[duration["time"].dt.season == "SON", ...].quantile(percentile / 100, dim="time", keep_attrs=True, skipna=True)

duration_ALL_percentile = duration_ALL_percentile.assign_attrs({"long_name": f"{percentile}th percentile of precipitation duration within 3-hour window"})
duration_DJF_percentile = duration_DJF_percentile.assign_attrs({"long_name": f"{percentile}th percentile of precipitation duration within 3-hour window in DJF"})
duration_MAM_percentile = duration_MAM_percentile.assign_attrs({"long_name": f"{percentile}th percentile of precipitation duration within 3-hour window in MAM"})
duration_JJA_percentile = duration_JJA_percentile.assign_attrs({"long_name": f"{percentile}th percentile of precipitation duration within 3-hour window in JJA"})
duration_SON_percentile = duration_SON_percentile.assign_attrs({"long_name": f"{percentile}th percentile of precipitation duration within 3-hour window in SON"})

print(duration_ALL_percentile)
print(duration_DJF_percentile)
print(duration_MAM_percentile)
print(duration_JJA_percentile)
print(duration_SON_percentile)

dso = xr.Dataset()
dso[f"duration_ALL_{percentile}p"] = duration_ALL_percentile
dso[f"duration_DJF_{percentile}p"] = duration_DJF_percentile
dso[f"duration_MAM_{percentile}p"] = duration_MAM_percentile
dso[f"duration_JJA_{percentile}p"] = duration_JJA_percentile
dso[f"duration_SON_{percentile}p"] = duration_SON_percentile

fileo = f"{diro}duration.{percentile}p.{lat_min_string}_{lat_max_string}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")