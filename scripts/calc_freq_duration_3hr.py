import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *

lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
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

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_duration/duration.*.nc"))

ds = xr.open_mfdataset(files, preprocess=_preprocess)

duration = ds["duration"].compute()

duration = xr.where(duration > 0.0, duration, np.nan)
duration_3hr = xr.where(duration == 3.0, duration, np.nan)

freq_duration_ALL_3hr = duration_3hr.count(dim="time") / duration.count(dim="time")
freq_duration_DJF_3hr = duration_3hr[duration_3hr["time"].dt.season == "DJF", ...].count(dim="time") / duration[duration["time"].dt.season == "DJF", ...].count(dim="time")
freq_duration_MAM_3hr = duration_3hr[duration_3hr["time"].dt.season == "MAM", ...].count(dim="time") / duration[duration["time"].dt.season == "MAM", ...].count(dim="time")
freq_duration_JJA_3hr = duration_3hr[duration_3hr["time"].dt.season == "JJA", ...].count(dim="time") / duration[duration["time"].dt.season == "JJA", ...].count(dim="time")
freq_duration_SON_3hr = duration_3hr[duration_3hr["time"].dt.season == "SON", ...].count(dim="time") / duration[duration["time"].dt.season == "SON", ...].count(dim="time")

freq_duration_ALL_3hr = freq_duration_ALL_3hr.assign_attrs({"long_name": "Frequency of 3-hour long rainfall events within 3-hour window"})
freq_duration_DJF_3hr = freq_duration_DJF_3hr.assign_attrs({"long_name": "Frequency of 3-hour long rainfall events within 3-hour window in DJF"})
freq_duration_MAM_3hr = freq_duration_MAM_3hr.assign_attrs({"long_name": "Frequency of 3-hour long rainfall events within 3-hour window in MAM"})
freq_duration_JJA_3hr = freq_duration_JJA_3hr.assign_attrs({"long_name": "Frequency of 3-hour long rainfall events within 3-hour window in JJA"})
freq_duration_SON_3hr = freq_duration_SON_3hr.assign_attrs({"long_name": "Frequency of 3-hour long rainfall events within 3-hour window in SON"})

print(freq_duration_ALL_3hr)
print(freq_duration_DJF_3hr)
print(freq_duration_MAM_3hr)
print(freq_duration_JJA_3hr)
print(freq_duration_SON_3hr)

dso = xr.Dataset()
dso["freq_duration_ALL_3hr"] = freq_duration_ALL_3hr
dso["freq_duration_DJF_3hr"] = freq_duration_DJF_3hr
dso["freq_duration_MAM_3hr"] = freq_duration_MAM_3hr
dso["freq_duration_JJA_3hr"] = freq_duration_JJA_3hr
dso["freq_duration_SON_3hr"] = freq_duration_SON_3hr

fileo = f"{diro}freq.duration.3hr.{lat_min_string}_{lat_max_string}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
