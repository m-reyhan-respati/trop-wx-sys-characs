import numpy as np
import pandas as pd
import xarray as xr
import tobac
from glob import glob

import os
import sys

from config import *

import track_cell

diri = os.environ.get("DIRI")
file_name = os.environ.get("FILE_NAME")
var_name = os.environ.get("VAR_NAME")
mode = os.environ.get("MODE")
lag = float(os.environ.get("LAG"))
diro = os.environ.get("DIRO")

if mode == "Moisture Mode":
    mode_var_name = "moisture_mode"
    mode_file_name = "moisture.mode"
elif mode == "Mixed System":
    mode_var_name = "mixed_system"
    mode_file_name = "mixed.system"
elif mode == "IG Wave":
    mode_var_name = "ig_wave"
    mode_file_name = "ig.wave"
elif mode == "Eastward Moisture Mode":
    mode_var_name = "eastward_moisture_mode"
    mode_file_name = "eastward.moisture.mode"
elif mode == "Eastward Mixed System":
    mode_var_name = "eastward_mixed_system"
    mode_file_name = "eastward.mixed.system"
elif mode == "Eastward IG Wave":
    mode_var_name = "eastward_ig_wave"
    mode_file_name = "eastward.ig.wave"
elif mode == "Westward Moisture Mode":
    mode_var_name = "westward_moisture_mode"
    mode_file_name = "westward.moisture.mode"
elif mode == "Westward Mixed System":
    mode_var_name = "westward_mixed_system"
    mode_file_name = "westward.mixed.system"
elif mode == "Westward IG Wave":
    mode_var_name = "westward_ig_wave"
    mode_file_name = "westward.ig.wave"
elif mode == "Tropical Cyclone":
    mode_var_name = "anom_tc"
    mode_file_name = "tc"
else:
    sys.exit("Mode unknown!")

files = sorted(glob(f"{diri}stacks/stack.{file_name}.{mode_file_name}.*.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for {diri}stacks/stack.{file_name}.{mode_file_name}")

fileo = f"{diro}composite.{file_name}.{mode_file_name}.lag.{lag:.0f}.nc"

track_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/track.txuptp.{mode_file_name}.*.csv"))

if len(track_files) == 0:
    sys.exit(f"No .csv file for track.txuptp.{mode_file_name}")

track = pd.read_csv(track_files[0])

track_before = track.loc[pd.to_datetime(track["timestr"].values) < pd.to_datetime("2000-06-01 00:00:00")]

max_activity_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.{mode_file_name}.*.csv"))

if len(max_activity_files) == 0:
    sys.exit(f"No .csv file for max.activity.track.txuptp.{mode_file_name}")

max_activity = pd.read_csv(max_activity_files[0])

max_activity = max_activity.loc[~np.isin(max_activity["cell"].values, track_before["cell"].values)]

track = track.loc[np.isin(track["cell"].values, max_activity["cell"].values)]

del track["Unnamed: 0"], max_activity["Unnamed: 0"], max_activity["Unnamed: 0.1"]

print(track)
print(max_activity)

for i in range(0, len(files)):
    x_i = xr.open_dataset(files[i])[f"stack_{var_name}_{mode_var_name}"].sel(lag=slice(lag, lag))
    
    if i == 0:
        x = x_i.copy()
    else:
        x = xr.concat([x, x_i], dim="n")

print(x)

seasons = ["ALL", "DJF", "MAM", "JJA", "SON"]
domains = ["EQ", "NH", "SH"]

dso = xr.Dataset()

for season in seasons:
    for domain in domains:
        composite, pvalue = track_cell.stats.compositeTLL_gpm(x, max_activity, season, domain)
        
        print(composite)
        
        dso[f"composite_{var_name}_{mode_var_name}_{season}_{domain}"] = composite
        dso[f"pvalue_{var_name}_{mode_var_name}_{season}_{domain}"] = pvalue

dso.to_netcdf(path=fileo, format="NETCDF4")
