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
year_start = int(os.environ.get("YEAR_START"))
year_end = int(os.environ.get("YEAR_END"))
mode = os.environ.get("MODE")
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
elif mode == "Raw Anomaly":
    mode_var_name = "anom"
    mode_file_name = "clim.and.anom"
else:
    sys.exit("Mode unknown!")

if mode == "Raw Anomaly":
    mode_file_name = "anom"

max_activity_files = sorted(glob(f"{diri}max.activity.track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.csv"))

if len(max_activity_files) == 0:
    sys.exit(f"No .csv file for max.activity.track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}")

max_activity = pd.read_csv(max_activity_files[0])

print(max_activity)

dso = xr.Dataset()

seasons = ["ALL", "DJF", "MAM", "JJA", "SON"]

for season in seasons:
    if season == "ALL":
        max_activity_in_season = max_activity
    elif season == "DJF":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month == 12) | (pd.to_datetime(max_activity["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 3) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 6) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 8)]
    elif season == "SON":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 9) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 11)]
    
    count_max_activity = track_cell.track.count_track(max_activity_in_season)
    
    count_max_activity = count_max_activity.assign_attrs({"long_name": f"Number of detected centres for {mode} OLR objects at their maximum activity ({season})"})
    
    print(count_max_activity)
    
    print(f"Total number of detected centres ({season}): {np.sum(count_max_activity.values)}")
    
    dso[f"count_max_activity_track_{mode_var_name}_{season}"] = count_max_activity

fileo = f"{diro}count.max.activity.track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
