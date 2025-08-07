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

track_files = sorted(glob(f"{diri}track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.csv"))

if len(track_files) == 0:
    sys.exit(f"No .csv file for track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}")

track = pd.read_csv(track_files[0])

print(track)

dso = xr.Dataset()

seasons = ["ALL", "DJF", "MAM", "JJA", "SON"]

for season in seasons:
    if season == "ALL":
        track_in_season = track
    elif season == "DJF":
        track_in_season = track.loc[(pd.to_datetime(track["timestr"]).dt.month == 12) | (pd.to_datetime(track["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        track_in_season = track.loc[(pd.to_datetime(track["timestr"]).dt.month >= 3) & (pd.to_datetime(track["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        track_in_season = track.loc[(pd.to_datetime(track["timestr"]).dt.month >= 6) & (pd.to_datetime(track["timestr"]).dt.month <= 8)]
    elif season == "SON":
        track_in_season = track.loc[(pd.to_datetime(track["timestr"]).dt.month >= 9) & (pd.to_datetime(track["timestr"]).dt.month <= 11)]
    
    count_track = track_cell.track.quality_control(track_in_season, spd=8)
    count_track = track_cell.track.count_track(track_in_season)
    
    count_track = count_track.assign_attrs({"long_name": f"Number of detected centres for {mode} OLR objects ({season})"})
    
    print(count_track)
    
    print(f"Total number of detected centres ({season}): {np.sum(count_track.values)}")
    
    dso[f"count_track_{mode_var_name}_{season}"] = count_track

fileo = f"{diro}count.track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
