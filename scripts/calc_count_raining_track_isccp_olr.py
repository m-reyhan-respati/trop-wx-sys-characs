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

track_files = sorted(glob(f"{diri}object.mean.precipitation.{mode_file_name}.*.csv"))

track = pd.DataFrame()

for i in range(0, len(track_files)):
    track = pd.concat([track, pd.read_csv(track_files[i])])

del track["Unnamed: 0.1"], track["Unnamed: 0"]

track.index = np.arange(0, len(track.index), 1, dtype=int)

print(track)

track_rn = track.loc[track["precipitation"].values >= 0.05]
track_nr = track.loc[track["precipitation"].values < 0.05]

track_rn.index = np.arange(0, len(track_rn.index), 1, dtype=int)
track_nr.index = np.arange(0, len(track_nr.index), 1, dtype=int)

print(track_rn)
print(track_nr)

dso_rn = xr.Dataset()
dso_nr = xr.Dataset()

seasons = ["ALL", "DJF", "MAM", "JJA", "SON"]

for season in seasons:
    if season == "ALL":
        track_rn_in_season = track_rn
        track_nr_in_season = track_nr
    elif season == "DJF":
        track_rn_in_season = track_rn.loc[(pd.to_datetime(track_rn["timestr"]).dt.month == 12) | (pd.to_datetime(track_rn["timestr"]).dt.month <= 2)]
        track_nr_in_season = track_nr.loc[(pd.to_datetime(track_nr["timestr"]).dt.month == 12) | (pd.to_datetime(track_nr["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        track_rn_in_season = track_rn.loc[(pd.to_datetime(track_rn["timestr"]).dt.month >= 3) & (pd.to_datetime(track_rn["timestr"]).dt.month <= 5)]
        track_nr_in_season = track_nr.loc[(pd.to_datetime(track_nr["timestr"]).dt.month >= 3) & (pd.to_datetime(track_nr["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        track_rn_in_season = track_rn.loc[(pd.to_datetime(track_rn["timestr"]).dt.month >= 6) & (pd.to_datetime(track_rn["timestr"]).dt.month <= 8)]
        track_nr_in_season = track_nr.loc[(pd.to_datetime(track_nr["timestr"]).dt.month >= 6) & (pd.to_datetime(track_nr["timestr"]).dt.month <= 8)]
    elif season == "SON":
        track_rn_in_season = track_rn.loc[(pd.to_datetime(track_rn["timestr"]).dt.month >= 9) & (pd.to_datetime(track_rn["timestr"]).dt.month <= 11)]
        track_nr_in_season = track_nr.loc[(pd.to_datetime(track_nr["timestr"]).dt.month >= 9) & (pd.to_datetime(track_nr["timestr"]).dt.month <= 11)]
    
    count_track_rn = track_cell.track.count_track(track_rn_in_season)
    count_track_nr = track_cell.track.count_track(track_nr_in_season)
    
    count_track_rn = count_track_rn.assign_attrs({"long_name": f"Number of detected centres for raining {mode} OLR objects ({season})"})
    count_track_nr = count_track_nr.assign_attrs({"long_name": f"Number of detected centres for non-raining {mode} OLR objects ({season})"})
    
    print(count_track_rn)
    print(count_track_nr)
    
    print(f"Total number of detected centres of raining objects ({season})    : {np.sum(count_track_rn.values)}")
    print(f"Total number of detected centres of non-raining objects ({season}): {np.sum(count_track_nr.values)}")
    
    dso_rn[f"count_track_{mode_var_name}_{season}"] = count_track_rn
    dso_nr[f"count_track_{mode_var_name}_{season}"] = count_track_nr

fileo_rn = f"{diro}count.raining.track.txuptp.{mode_file_name}.nc"
fileo_nr = f"{diro}count.non.raining.track.txuptp.{mode_file_name}.nc"

dso_rn.to_netcdf(path=fileo_rn, format="NETCDF4")
dso_nr.to_netcdf(path=fileo_nr, format="NETCDF4")
