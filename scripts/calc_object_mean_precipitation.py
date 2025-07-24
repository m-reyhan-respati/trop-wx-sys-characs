import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

year = int(os.environ.get("YEAR"))
month = int(os.environ.get("MONTH"))
mode = os.environ.get("MODE")
diro = os.environ.get("DIRO")

def calc_object_mean_precipitation(track, max_activity, mask, precipitation):
    track_new = pd.DataFrame()
    
    for cell in max_activity["cell"].values:
        track_n = track.loc[track["cell"].values == cell]
        
        precipitation_n = []
        
        for i in range(0, len(track_n["feature"].values)):
            precipitation_i = xr.where(mask.values == track_n["feature"].values[i], precipitation, np.nan)
            
            precipitation_n += [precipitation_i.mean().values]
        
        track_n.insert(len(track_n.columns), "precipitation", np.asarray(precipitation_n))
        
        track_new = pd.concat([track_new, track_n])
    
    return track_new

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

track = pd.read_csv(f"{SCRATCH_TRACK_DIR}/track.txuptp.{mode_file_name}.1984-2016.csv")
max_activity = pd.read_csv(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.{mode_file_name}.1984-2016.csv")

max_activity = max_activity.loc[pd.to_datetime(max_activity["timestr"].values).year * 100 + pd.to_datetime(max_activity["timestr"].values).month == year * 100 + month]

track = track.loc[np.isin(track["cell"].values, max_activity["cell"].values)]

if (year == 2000) & (month == 6):
    track_before = track.loc[pd.to_datetime(track["timestr"].values) < pd.to_datetime("2000-06-01 00:00:00")]
    max_activity = max_activity.loc[~np.isin(max_activity["cell"].values, track_before["cell"].values)]
    track = track.loc[np.isin(track["cell"].values, max_activity["cell"].values)]
elif (year == 2016) & (month == 12):
    track_after = track.loc[pd.to_datetime(track["timestr"].values) > pd.to_datetime("2016-12-31 21:00:00")]
    max_activity = max_activity.loc[~np.isin(max_activity["cell"].values, track_after["cell"].values)]
    track = track.loc[np.isin(track["cell"].values, max_activity["cell"].values)]

time_start = np.datetime_as_string(np.min(pd.to_datetime(track["timestr"].values).values), unit="s")
time_end = np.datetime_as_string(np.max(pd.to_datetime(track["timestr"].values).values), unit="s")

print(f"Time period: {time_start}-{time_end}")
print(track)

files = sorted(glob("/scratch/k10/mr4682/data/GPM/3hr_mean_precipitation/1p0deg/precipitation.*.nc"))

precipitation = xr.open_mfdataset(files)["precipitation"].sel(time=slice(time_start, time_end)).compute()
precipitation = precipitation.transpose("time", "lat", "lon")

precipitation[:, 1:-1, :] /= 300.0
precipitation[:, 0, :] /= 150.0
precipitation[:, -1, :] /= 150.0

precipitation = precipitation.drop_attrs()
precipitation = precipitation.assign_attrs({"long_name": "Mean precipitation", "units": "mm hr**-1"})

print(precipitation)

files_mask = sorted(glob(f"/scratch/k10/mr4682/data/track_isccp_olr/mask/mask.track.txuptp.{mode_file_name}.*.nc"))

mask = xr.open_mfdataset(files_mask)[f"mask_track_txuptp_{mode_var_name}"].sel(time=slice(time_start, time_end))

track = calc_object_mean_precipitation(track, max_activity, mask, precipitation)

print(track)

fileo = f"{diro}object.mean.precipitation.{mode_file_name}.{year:04d}{month:02d}.csv"

track.to_csv(fileo)