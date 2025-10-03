import numpy as np
import pandas as pd
import xarray as xr
import tobac
from glob import glob

import os
import sys

from config import *
from util import *

import track_cell

diri = os.environ.get("DIRI")
file_name = os.environ.get("FILE_NAME")
var_name = os.environ.get("VAR_NAME")
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
else:
    sys.exit("Mode unknown!")

files = sorted(glob(f"{diri}{file_name}.clim.and.anom.{year_start:04d}-{year_end:04d}.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for {diri}{file_name}.clim.and.anom.{year_start:04d}-{year_end:04d}.nc")

fileo = f"{diro}composite.{file_name}.{mode_file_name}.{year_start:04d}-{year_end:04d}.nc"

ds = xr.open_dataset(files[0])

clim = ds[f"{var_name}_clim"]
anom = ds[f"{var_name}_anom"]

print(anom)

x = add_clim_to_anom(clim, anom, spd=8)

print(x)

track_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.csv"))

if len(track_files) == 0:
    sys.exit(f"No .csv file for track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}")

track = pd.read_csv(track_files[0])

print(track)

max_activity_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.csv"))

if len(max_activity_files) == 0:
    sys.exit(f"No .csv file for max.activity.track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}")

max_activity = pd.read_csv(max_activity_files[0])

print(max_activity)

time = x["time"]
lat = x["latitude"]
lon = x["longitude"]

grid_res = lon.values[1] - lon.values[0]
time_res = (time.values[1] - time.values[0]) / np.timedelta64(1, "h")

x_size = 15.0
y_size = 15.0

lagmax = 20

seasons = ["ALL", "DJF", "MAM", "JJA", "SON"]
domains = ["EQ", "NH", "SH"]

dso = xr.Dataset()

for season in seasons:
    for domain in domains:
        if len(x.dims) == 3:
            composite, pvalue = track_cell.stats.compositeTLL(x, track, max_activity, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res)
        elif len(x.dims) == 4:
            composite, pvalue = track_cell.stats.compositeTLLL(x, track, max_activity, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res)
        else:
            sys.exit("Only 3D or 4D arrays are accepted")
        
        print(composite)
        
        dso[f"composite_{var_name}_{mode_var_name}_{season}_{domain}"] = composite
        dso[f"pvalue_{var_name}_{mode_var_name}_{season}_{domain}"] = pvalue

dso.to_netcdf(path=fileo, format="NETCDF4")
