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
filtered = int(os.environ.get("FILTERED"))
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

if filtered == 1:
    files = sorted(glob(f"{diri}filtered/{file_name}.{mode_file_name}.1984-2016.nc"))
    
    if len(files) == 0:
        sys.exit(f"No .nc file for {diri}filtered/{file_name}.{mode_file_name}.1984-2016")
    
    fileo_rn = f"{diro}composite.raining.{file_name}.{mode_file_name}.nc"
    fileo_nr = f"{diro}composite.non.raining.{file_name}.{mode_file_name}.nc"
elif filtered == 0:
    files = sorted(glob(f"{diri}{file_name}.clim.and.anom.1984-2016.nc"))
    
    if len(files) == 0:
        sys.exit(f"No .nc file for {diri}{file_name}.clim.and.anom.1984-2016.nc")
    
    fileo_rn = f"{diro}composite.raining.anom.{file_name}.{mode_file_name}.nc"
    fileo_nr = f"{diro}composite.non.raining.anom.{file_name}.{mode_file_name}.nc"
    
    mode_var_name = "anom"
else:
    sys.exit("filtered must be either 0 or 1")

x = xr.open_dataset(files[0])[f"{var_name}_{mode_var_name}"]

print(x)

track_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/object_mean_precipitation/object.mean.precipitation.{mode_file_name}.*.csv"))

if len(track_files) == 0:
    sys.exit(f"No .csv file for object.mean.precipitation.{mode_file_name}")

track = pd.DataFrame()

for i in range(0, len(track_files)):
    track = pd.concat([track, pd.read_csv(track_files[i])])

del track["Unnamed: 0.1"], track["Unnamed: 0"]

print(track)

max_activity_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.{mode_file_name}.1984-2016.csv"))

if len(max_activity_files) == 0:
    sys.exit(f"No .csv file for max.activity.track.txuptp.{mode_file_name}")

max_activity = pd.read_csv(max_activity_files[0])

del max_activity["Unnamed: 0.1"], max_activity["Unnamed: 0"]

print(max_activity)

max_activity_rn = track.loc[np.isin(track["feature"].values, max_activity["feature"].values) & (track["precipitation"].values >= 0.05)]

max_activity_rn.index = np.arange(0, len(max_activity_rn.index), 1, dtype=int)

print(max_activity_rn)

max_activity_nr = track.loc[np.isin(track["feature"].values, max_activity["feature"].values) & (track["precipitation"].values < 0.05)]

max_activity_nr.index = np.arange(0, len(max_activity_nr.index), 1, dtype=int)

print(max_activity_nr)

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

dso_rn = xr.Dataset()
dso_nr = xr.Dataset()

for season in seasons:
    for domain in domains:
        if len(x.dims) == 3:
            composite_rn, pvalue_rn = track_cell.stats.compositeTLL(x, track, max_activity_rn, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res)
            composite_nr, pvalue_nr = track_cell.stats.compositeTLL(x, track, max_activity_nr, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res)
        elif len(x.dims) == 4:
            composite_rn, pvalue_rn = track_cell.stats.compositeTLLL(x, track, max_activity_rn, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res)
            composite_nr, pvalue_nr = track_cell.stats.compositeTLLL(x, track, max_activity_nr, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res)
        else:
            sys.exit("Only 3D or 4D arrays are accepted")
        
        print(composite_rn)
        print(composite_nr)
        
        dso_rn[f"composite_{var_name}_{mode_var_name}_{season}_{domain}"] = composite_rn
        dso_rn[f"pvalue_{var_name}_{mode_var_name}_{season}_{domain}"] = pvalue_rn
        
        dso_nr[f"composite_{var_name}_{mode_var_name}_{season}_{domain}"] = composite_nr
        dso_nr[f"pvalue_{var_name}_{mode_var_name}_{season}_{domain}"] = pvalue_nr

dso_rn.to_netcdf(path=fileo_rn, format="NETCDF4")
dso_nr.to_netcdf(path=fileo_nr, format="NETCDF4")