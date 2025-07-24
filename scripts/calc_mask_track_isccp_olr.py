import numpy as np
import pandas as pd
import xarray as xr
import tobac
from glob import glob

import os
import sys

from config import *

diri = os.environ.get("DIRI")
year = int(os.environ.get("YEAR"))
mode = os.environ.get("MODE")
diro = os.environ.get("DIRO")

if (year < 1984) | (year > 2016):
    sys.exit("Year must be between 1984 and 2016")

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
    diri_olr = f"{SCRATCH_ISCCP_DIR}/txuptp/"
else:
    diri_olr = f"{SCRATCH_ISCCP_DIR}/txuptp/filtered/"

olr = xr.open_dataset(f"{diri_olr}txuptp.{mode_file_name}.1984-2016.nc")[f"txuptp_{mode_var_name}"].sel(time=slice(f"{year:04d}-01-01 00:00:00", f"{year:04d}-12-31 21:00:00"))

if mode == "Raw Anomaly":
    mode_file_name = "anom"

stddev_olr = xr.open_dataset(f"{SCRATCH_ISCCP_DIR}/txuptp/stddev.txuptp.{mode_file_name}.1984-2016.nc")[f"stddev_txuptp_{mode_var_name}"].values

olr /= stddev_olr

olr = olr.assign_attrs({"units": "standard deviation"})

print(olr)

track_files = sorted(glob(f"{diri}track.txuptp.{mode_file_name}.1984-2016.csv"))

if len(track_files) == 0:
    sys.exit(f"No .csv file for track.txuptp.{mode_file_name}.1984-2016")

track = pd.read_csv(track_files[0])
track = track.loc[pd.to_datetime(track["timestr"].values).year == year]

print(track)

max_activity_files = sorted(glob(f"{diri}max.activity.track.txuptp.{mode_file_name}.1984-2016.csv"))

if len(max_activity_files) == 0:
    sys.exit(f"No .csv file for max.activity.track.txuptp.{mode_file_name}.1984-2016")

max_activity = pd.read_csv(max_activity_files[0])

track = track.loc[np.isin(track["cell"].values, max_activity["cell"].values)]

track.index = np.arange(0, len(track.index), 1, dtype=int)

time = olr["time"]
latitude = olr["latitude"]
longitude = olr["longitude"]

dxy = np.radians(longitude.values[1] - longitude.values[0]) * 6.371e+06
dt = (time.values[1] - time.values[0]) / np.timedelta64(1, "s")

parameters_segmentation = {}
parameters_segmentation["threshold"] = -2.0
parameters_segmentation["target"] = "minimum"
parameters_segmentation["PBC_flag"] = "hdim_2"

mask, track = tobac.segmentation_2D(track, olr, dxy=dxy, **parameters_segmentation)

print(track)
print(mask)

dso = xr.Dataset()
dso[f"mask_track_txuptp_{mode_var_name}"] = mask

fileo = f"{diro}mask.track.txuptp.{mode_file_name}.{year:04d}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
