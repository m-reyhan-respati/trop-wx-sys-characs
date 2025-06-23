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
    diri = f"{diri}../"
else:
    sys.exit("Mode unknown!")

files = sorted(glob(f"{diri}txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}")

if mode == "Raw Anomaly":
    mode_file_name = "anom"

x = xr.open_dataset(files[0])[f"txuptp_{mode_var_name}"]

print(x)

track_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.csv"))

if len(track_files) == 0:
    sys.exit(f"No .csv file for track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}")

track = pd.read_csv(track_files[0])

max_activity = track_cell.track.calc_max_activity(x, track, x["latitude"], x["longitude"], margin=2.0)

print(max_activity)

fileo = f"{diro}max.activity.track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.csv"

max_activity.to_csv(fileo)
