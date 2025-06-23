import numpy as np
import pandas as pd
import xarray as xr
import tobac
from glob import glob

import os
import sys

from config import *

diri = os.environ.get("DIRI")
year_start = int(os.environ.get("YEAR_START"))
year_end = int(os.environ.get("YEAR_END"))
mode = os.environ.get("MODE")
diro = os.environ.get("DIRO")

if mode == "Moisture Mode":
    mode_var_name = "moisture_mode"
    mode_file_name = "moisture.mode"
    v_max = 9.0
elif mode == "Mixed System":
    mode_var_name = "mixed_system"
    mode_file_name = "mixed.system"
    v_max = 27.0
elif mode == "IG Wave":
    mode_var_name = "ig_wave"
    mode_file_name = "ig.wave"
    v_max = 50.0
elif mode == "Eastward Moisture Mode":
    mode_var_name = "eastward_moisture_mode"
    mode_file_name = "eastward.moisture.mode"
    v_max = 9.0
elif mode == "Eastward Mixed System":
    mode_var_name = "eastward_mixed_system"
    mode_file_name = "eastward.mixed.system"
    v_max = 27.0
elif mode == "Eastward IG Wave":
    mode_var_name = "eastward_ig_wave"
    mode_file_name = "eastward.ig.wave"
    v_max = 50.0
elif mode == "Westward Moisture Mode":
    mode_var_name = "westward_moisture_mode"
    mode_file_name = "westward.moisture.mode"
    v_max = 9.0
elif mode == "Westward Mixed System":
    mode_var_name = "westward_mixed_system"
    mode_file_name = "westward.mixed.system"
    v_max = 27.0
elif mode == "Westward IG Wave":
    mode_var_name = "westward_ig_wave"
    mode_file_name = "westward.ig.wave"
    v_max = 50.0
elif mode == "Tropical Cyclone":
    mode_var_name = "anom_tc"
    mode_file_name = "tc"
    v_max = 10.0
elif mode == "Raw Anomaly":
    mode_var_name = "anom"
    mode_file_name = "clim.and.anom"
    v_max = 50.0
    diri = f"{diri}../"
else:
    sys.exit("Mode unknown!")

files = sorted(glob(f"{diri}txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}")

if mode == "Raw Anomaly":
    mode_file_name = "anom"

x = xr.open_dataset(files[0])[f"txuptp_{mode_var_name}"]
x = x.assign_attrs({"units": "W m**-2"})

x_stddev = xr.open_dataset(f"{SCRATCH_ISCCP_DIR}/txuptp/stddev.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.nc")[f"stddev_txuptp_{mode_var_name}"].values

x /= x_stddev
x = x.fillna(0.0)

print(x)

time = x["time"]
latitude = x["latitude"]
longitude = x["longitude"]

dxy = np.radians(longitude.values[1] - longitude.values[0]) * 6.371e+06
dt = (time.values[1] - time.values[0]) / np.timedelta64(1, "s")

parameters_features = {}
parameters_features["threshold"] = [-2.0, -2.5, -3.0, -3.5, -4.0]
parameters_features["target"] = "minimum"
parameters_features["position_threshold"] = "weighted_diff"
parameters_features["sigma_threshold"] = 1.0
parameters_features["n_min_threshold"] = 4
parameters_features["PBC_flag"] = "hdim_2"

Features = tobac.feature_detection_multithreshold(x, dxy, **parameters_features)

parameters_linking = {}
parameters_linking["v_max"] = v_max
parameters_linking["stubs"] = 3
parameters_linking["method_linking"] = "predict"
parameters_linking["PBC_flag"] = "hdim_2"
parameters_linking["min_h2"] = 0
parameters_linking["max_h2"] = len(longitude.values) - 1

Track = tobac.linking_trackpy(Features, x, dt=dt, dxy=dxy, **parameters_linking)
Track = Track.loc[Track["cell"] != -1]

print(Track)

fileo = f"{diro}track.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.csv"

Track.to_csv(fileo)
