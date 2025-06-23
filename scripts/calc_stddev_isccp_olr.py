import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

year_start = int(os.environ.get("YEAR_START"))
year_end = int(os.environ.get("YEAR_END"))
mode = os.environ.get("MODE")
diro = os.environ.get("DIRO")

diri = "/scratch/k10/mr4682/data/ISCCP/txuptp/filtered/"

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
    diri = "/scratch/k10/mr4682/data/ISCCP/txuptp/tmp/"
else:
    sys.exit("Mode unknown!")

files = f"{diri}txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.*.nc"

if len(files) == 1:
    ds = xr.open_dataset(files[0])
elif len(files) > 1:
    ds = xr.open_mfdataset(files)
else:
    sys.exit(f"No .nc files for txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}")

if mode == "Raw Anomaly":
    mode_file_name = "anom"

x = ds[f"txuptp_{mode_var_name}"]

print(x)

x_stddev = x.std(ddof=1)

print(x_stddev)

dso = xr.Dataset()
dso[f"stddev_txuptp_{mode_var_name}"] = x_stddev

fileo = f"{diro}stddev.txuptp.{mode_file_name}.{year_start:04d}-{year_end:04d}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
