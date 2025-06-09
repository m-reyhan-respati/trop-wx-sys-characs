import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

import filter_mode

diri = os.environ.get("DIRI")
file_name = os.environ.get("FILE_NAME")
var_name = os.environ.get("VAR_NAME")
year_start = int(os.environ.get("YEAR_START"))
year_end = int(os.environ.get("YEAR_END"))
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
mode = os.environ.get("MODE")
spd = int(os.environ.get("SPD"))
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
else:
    sys.exit("Mode unknown!")

if lat_min < 0:
    lat_min_string = f"{np.abs(lat_min):.2f}S"
else:
    lat_min_string = f"{lat_min:.2f}N"

if lat_max < 0:
    lat_max_string = f"{np.abs(lat_max):.2f}S"
else:
    lat_max_string = f"{lat_max:.2f}N"

files = sorted(glob(f"{diri}{file_name}.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}*"))

if len(files) == 1:
    ds = xr.open_dataset(files[0])
elif len(files) > 1:
    ds = xr.open_mfdataset(files)
else:
    sys.exit(f"No .nc files for {diri}{file_name}.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}")

x = ds[f"{var_name}_anom"]

print(x)

ntime = len(x["time"].values)

if len(x.dims) == 3:
    y = filter_mode.filter.filter_mode_TLL(x, mode, ntime, spd)
elif len(x.dims) == 4:
    y = filter_mode.filter.filter_mode_TLLL(x, mode, ntime, spd)
else:
    sys.exit("Only 3D or 4D arrays are accepted")

print(y)

dso = xr.Dataset()
dso[f"{var_name}_{mode_var_name}"] = y

fileo = f"{diro}{file_name}.{mode_file_name}.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
