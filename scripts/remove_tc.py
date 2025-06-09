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
lat_name = os.environ.get("LAT_NAME")
lon_name = os.environ.get("LON_NAME")
diro = os.environ.get("DIRO")

if lat_min < 0:
    lat_min_string = f"{np.abs(lat_min):.2f}S"
else:
    lat_min_string = f"{lat_min:.2f}N"

if lat_max < 0:
    lat_max_string = f"{np.abs(lat_max):.2f}S"
else:
    lat_max_string = f"{lat_max:.2f}N"

files = sorted(glob(f"{diri}{file_name}.clim.and.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}*"))

if len(files) == 1:
    ds = xr.open_dataset(files[0])
elif len(files) > 1:
    ds = xr.open_mfdataset(files)
else:
    sys.exit(f"No .nc files for {diri}{file_name}.clim.and.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}")

x = ds[f"{var_name}_anom"]

print(x)

if len(x.dims) == 3:
    y = filter_mode.filter.remove_tc_TLL(x, x["time"], x[lat_name], x[lon_name])
elif len(x.dims) == 4:
    y = filter_mode.filter.remove_tc_TLLL(x, x["time"], x[lat_name], x[lon_name])
else:
    sys.exit("Only 3D or 4D arrays are accepted")

print(y)

tc = np.copy(x.values) - np.copy(y.values)
tc = xr.DataArray(tc, dims=x.dims, coords=x.coords)
tc = tc.assign_attrs({"long_name": f"Tropical cylones in {x.attrs['long_name']}", "units": x.attrs["units"]})

print(tc)

dso1 = xr.Dataset()
dso1[f"{var_name}_anom"] = y

dso1.to_netcdf(path=f"{diro}{file_name}.anom.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}.nc", format="NETCDF4")

dso2 = xr.Dataset()
dso2[f"{var_name}_anom_tc"] = tc

dso2.to_netcdf(path=f"{diro}{file_name}.tc.{year_start:04d}-{year_end:04d}.{lat_min_string}_{lat_max_string}.nc", format="NETCDF4")
