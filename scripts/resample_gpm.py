import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import sys

from config import *

year = int(os.environ.get("YEAR"))
diro = os.environ.get("DIRO")

if (year < 2000) | (year > 2016):
    sys.exit("Data is only available from 2000 to 2016")

var_name = "precipitation"

ds = xr.open_dataset(f"{SCRATCH_GPM_DIR}/3hr_mean_precipitation/precipitation.{year:04d}.nc")

x = ds[var_name]
x = x.drop_attrs()
x *= 3.0

lat = np.arange(-30.45, 30.45 + 0.1, 0.1)
lat = xr.DataArray(lat, dims=["lat"], coords={"lat": lat}, attrs=x["lat"].attrs)

lon = np.arange(-179.95, 179.95 + 0.1, 0.1)
lon = xr.DataArray(lon, dims=["lon"], coords={"lon": lon}, attrs=x["lon"].attrs)

x = x.assign_coords({"lat": lat, "lon": lon})
x = x.assign_attrs({"long_name": "3-hourly precipitation accumulation", "units": "mm"})

print(x)

lon_new = np.zeros(lon.shape) * np.nan
lon_new[0:len(lon_new) // 2] = lon[len(lon_new) // 2:].values
lon_new[len(lon_new) // 2:] = lon[0:len(lon_new) // 2].values + 360.0

x_new = np.zeros(x.shape) * np.nan
x_new[:, 0:len(lon_new) // 2, :] = x[:, len(lon_new) // 2:, :].values
x_new[:, len(lon_new) // 2:, :] = x[:, 0:len(lon_new) // 2, :].values

lon_new = xr.DataArray(lon_new, dims=["lon"], coords={"lon": lon_new}, attrs=lon.attrs)

x_new = xr.DataArray(x_new, dims=["time", "lon", "lat"], coords={"time": x["time"], "lon": lon_new, "lat": x["lat"]}, attrs=x.attrs)

del x

lat_pad = np.arange(-30.95, 30.95 + 0.1, 0.1)
lat_pad = xr.DataArray(lat_pad, dims=["lat"], coords={"lat": lat_pad}, attrs=lat.attrs)

x_new = x_new.pad(lat=5, keep_attrs=True)
x_new = x_new.assign_coords({"lat": lat_pad})

print(x_new)

x_1p0deg = x_new.coarsen(lat=10, lon=10).sum()
x_1p0deg = x_1p0deg.assign_attrs({"long_name": "3-hourly precipitation accumulation over 10 by 10 grid boxes around the coordinate points", "units": "mm"})

print(x_1p0deg)

dso = xr.Dataset()
dso[var_name] = x_1p0deg

fileo = f"{diro}precipitation.{year:04d}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")