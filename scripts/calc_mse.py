import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

diri = os.environ.get("DIRI")
level = int(os.environ.get("LEVEL"))
year = int(os.environ.get("YEAR"))
diro = os.environ.get("DIRO")

if (year < 1984) | (year > 2016):
    sys.exit("There are only data from year 1984 to year 2016")

files1 = sorted(glob(f"{diri}t/t.{level:d}.clim.and.anom.1984-2016.nc"))

if len(files1) == 0:
    sys.exit(f"No .nc file for t.{level:d}.clim.and.anom.1984-2016.nc")

ds1 = xr.open_dataset(files1[0])

files2 = sorted(glob(f"{diri}z/z.{level:d}.clim.and.anom.1984-2016.nc"))

if len(files2) == 0:
    sys.exit(f"No .nc file for z.{level:d}.clim.and.anom.1984-2016.nc")

ds2 = xr.open_dataset(files2[0])

files3 = sorted(glob(f"{diri}q/q.{level:d}.clim.and.anom.1984-2016.nc"))

if len(files3) == 0:
    sys.exit(f"No .nc file for q.{level:d}.clim.and.anom.1984-2016.nc")

ds3 = xr.open_dataset(files3[0])

t = add_clim_to_anom(ds1["t_clim"], ds1["t_anom"].sel(time=slice(f"{year:04d}-01-01 00:00:00", f"{year:04d}-12-31 21:00:00")), spd=8)

print(t)

z = add_clim_to_anom(ds2["z_clim"], ds2["z_anom"].sel(time=slice(f"{year:04d}-01-01 00:00:00", f"{year:04d}-12-31 21:00:00")), spd=8)

print(z)

q = add_clim_to_anom(ds3["q_clim"], ds3["q_anom"].sel(time=slice(f"{year:04d}-01-01 00:00:00", f"{year:04d}-12-31 21:00:00")), spd=8)

print(q)

mse = calc_mse(t, z, q)

print(mse)

dso = xr.Dataset()
dso["mse"] = mse

fileo = f"{diro}mse.{level:d}.{year:d}.nc"

dso.to_netcdf(fileo, format="NETCDF4")
