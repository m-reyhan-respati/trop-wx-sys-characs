import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

import budget

diri = os.environ.get("DIRI")
year = int(os.environ.get("YEAR"))
diro = os.environ.get("DIRO")

if (year < 1984) | (year > 2016):
    sys.exit("There are only data from year 1984 to year 2016")

levels = [100, 125, 150, 175, 200, 225, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 775, 800, 825, 850, 875, 900, 925, 950, 975, 1000]

ds1 = xr.open_dataset(f"{diri}mse/tmp/mse.{levels[0]:d}.{year:04d}.nc")
ds2 = xr.open_dataset(f"{diri}sp/tmp/sp.{year:04d}.nc")

mse = ds1["mse"]
sp = ds2["sp"]

for i in range(1, len(levels)):
    ds1 = xr.open_dataset(f"{diri}mse/tmp/mse.{levels[i]:d}.{year:04d}.nc")
    
    mse_i = ds1["mse"]
    
    mse = xr.concat([mse, mse_i], dim="level")

print(mse)

print(sp)

mse_d_dt = budget.gradient.calc_df_dt_TLLL(mse)

for i, level in enumerate(mse[mse.dims[1]].values):
    mse_d_dt[:, i, :, :] = xr.where(sp.values >= level * 100.0, mse_d_dt[:, i, :, :], np.nan, keep_attrs=True)

mse_d_dt = mse_d_dt.assign_attrs({"long_name": "Temporal tendency of moist static energy", "units": "J kg**-1 s**-1"})

print(mse_d_dt)

vi_mse_d_dt = budget.integral.calc_vertical_integral_TLLL(mse_d_dt, sp)
vi_mse_d_dt = vi_mse_d_dt.assign_attrs({"long_name": "Vertical integral of temporal tendency of moist static energy", "units": "W m**-2"})

print(vi_mse_d_dt)

fout = f"{diro}vi_mse_d_dt.{year:04d}.nc"

dso = xr.Dataset()
dso["vi_mse_d_dt"] = vi_mse_d_dt

dso.to_netcdf(path=fout, format="NETCDF4")
