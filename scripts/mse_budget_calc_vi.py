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

for i, level in enumerate(mse[mse.dims[1]].values):
    mse[:, i, :, :] = xr.where(sp.values >= level * 100.0, mse[:, i, :, :], np.nan, keep_attrs=True)

print(mse)

vi_mse = budget.integral.calc_vertical_integral_TLLL(mse, sp)
vi_mse = vi_mse.assign_attrs({"long_name": "Vertical integral of moist static energy", "units": "J m**-2"})

print(vi_mse)

fout = f"{diro}vi_mse.{year:04d}.nc"

dso = xr.Dataset()
dso["vi_mse"] = vi_mse

dso.to_netcdf(path=fout, format="NETCDF4")
