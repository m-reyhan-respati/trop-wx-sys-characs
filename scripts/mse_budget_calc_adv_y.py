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
ds2 = xr.open_dataset(f"{diri}v/v.{levels[0]:d}.clim.and.anom.1984-2016.nc")
ds3 = xr.open_dataset(f"{diri}sp/tmp/sp.{year:04d}.nc")

mse = ds1["mse"]
v = add_clim_to_anom(ds2["v_clim"], ds2["v_anom"].sel(time=slice(f"{year:04d}-01-01 00:00:00", f"{year:04d}-12-31 21:00:00")), spd=8)
sp = ds3["sp"]

for i in range(1, len(levels)):
    ds1 = xr.open_dataset(f"{diri}mse/tmp/mse.{levels[i]:d}.{year:04d}.nc")
    ds2 = xr.open_dataset(f"{diri}v/v.{levels[i]:d}.clim.and.anom.1984-2016.nc")
    
    mse_i = ds1["mse"]
    v_i = add_clim_to_anom(ds2["v_clim"], ds2["v_anom"].sel(time=slice(f"{year:04d}-01-01 00:00:00", f"{year:04d}-12-31 21:00:00")), spd=8)
    
    mse = xr.concat([mse, mse_i], dim="level")
    v = xr.concat([v, v_i], dim="level")

print(mse)

print(v)

print(sp)

mse_adv_y = -v * budget.gradient.calc_df_dy_TLLL(mse)

for i, level in enumerate(mse[mse.dims[1]].values):
    mse_adv_y[:, i, :, :] = xr.where(sp.values >= level * 100.0, mse_adv_y[:, i, :, :], np.nan, keep_attrs=True)

mse_adv_y = mse_adv_y.assign_attrs({"long_name": "Meridional advection of moist static energy", "units": "J kg**-1 s**-1"})

print(mse_adv_y)

vi_mse_adv_y = budget.integral.calc_vertical_integral_TLLL(mse_adv_y, sp)
vi_mse_adv_y = vi_mse_adv_y.assign_attrs({"long_name": "Vertical integral of meridional advection of moist static energy", "units": "W m**-2"})

print(vi_mse_adv_y)

fout = f"{diro}vi_mse_adv_y.{year:04d}.nc"

dso = xr.Dataset()
dso["vi_mse_adv_y"] = vi_mse_adv_y

dso.to_netcdf(path=fout, format="NETCDF4")
