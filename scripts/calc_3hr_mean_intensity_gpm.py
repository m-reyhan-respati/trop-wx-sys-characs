import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import sys

from config import *
from util import *

year = int(os.environ.get("YEAR"))
month = int(os.environ.get("MONTH"))
diro = os.environ.get("DIRO")

var_name = "precipitation"
spd = 48
threshold = 0.05

x = open_gpm_ia39_per_month(var_name, year, month, spd, lat_min=-30.5, lat_max=30.5)

duration = xr.where(x >= threshold, 0.5, 0.0)

duration = duration.resample(time="3h").sum()

print(f"Duration has {np.count_nonzero(np.isnan(duration.values))} NaNs")

x = xr.where(x >= threshold, x * 0.5, 0.0)

x = x.resample(time="3h").sum()

x = xr.where(duration != 0.0, x / duration, 0.0)

print(f"Mean intensity has {np.count_nonzero(np.isnan(x.values))} NaNs")

x = x.assign_attrs({"long_name": "Mean intensity of precipitation within 3-hour time window", "units": "mm hr**-1"})

dso = xr.Dataset()
dso["mean_intensity"] = x

print(dso)

dso.to_netcdf(path=f"{diro}mean_intensity.{year:04d}{month:02d}.nc")
