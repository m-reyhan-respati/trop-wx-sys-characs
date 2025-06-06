import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import sys

from config import *
from util import *

year = int(os.environ.get("YEAR"))
diro = os.environ.get("DIRO")

var_name = "precipitation"
spd = 8

x = open_gpm_ia39_per_year(var_name, year, spd, lat_min=-30.5, lat_max=30.5)

dso = xr.Dataset()
dso["precipitation"] = x

print(dso)

dso.to_netcdf(path=f"{diro}precipitation.{year:04d}.nc")
