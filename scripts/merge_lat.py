import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

diri = os.environ.get("DIRI")
filei = os.environ.get("FILEI")
filei_tail = os.environ.get("FILEI_TAIL")
diro = os.environ.get("DIRO")
fileo = os.environ.get("FILEO")
lat_name = os.environ.get("LAT_NAME")

files = sorted(glob(f"{diri}{filei}{filei_tail}*.nc"))

if len(files) == 0:
    sys.exit(f"No .nc files for {diri}{filei}")

x = xr.open_dataset(files[0])

for i in range(1, len(files)):
    xi = xr.open_dataset(files[i])
    
    x = xr.concat([x, xi], dim=lat_name)

x = x.sortby(x[lat_name])

print(x)

x.to_netcdf(path=f"{diro}{fileo}", format="NETCDF4")
