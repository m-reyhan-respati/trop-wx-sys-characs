import numpy as np
import pandas as pd
import xarray as xr
import tobac
from glob import glob

import os
import sys

from config import *
from util import *

import track_cell

diri = os.environ.get("DIRI")
file_name = os.environ.get("FILE_NAME")
var_name = os.environ.get("VAR_NAME")
mode = os.environ.get("MODE")
percentile = int(os.environ.get("PERCENTILE"))
raining = int(os.environ.get("RAINING"))
lag = float(os.environ.get("LAG"))
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
elif mode == "Tropical Cyclone":
    mode_var_name = "anom_tc"
    mode_file_name = "tc"
else:
    sys.exit("Mode unknown!")

files = sorted(glob(f"{diri}stacks/stack.{file_name}.{mode_file_name}.*.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for stack.{file_name}.{mode_file_name}")

if raining == 1:
    files_p = sorted(glob(f"{diri}percentiles/raining/{file_name}.{percentile:d}p.*.nc"))
    fileo = f"{diro}raining/extreme.probability.{percentile:d}p.{file_name}.{mode_file_name}.lag.{lag:.0f}.nc"
else:
    files_p = sorted(glob(f"{diri}percentiles/{file_name}.{percentile:d}p.*.nc"))
    fileo = f"{diro}extreme.probability.{percentile:d}p.{file_name}.{mode_file_name}.lag.{lag:.0f}.nc"

track_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/track.txuptp.{mode_file_name}.*.csv"))

if len(track_files) == 0:
    sys.exit(f"No .csv file for track.txuptp.{mode_file_name}")

track = pd.read_csv(track_files[0])

track_before = track.loc[pd.to_datetime(track["timestr"].values) < pd.to_datetime("2000-06-01 00:00:00")]

max_activity_files = sorted(glob(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.{mode_file_name}.*.csv"))

if len(max_activity_files) == 0:
    sys.exit(f"No .csv file for max.activity.track.txuptp.{mode_file_name}")

max_activity = pd.read_csv(max_activity_files[0])

max_activity = max_activity.loc[~np.isin(max_activity["cell"].values, track_before["cell"].values)]

max_activity = max_activity.loc[(max_activity["latitude"].values >= -20.0) & (max_activity["latitude"].values <= 20.0)]

track = track.loc[np.isin(track["cell"].values, max_activity["cell"].values)]

del track["Unnamed: 0"], max_activity["Unnamed: 0"], max_activity["Unnamed: 0.1"]

track.index = np.arange(0, len(track.index), 1, dtype=int)
max_activity.index = np.arange(0, len(max_activity.index), 1, dtype=int)

print(track)
print(max_activity)

for i in range(0, len(files)):
    x_i = xr.open_dataset(files[i])[f"stack_{var_name}_{mode_var_name}"].sel(lag=slice(lag, lag))
    
    if i == 0:
        x = x_i.copy()
    else:
        x = xr.concat([x, x_i], dim="n")

print(x)

ds_p = xr.open_mfdataset(files_p)

p_ALL = ds_p[f"{var_name}_ALL_{percentile:d}p"].compute().expand_dims(dim="season")
p_DJF = ds_p[f"{var_name}_DJF_{percentile:d}p"].compute().expand_dims(dim="season")
p_MAM = ds_p[f"{var_name}_MAM_{percentile:d}p"].compute().expand_dims(dim="season")
p_JJA = ds_p[f"{var_name}_JJA_{percentile:d}p"].compute().expand_dims(dim="season")
p_SON = ds_p[f"{var_name}_SON_{percentile:d}p"].compute().expand_dims(dim="season")

p = xr.concat([p_ALL, p_DJF, p_MAM, p_JJA, p_SON], dim="season", combine_attrs="drop")

season = np.asarray(["ALL", "DJF", "MAM", "JJA", "SON"])
season = xr.DataArray(season, dims=["season"], coords={"season": season}, attrs={"long_name": "Season"})

p = p.assign_coords({"season": season})
p = p.transpose("season", "lat", "lon")
p = convert_lon_180_to_360(p, "lon")

print(p)

x_size = 15.0
y_size = 15.0

grid_res = 0.1
time_res = 3.0

lat = np.arange(-30.45, 30.45 + grid_res, grid_res)
lat = xr.DataArray(lat, dims=["lat"], coords={"lat": lat}, attrs=x["lat"].attrs)

lon = np.arange(0.05, 359.95 + grid_res, grid_res)
lon = xr.DataArray(lon, dims=["lon"], coords={"lon": lon}, attrs=x["lon"].attrs)

seasons = ["ALL", "DJF", "MAM", "JJA", "SON"]
domains = ["EQ", "NH", "SH"]

dso = xr.Dataset()

for season in seasons:
    for domain in domains:
        if season == "ALL":
            probability, pvalue = track_cell.stats.calc_extreme_probability_gpm(x, track, max_activity, season, domain, p, lat, lon, x_size, y_size, grid_res, time_res, H_0=1.0 - percentile / 100.0, seasonal_threshold=False, raining=False)
        else:
            probability, pvalue = track_cell.stats.calc_extreme_probability_gpm(x, track, max_activity, season, domain, p, lat, lon, x_size, y_size, grid_res, time_res, H_0=1.0 - percentile / 100.0, seasonal_threshold=True, raining=False)
        
        print(probability)
        
        dso[f"extreme_probability_{var_name}_{mode_var_name}_{season}_{domain}"] = probability
        dso[f"pvalue_{var_name}_{mode_var_name}_{season}_{domain}"] = pvalue

dso.to_netcdf(path=fileo, format="NETCDF4")
