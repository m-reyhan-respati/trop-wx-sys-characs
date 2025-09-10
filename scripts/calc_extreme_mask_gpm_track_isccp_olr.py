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
year = int(os.environ.get("YEAR"))
month = int(os.environ.get("MONTH"))
mode = os.environ.get("MODE")
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

files = sorted(glob(f"{diri}{file_name}.*.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for {diri}{file_name}")

fileo = f"{diro}extreme.mask.{file_name}.{mode_file_name}.{year:04d}{month:02d}.nc"

month_length = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

year_start = year
month_start = month - 1

if month_start < 1:
    year_start -= 1
    month_start += 12

year_end = year
month_end = month + 1

if month_end > 12:
    year_end += 1
    month_end -= 12

if year_end % 4 == 0:
    month_length[2] = 29

x = xr.open_mfdataset(files)[f"{var_name}"].sel(time=slice(f"{year_start:04d}-{month_start:02d}-01 00:00:00", f"{year_end:04d}-{month_end:02d}-{month_length[month_end]} 21:00:00")).compute()
x = x.transpose("time", "lat", "lon")
x = convert_lon_180_to_360(x, "lon")

if var_name == "precipitation":
    x *= 3.0
    x = x.assign_attrs({"units": "mm"})

print(x)

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

max_activity = max_activity.loc[(pd.to_datetime(max_activity["timestr"].values).year.values == year) & (pd.to_datetime(max_activity["timestr"].values).month.values == month)]

max_activity = max_activity.loc[(max_activity["latitude"].values >= -20.0) & (max_activity["latitude"].values <= 20.0)]

track = track.loc[np.isin(track["cell"].values, max_activity["cell"].values)]

del track["Unnamed: 0"], max_activity["Unnamed: 0"], max_activity["Unnamed: 0.1"]

track.index = np.arange(0, len(track.index), 1, dtype=int)
max_activity.index = np.arange(0, len(max_activity.index), 1, dtype=int)

print(track)
print(max_activity)

if pd.to_datetime(track["timestr"].values).max() > pd.to_datetime(f"{year_end:04d}-{month_end:02d}-{month_length[month_end]} 21:00:00"):
    cell_long = track.loc[pd.to_datetime(track["timestr"].values) == pd.to_datetime(track["timestr"].values).max()]["cell"].values[0]
    
    print(track.loc[track["cell"].values == cell_long]["timestr"])
    
    sys.exit()

if var_name == "precipitation":
    files_p95 = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_mean_{var_name}/percentiles/{var_name}.95p.*.nc"))
else:
    files_p95 = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_{var_name}/percentiles/{var_name}.95p.*.nc"))

ds_p95 = xr.open_mfdataset(files_p95)

p95_DJF = ds_p95[f"{var_name}_DJF_95p"].compute().expand_dims(dim="season")
p95_MAM = ds_p95[f"{var_name}_MAM_95p"].compute().expand_dims(dim="season")
p95_JJA = ds_p95[f"{var_name}_JJA_95p"].compute().expand_dims(dim="season")
p95_SON = ds_p95[f"{var_name}_SON_95p"].compute().expand_dims(dim="season")

p95 = xr.concat([p95_DJF, p95_MAM, p95_JJA, p95_SON], dim="season", combine_attrs="drop")

season = np.asarray(["DJF", "MAM", "JJA", "SON"])
season = xr.DataArray(season, dims=["season"], coords={"season": season}, attrs={"long_name": "Season"})

time = x["time"]

grid_res = 0.1
time_res = (time.values[1] - time.values[0]) / np.timedelta64(1, "h")

lat = np.arange(-30.45, 30.45 + grid_res, grid_res)
lat = xr.DataArray(lat, dims=["lat"], coords={"lat": lat}, attrs=x["lat"].attrs)

lon = np.arange(0.05, 359.95 + grid_res, grid_res)
lon = xr.DataArray(lon, dims=["lon"], coords={"lon": lon}, attrs=x["lon"].attrs)

x_size = 15.0
y_size = 15.0

lagmax = 20

p95 = p95.assign_coords({"season": season, "lat": lat, "lon": lon})
p95 = p95.transpose("season", "lat", "lon")
p95 = convert_lon_180_to_360(p95, "lon")

print(p95)

dso = xr.Dataset()

extreme_mask = track_cell.stats.calc_extreme_mask_gpm(x, track, max_activity, p95, lat, lon, x_size, y_size, grid_res, lagmax, time_res)

print(extreme_mask)

dso[f"extreme_mask_{var_name}_{mode_var_name}"] = extreme_mask

dso.to_netcdf(path=fileo, format="NETCDF4")
