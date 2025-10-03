import numpy as np
import pandas as pd
import xarray as xr
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
lat_min = float(os.environ.get("LAT_MIN"))
lat_max = float(os.environ.get("LAT_MAX"))
diro = os.environ.get("DIRO")

def calc_ci(x, n, lat_range, lon_range):
    for lat_i in lat_range:
        for lon_j in lon_range:
            #print(f"{lat_i:6.2f}, {lon_j:6.2f}", end="\x1b[1K\r")
            
            x_ij = x[:, :, (x["lat"].values > lat_i - 0.05) & (x["lat"].values < lat_i - 0.05 + 0.1), (x["lon"].values > lon_j - 0.05) & (x["lon"].values < lon_j - 0.05 + 0.1)]
            
            ci_low_ij, ci_high_ij = track_cell.stats.bootstrap_mean_ci(x_ij, n=1000)
            
            if lon_j == lon_range[0]:
                ci_low_i = ci_low_ij.copy()
                ci_high_i = ci_high_ij.copy()
            else:
                ci_low_i = xr.concat([ci_low_i, ci_low_ij], dim="lon")
                ci_high_i = xr.concat([ci_high_i, ci_high_ij], dim="lon")
        
        if lat_i == lat_range[0]:
            ci_low = ci_low_i.copy()
            ci_high = ci_high_i.copy()
        else:
            ci_low = xr.concat([ci_low, ci_low_i], dim="lat")
            ci_high = xr.concat([ci_high, ci_high_i], dim="lat")
    
    return ci_low, ci_high

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

if lat_min < 0:
    lat_min_string = f"{np.abs(lat_min):.2f}S"
else:
    lat_min_string = f"{lat_min:.2f}N"

if lat_max < 0:
    lat_max_string = f"{np.abs(lat_max):.2f}S"
else:
    lat_max_string = f"{lat_max:.2f}N"

files = sorted(glob(f"{diri}stacks/stack.{file_name}.{mode_file_name}.*.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for stack.{file_name}.{mode_file_name}")

if raining == 1:
    files_p = sorted(glob(f"{diri}percentiles/raining/{file_name}.{percentile:d}p.*.nc"))
    fileo = f"{diro}raining/bootstrapped.extreme.probability.{percentile:d}p.{file_name}.{mode_file_name}.lag.{lag:.0f}.{lat_min_string}-{lat_max_string}.nc"
else:
    files_p = sorted(glob(f"{diri}percentiles/{file_name}.{percentile:d}p.*.nc"))
    fileo = f"{diro}bootstrapped.extreme.probability.{percentile:d}p.{file_name}.{mode_file_name}.lag.{lag:.0f}.{lat_min_string}-{lat_max_string}.nc"

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
    ds_i = xr.open_dataset(files[i])
    
    x_i = ds_i[f"stack_{var_name}_{mode_var_name}"].sel(lag=slice(lag, lag))[:, :, (ds_i["lat"].values > lat_min - 0.05) & (ds_i["lat"].values < lat_max + 0.05), :]
    
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

lon_range = np.arange(-x_size, x_size + grid_res, grid_res)
lat_range = lon_range[(lon_range > np.min(x["lat"].values) - 0.05) & (lon_range < np.max(x["lat"].values) + 0.05)]

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
            instance = track_cell.stats.calc_extreme_instance_gpm(x, track, max_activity, season, domain, p, lat, lon, x_size, y_size, grid_res, time_res, H_0=1.0 - percentile / 100.0, seasonal_threshold=False, raining=False)
        else:
            instance = track_cell.stats.calc_extreme_instance_gpm(x, track, max_activity, season, domain, p, lat, lon, x_size, y_size, grid_res, time_res, H_0=1.0 - percentile / 100.0, seasonal_threshold=True, raining=False)
        
        print(instance)
        
        probability = instance.mean(dim="n")
        probability = probability.assign_attrs({"long_name": f"Probability of Extreme Event {instance.attrs['long_name'][18:]}"})
        
        print(probability)
        
        ci_low, ci_high = calc_ci(instance, 1000, lat_range, lon_range)
        
        dso[f"extreme_probability_{var_name}_{mode_var_name}_{season}_{domain}"] = probability
        dso[f"ci_low_{var_name}_{mode_var_name}_{season}_{domain}"] = ci_low
        dso[f"ci_high_{var_name}_{mode_var_name}_{season}_{domain}"] = ci_high

dso.to_netcdf(path=fileo, format="NETCDF4")
