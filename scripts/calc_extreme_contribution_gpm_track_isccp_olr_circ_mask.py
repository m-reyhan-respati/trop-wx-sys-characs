import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

diri = os.environ.get("DIRI")
file_name = os.environ.get("FILE_NAME")
var_name = os.environ.get("VAR_NAME")
year = int(os.environ.get("YEAR"))
month = int(os.environ.get("MONTH"))
percentile = int(os.environ.get("PERCENTILE"))
radius = float(os.environ.get("RADIUS"))
raining = int(os.environ.get("RAINING"))
diro = os.environ.get("DIRO")

def calc_circ_mask(track, time, lat, lon, radius, grid_res):
    lon2d, lat2d = np.meshgrid(lon.values, lat.values)
    
    circ_mask = np.zeros((len(time.values), len(lat.values), len(lon.values)), dtype=int)
    
    for t in range(0, len(time.values)):
        track_t = track.loc[pd.to_datetime(track["timestr"].values) == pd.to_datetime(time.values[t])]
        
        for i in range(0, len(track_t["feature"].values)):
            lat_i = track_t["latitude"].values[i]
            lon_i = track_t["longitude"].values[i]
            
            c = np.sin(np.radians(lat2d)) * np.sin(np.radians(lat_i)) + np.cos(np.radians(lat2d)) * np.cos(np.radians(lat_i)) * np.cos(np.radians(lon2d - lon_i))
            c = np.where((c >= -1.0) & (c <= 1.0), c, np.nan)
            
            r = np.arccos(c) * 6371.0
            r = np.where(r <= radius, 1.0, 0.0)
            
            circ_mask[t, :, :] += r.astype(int)
    
    circ_mask = np.where(circ_mask > 0, 1, circ_mask)
    
    circ_mask = xr.DataArray(circ_mask, dims=["time", "lat", "lon"], coords={"time": time, "lat": lat, "lon": lon})
    
    return circ_mask

files = sorted(glob(f"{diri}{file_name}.*.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for {diri}{file_name}")

if raining == 1:
    files_p = sorted(glob(f"{diri}percentiles/raining/{file_name}.{percentile:d}p.*.nc"))
    fileo = f"{diro}raining/circ_mask/extreme.contribution.{percentile:d}p.{file_name}.{year:04d}{month:02d}.circ.mask.{radius:.1f}km.nc"
else:
    files_p = sorted(glob(f"{diri}percentiles/{file_name}.{percentile:d}p.*.nc"))
    fileo = f"{diro}circ_mask/extreme.contribution.{percentile:d}p.{file_name}.{year:04d}{month:02d}.circ.mask.{radius:.1f}km.nc"

month_length = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

if year % 4 == 0:
    month_length[2] = 29

x = xr.open_mfdataset(files)[f"{var_name}"].sel(time=slice(f"{year:04d}-{month:02d}-01 00:00:00", f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00")).compute()
x = x.transpose("time", "lat", "lon")
x = convert_lon_180_to_360(x, "lon")

if var_name == "precipitation":
    x *= 3.0
    x = x.assign_attrs({"units": "mm"})

time = x["time"]

seasons = {1: "DJF", 2: "DJF", 3: "MAM", 4: "MAM", 5: "MAM", 6: "JJA", 7: "JJA", 8: "JJA", 9: "SON", 10: "SON", 11: "SON", 12: "DJF"}

ds_p = xr.open_mfdataset(files_p)

p_all = ds_p[f"{var_name}_ALL_{percentile:d}p"].compute()
p_season = ds_p[f"{var_name}_{seasons[month]}_{percentile:d}p"].compute()

p_all = p_all.transpose("lat", "lon")
p_all = convert_lon_180_to_360(p_all, "lon")

p_season = p_season.transpose("lat", "lon")
p_season = convert_lon_180_to_360(p_season, "lon")

x_all = xr.where((x >= p_all), 1.0, 0.0)
x_all = xr.where((p_all > 0.0), x_all, 0.0)
x_all = x_all.transpose("time", "lat", "lon")

x_season = xr.where((x >= p_season), 1.0, 0.0)
x_season = xr.where((p_season > 0.0), x_season, 0.0)
x_season = x_season.transpose("time", "lat", "lon")

del x

print(x_all)
print(x_season)

track1 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/track.txuptp.moisture.mode.1984-2016.csv")
track2 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/track.txuptp.mixed.system.1984-2016.csv")
track3 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/track.txuptp.ig.wave.1984-2016.csv")
track4 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/track.txuptp.tc.1984-2016.csv")
track5 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/track.txuptp.anom.1984-2016.csv")

max_activity1 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.moisture.mode.1984-2016.csv")
max_activity2 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.mixed.system.1984-2016.csv")
max_activity3 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.ig.wave.1984-2016.csv")
max_activity4 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.tc.1984-2016.csv")
max_activity5 = pd.read_csv(f"{SCRATCH_TRACK_DIR}/max.activity.track.txuptp.anom.1984-2016.csv")

track1 = track1.loc[np.isin(track1["cell"].values, max_activity1["cell"].values)]
track2 = track2.loc[np.isin(track2["cell"].values, max_activity2["cell"].values)]
track3 = track3.loc[np.isin(track3["cell"].values, max_activity3["cell"].values)]
track4 = track4.loc[np.isin(track4["cell"].values, max_activity4["cell"].values)]
track5 = track5.loc[np.isin(track5["cell"].values, max_activity5["cell"].values)]

track1 = track1.loc[(pd.to_datetime(track1["timestr"].values) >= pd.to_datetime(f"{year:04d}-{month:02d}-01 00:00:00")) & (pd.to_datetime(track1["timestr"].values) <= pd.to_datetime(f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))]
track2 = track2.loc[(pd.to_datetime(track2["timestr"].values) >= pd.to_datetime(f"{year:04d}-{month:02d}-01 00:00:00")) & (pd.to_datetime(track2["timestr"].values) <= pd.to_datetime(f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))]
track3 = track3.loc[(pd.to_datetime(track3["timestr"].values) >= pd.to_datetime(f"{year:04d}-{month:02d}-01 00:00:00")) & (pd.to_datetime(track3["timestr"].values) <= pd.to_datetime(f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))]
track4 = track4.loc[(pd.to_datetime(track4["timestr"].values) >= pd.to_datetime(f"{year:04d}-{month:02d}-01 00:00:00")) & (pd.to_datetime(track4["timestr"].values) <= pd.to_datetime(f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))]
track5 = track5.loc[(pd.to_datetime(track5["timestr"].values) >= pd.to_datetime(f"{year:04d}-{month:02d}-01 00:00:00")) & (pd.to_datetime(track5["timestr"].values) <= pd.to_datetime(f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))]

grid_res = 0.1

lat = np.arange(-30.45, 30.45 + grid_res, grid_res)
lat = xr.DataArray(lat, dims=["lat"], coords={"lat": lat}, attrs={"long_name": "latitude", "units": "degrees_north"})

lon = np.arange(0.05, 359.95 + grid_res, grid_res)
lon = xr.DataArray(lon, dims=["lon"], coords={"lon": lon}, attrs={"long_name": "longitude", "units": "degrees_east"})

mask_moisture_mode = calc_circ_mask(track1, time, lat, lon, radius, grid_res)
mask_mixed_system = calc_circ_mask(track2, time, lat, lon, radius, grid_res)
mask_ig_wave = calc_circ_mask(track3, time, lat, lon, radius, grid_res)
mask_tc = calc_circ_mask(track4, time, lat, lon, radius, grid_res)
mask_anom = calc_circ_mask(track5, time, lat, lon, radius, grid_res)

all_contribution_moisture_mode = xr.where(mask_moisture_mode.values > 0, x_all, 0.0)
all_contribution_mixed_system = xr.where(mask_mixed_system.values > 0, x_all, 0.0)
all_contribution_ig_wave = xr.where(mask_ig_wave.values > 0, x_all, 0.0)
all_contribution_tc = xr.where(mask_tc.values > 0, x_all, 0.0)

all_contribution_anom = xr.where(mask_anom.values > 0, x_all, 0.0)

season_contribution_moisture_mode = xr.where(mask_moisture_mode.values > 0, x_season, 0.0)
season_contribution_mixed_system = xr.where(mask_mixed_system.values > 0, x_season, 0.0)
season_contribution_ig_wave = xr.where(mask_ig_wave.values > 0, x_season, 0.0)
season_contribution_tc = xr.where(mask_tc.values > 0, x_season, 0.0)

season_contribution_anom = xr.where(mask_anom.values > 0, x_season, 0.0)

dso = xr.Dataset()

dso[f"all_extreme_contribution_{var_name}_moisture_mode"] = all_contribution_moisture_mode
dso[f"all_extreme_contribution_{var_name}_mixed_system"] = all_contribution_mixed_system
dso[f"all_extreme_contribution_{var_name}_ig_wave"] = all_contribution_ig_wave
dso[f"all_extreme_contribution_{var_name}_tc"] = all_contribution_tc
dso[f"all_extreme_contribution_{var_name}_anom"] = all_contribution_anom

dso[f"season_extreme_contribution_{var_name}_moisture_mode"] = season_contribution_moisture_mode
dso[f"season_extreme_contribution_{var_name}_mixed_system"] = season_contribution_mixed_system
dso[f"season_extreme_contribution_{var_name}_ig_wave"] = season_contribution_ig_wave
dso[f"season_extreme_contribution_{var_name}_tc"] = season_contribution_tc
dso[f"season_extreme_contribution_{var_name}_anom"] = season_contribution_anom

dso.to_netcdf(path=fileo, format="NETCDF4")