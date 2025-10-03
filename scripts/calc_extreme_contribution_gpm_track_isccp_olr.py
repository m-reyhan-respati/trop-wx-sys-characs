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
raining = int(os.environ.get("RAINING"))
diro = os.environ.get("DIRO")

def calc_mask_0p1(mask):
    lon_min = np.min(mask["longitude"].values) - 0.45
    lon_max = np.max(mask["longitude"].values) + 0.45
    
    lat_min = np.min(mask["latitude"].values) - 0.45
    lat_max = np.max(mask["latitude"].values) + 0.45
    
    lon = np.arange(lon_min, lon_max + 0.1, 0.1)
    lon = xr.DataArray(lon, dims=["longitude"], coords={"longitude": lon}, attrs=mask["longitude"].attrs)
    
    lat = np.arange(lat_min, lat_max + 0.1, 0.1)
    lat = xr.DataArray(lat, dims=["latitude"], coords={"latitude": lat}, attrs=mask["latitude"].attrs)
    
    mask_0p1 = np.zeros((mask.shape[0], len(lat.values), len(lon.values)))
    
    for i in range(0, 10):
        for j in range(0, 10):
            mask_0p1[:, j::10, i::10] = mask.values
    
    mask_0p1 = xr.DataArray(mask_0p1, dims=["time", "latitude", "longitude"], coords={"time": mask["time"], "latitude": lat, "longitude": lon}, attrs=mask.attrs)
    mask_0p1 = mask_0p1[:, (lat.values > np.min(mask["latitude"].values)) & (lat.values < np.max(mask["latitude"].values)), :]
    
    return mask_0p1

files = sorted(glob(f"{diri}{file_name}.*.nc"))

if len(files) == 0:
    sys.exit(f"No .nc file for {diri}{file_name}")

if raining == 1:
    files_p = sorted(glob(f"{diri}percentiles/raining/{file_name}.{percentile:d}p.*.nc"))
    fileo = f"{diro}raining/extreme.contribution.{percentile:d}p.{file_name}.{year:04d}{month:02d}.nc"
else:
    files_p = sorted(glob(f"{diri}percentiles/{file_name}.{percentile:d}p.*.nc"))
    fileo = f"{diro}extreme.contribution.{percentile:d}p.{file_name}.{year:04d}{month:02d}.nc"

month_length = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

if year % 4 == 0:
    month_length[2] = 29

x = xr.open_mfdataset(files)[f"{var_name}"].sel(time=slice(f"{year:04d}-{month:02d}-01 00:00:00", f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00")).compute()
x = x.transpose("time", "lat", "lon")
x = convert_lon_180_to_360(x, "lon")

if var_name == "precipitation":
    x *= 3.0
    x = x.assign_attrs({"units": "mm"})

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

file_mask_moisture_mode = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.moisture.mode.{year:04d}.nc"
file_mask_mixed_system = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.mixed.system.{year:04d}.nc"
file_mask_ig_wave = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.ig.wave.{year:04d}.nc"
file_mask_tc = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.tc.{year:04d}.nc"
file_mask_anom = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.anom.{year:04d}.nc"

mask_moisture_mode = xr.open_dataset(file_mask_moisture_mode)["mask_track_txuptp_moisture_mode"].sel(time=slice(f"{year:04d}-{month:02d}-01 00:00:00", f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))
mask_mixed_system = xr.open_dataset(file_mask_mixed_system)["mask_track_txuptp_mixed_system"].sel(time=slice(f"{year:04d}-{month:02d}-01 00:00:00", f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))
mask_ig_wave = xr.open_dataset(file_mask_ig_wave)["mask_track_txuptp_ig_wave"].sel(time=slice(f"{year:04d}-{month:02d}-01 00:00:00", f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))
mask_tc = xr.open_dataset(file_mask_tc)["mask_track_txuptp_anom_tc"].sel(time=slice(f"{year:04d}-{month:02d}-01 00:00:00", f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))
mask_anom = xr.open_dataset(file_mask_anom)["mask_track_txuptp_anom"].sel(time=slice(f"{year:04d}-{month:02d}-01 00:00:00", f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"))

mask_moisture_mode = calc_mask_0p1(mask_moisture_mode)
mask_mixed_system = calc_mask_0p1(mask_mixed_system)
mask_ig_wave = calc_mask_0p1(mask_ig_wave)
mask_tc = calc_mask_0p1(mask_tc)
mask_anom = calc_mask_0p1(mask_anom)

mask_moisture_mode = mask_moisture_mode[:, (mask_moisture_mode["latitude"].values > -30.5) & (mask_moisture_mode["latitude"].values < 30.5), :]
mask_mixed_system = mask_mixed_system[:, (mask_mixed_system["latitude"].values > -30.5) & (mask_mixed_system["latitude"].values < 30.5), :]
mask_ig_wave = mask_ig_wave[:, (mask_ig_wave["latitude"].values > -30.5) & (mask_ig_wave["latitude"].values < 30.5), :]
mask_tc = mask_tc[:, (mask_tc["latitude"].values > -30.5) & (mask_tc["latitude"].values < 30.5), :]
mask_anom = mask_anom[:, (mask_anom["latitude"].values > -30.5) & (mask_anom["latitude"].values < 30.5), :]

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