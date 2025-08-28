import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

year = int(os.environ.get("YEAR"))
month = int(os.environ.get("MONTH"))
threshold = float(os.environ.get("THRESHOLD"))
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

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_mean_precipitation/precipitation.{year:04d}.nc"))

if len(files) > 0:
    ds = xr.open_dataset(files[0])
elif len(files) > 1:
    ds = xr.open_mfdataset(files)
else:
    sys.exit(f"No .nc files for year {year:04d}")

month_length = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

if year % 4 == 0:
    month_length[2] = 29

time_start = f"{year:04d}-{month:02d}-01 00:00:00"
time_end = f"{year:04d}-{month:02d}-{month_length[month]:02d} 21:00:00"

precipitation = ds["precipitation"].sel(time=slice(time_start, time_end))
precipitation = precipitation.transpose("time", "lat", "lon")
precipitation = convert_lon_180_to_360(precipitation, "lon")

precipitation = xr.where(precipitation >= threshold, precipitation, 0.0, keep_attrs=True)

precipitation *= 3.0
precipitation = precipitation.drop_attrs()
precipitation = precipitation.assign_attrs({"long_name": "3-hourly precipitation accumulation", "units": "mm"})

precipitation = precipitation[:, (precipitation["lat"].values >= -20.05) & (precipitation["lat"].values <= 20.05), :]

print(precipitation)

file_mask_moisture_mode = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.moisture.mode.{year:04d}.nc"
file_mask_mixed_system = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.mixed.system.{year:04d}.nc"
file_mask_ig_wave = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.ig.wave.{year:04d}.nc"
file_mask_tc = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.tc.{year:04d}.nc"
file_mask_anom = f"{SCRATCH_TRACK_DIR}/mask/mask.track.txuptp.anom.{year:04d}.nc"

mask_moisture_mode = xr.open_dataset(file_mask_moisture_mode)["mask_track_txuptp_moisture_mode"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))
mask_mixed_system = xr.open_dataset(file_mask_mixed_system)["mask_track_txuptp_mixed_system"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))
mask_ig_wave = xr.open_dataset(file_mask_ig_wave)["mask_track_txuptp_ig_wave"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))
mask_tc = xr.open_dataset(file_mask_tc)["mask_track_txuptp_anom_tc"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))
mask_anom = xr.open_dataset(file_mask_anom)["mask_track_txuptp_anom"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))

mask_moisture_mode = calc_mask_0p1(mask_moisture_mode)
mask_mixed_system = calc_mask_0p1(mask_mixed_system)
mask_ig_wave = calc_mask_0p1(mask_ig_wave)
mask_tc = calc_mask_0p1(mask_tc)
mask_anom = calc_mask_0p1(mask_anom)

mask_moisture_mode = mask_moisture_mode[:, (mask_moisture_mode["latitude"].values > -20.1) & (mask_moisture_mode["latitude"].values < 20.1), :]
mask_mixed_system = mask_mixed_system[:, (mask_mixed_system["latitude"].values > -20.1) & (mask_mixed_system["latitude"].values < 20.1), :]
mask_ig_wave = mask_ig_wave[:, (mask_ig_wave["latitude"].values > -20.1) & (mask_ig_wave["latitude"].values < 20.1), :]
mask_tc = mask_tc[:, (mask_tc["latitude"].values > -20.1) & (mask_tc["latitude"].values < 20.1), :]
mask_anom = mask_anom[:, (mask_anom["latitude"].values > -20.1) & (mask_anom["latitude"].values < 20.1), :]

print(mask_moisture_mode)
print(mask_mixed_system)
print(mask_ig_wave)
print(mask_tc)
print(mask_anom)

total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc = xr.where((mask_moisture_mode.values > 0) & (mask_mixed_system.values > 0) & (mask_ig_wave.values > 0) & (mask_tc.values > 0), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)

total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave = xr.where((mask_moisture_mode.values > 0) & (mask_mixed_system.values > 0) & (mask_ig_wave.values > 0) & (mask_tc.values < 1), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_3_moisture_mode_and_mixed_system_and_tc = xr.where((mask_moisture_mode.values > 0) & (mask_mixed_system.values > 0) & (mask_ig_wave.values < 1) & (mask_tc.values > 0), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_3_moisture_mode_and_ig_wave_and_tc = xr.where((mask_moisture_mode.values > 0) & (mask_mixed_system.values < 1) & (mask_ig_wave.values > 0) & (mask_tc.values > 0), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_3_mixed_system_and_ig_wave_and_tc = xr.where((mask_moisture_mode.values < 1) & (mask_mixed_system.values > 0) & (mask_ig_wave.values > 0) & (mask_tc.values > 0), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)

total_precipitation_2_moisture_mode_and_mixed_system = xr.where((mask_moisture_mode.values > 0) & (mask_mixed_system.values > 0) & (mask_ig_wave.values < 1) & (mask_tc.values < 1), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_2_moisture_mode_and_ig_wave = xr.where((mask_moisture_mode.values > 0) & (mask_mixed_system.values < 1) & (mask_ig_wave.values > 0) & (mask_tc.values < 1), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_2_moisture_mode_and_tc = xr.where((mask_moisture_mode.values > 0) & (mask_mixed_system.values < 1) & (mask_ig_wave.values < 1) & (mask_tc.values > 0), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_2_mixed_system_and_ig_wave = xr.where((mask_moisture_mode.values < 1) & (mask_mixed_system.values > 0) & (mask_ig_wave.values > 0) & (mask_tc.values < 1), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_2_mixed_system_and_tc = xr.where((mask_moisture_mode.values < 1) & (mask_mixed_system.values > 0) & (mask_ig_wave.values < 1) & (mask_tc.values > 0), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_2_ig_wave_and_tc = xr.where((mask_moisture_mode.values < 1) & (mask_mixed_system.values < 1) & (mask_ig_wave.values > 0) & (mask_tc.values > 0), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)

total_precipitation_moisture_mode = xr.where((mask_moisture_mode.values > 0) & (mask_mixed_system.values < 1) & (mask_ig_wave.values < 1) & (mask_tc.values < 1), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_mixed_system = xr.where((mask_moisture_mode.values < 1) & (mask_mixed_system.values > 0) & (mask_ig_wave.values < 1) & (mask_tc.values < 1), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_ig_wave = xr.where((mask_moisture_mode.values < 1) & (mask_mixed_system.values < 1) & (mask_ig_wave.values > 0) & (mask_tc.values < 1), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)
total_precipitation_tc = xr.where((mask_moisture_mode.values < 1) & (mask_mixed_system.values < 1) & (mask_ig_wave.values < 1) & (mask_tc.values > 0), precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)

total_precipitation_anom = xr.where(mask_anom.values > 0, precipitation, np.nan).sum(dim=["lon", "lat"], skipna=True)

total_precipitation = precipitation.sum(dim=["lon", "lat"], skipna=True)

total_precipitation = total_precipitation.assign_attrs({"long_name": "Total precipitation in the tropics", "units": "mm"})

total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc = total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc.assign_attrs({"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, inertio-gravity waves, and tropical cyclones", "units": "mm"})

total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave = total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave.assign_attrs({"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, and inertio-gravity waves", "units": "mm"})
total_precipitation_3_moisture_mode_and_mixed_system_and_tc = total_precipitation_3_moisture_mode_and_mixed_system_and_tc.assign_attrs({"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, and tropical cyclones", "units": "mm"})
total_precipitation_3_moisture_mode_and_ig_wave_and_tc = total_precipitation_3_moisture_mode_and_ig_wave_and_tc.assign_attrs({"long_name": "Total precipitation in the tropics associated with moisture modes, inertio-gravity waves, and tropical cyclones", "units": "mm"})
total_precipitation_3_mixed_system_and_ig_wave_and_tc = total_precipitation_3_mixed_system_and_ig_wave_and_tc.assign_attrs({"long_name": "Total precipitation in the tropics associated with mixed systems, inertio-gravity waves, and tropical cyclones", "units": "mm"})

total_precipitation_2_moisture_mode_and_mixed_system = total_precipitation_2_moisture_mode_and_mixed_system.assign_attrs({"long_name": "Total precipitation in the tropics associated with moisture modes and mixed systems", "units": "mm"})
total_precipitation_2_moisture_mode_and_ig_wave = total_precipitation_2_moisture_mode_and_ig_wave.assign_attrs({"long_name": "Total precipitation in the tropics associated with moisture modes and inertio-gravity waves", "units": "mm"})
total_precipitation_2_moisture_mode_and_tc = total_precipitation_2_moisture_mode_and_tc.assign_attrs({"long_name": "Total precipitation in the tropics associated with moisture modes and tropical cyclones", "units": "mm"})
total_precipitation_2_mixed_system_and_ig_wave = total_precipitation_2_mixed_system_and_ig_wave.assign_attrs({"long_name": "Total precipitation in the tropics associated with mixed systems and inertio-gravity waves", "units": "mm"})
total_precipitation_2_mixed_system_and_tc = total_precipitation_2_mixed_system_and_tc.assign_attrs({"long_name": "Total precipitation in the tropics associated with mixed systems and tropical cyclones", "units": "mm"})
total_precipitation_2_ig_wave_and_tc = total_precipitation_2_ig_wave_and_tc.assign_attrs({"long_name": "Total precipitation in the tropics associated with inertio-gravity waves and tropical cyclones", "units": "mm"})

total_precipitation_moisture_mode = total_precipitation_moisture_mode.assign_attrs({"long_name": "Total precipitation in the tropics associated with moisture modes", "units": "mm"})
total_precipitation_mixed_system = total_precipitation_mixed_system.assign_attrs({"long_name": "Total precipitation in the tropics associated with mixed systems", "units": "mm"})
total_precipitation_ig_wave = total_precipitation_ig_wave.assign_attrs({"long_name": "Total precipitation in the tropics associated with inertio-gravity waves", "units": "mm"})
total_precipitation_tc = total_precipitation_tc.assign_attrs({"long_name": "Total precipitation in the tropics associated with tropical cyclones", "units": "mm"})

total_precipitation_anom = total_precipitation_anom.assign_attrs({"long_name": "Total precipitation in the tropics associated with strong OLR anomalies", "units": "mm"})

print(f"Total precipitation in the tropics:                {np.nansum(total_precipitation.values)} mm")
print(f"Associated with moisture modes:                    {np.nansum(total_precipitation_moisture_mode.values)} mm")
print(f"Associated with mixed systems:                     {np.nansum(total_precipitation_mixed_system.values)} mm")
print(f"Associated with inertio-gravity waves:             {np.nansum(total_precipitation_ig_wave.values)} mm")
print(f"Associated with tropical cyclones:                 {np.nansum(total_precipitation_tc.values)} mm")
print(f"Associated with strong OLR anomalies:              {np.nansum(total_precipitation_anom.values)} mm")

dso = xr.Dataset()

dso["precipitation"] = total_precipitation

dso["precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc"] = total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc

dso["precipitation_3_moisture_mode_and_mixed_system_and_ig_wave"] = total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave
dso["precipitation_3_moisture_mode_and_mixed_system_and_tc"] = total_precipitation_3_moisture_mode_and_mixed_system_and_tc
dso["precipitation_3_moisture_mode_and_ig_wave_and_tc"] = total_precipitation_3_moisture_mode_and_ig_wave_and_tc
dso["precipitation_3_mixed_system_and_ig_wave_and_tc"] = total_precipitation_3_mixed_system_and_ig_wave_and_tc

dso["precipitation_2_moisture_mode_and_mixed_system"] = total_precipitation_2_moisture_mode_and_mixed_system
dso["precipitation_2_moisture_mode_and_ig_wave"] = total_precipitation_2_moisture_mode_and_ig_wave
dso["precipitation_2_moisture_mode_and_tc"] = total_precipitation_2_moisture_mode_and_tc
dso["precipitation_2_mixed_system_and_ig_wave"] = total_precipitation_2_mixed_system_and_ig_wave
dso["precipitation_2_mixed_system_and_tc"] = total_precipitation_2_mixed_system_and_tc
dso["precipitation_2_ig_wave_and_tc"] = total_precipitation_2_ig_wave_and_tc

dso["precipitation_moisture_mode"] = total_precipitation_moisture_mode
dso["precipitation_mixed_system"] = total_precipitation_mixed_system
dso["precipitation_ig_wave"] = total_precipitation_ig_wave
dso["precipitation_tc"] = total_precipitation_tc

dso["precipitation_anom"] = total_precipitation_anom

fileo = f"{diro}precipitation.in.deep.tropics.a.w.mode.w.t.{threshold:.2f}.{year:04d}{month:02d}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
