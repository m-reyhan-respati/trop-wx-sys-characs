import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import os
import sys

from config import *
from util import *

year = int(os.environ.get("YEAR"))
diro = os.environ.get("DIRO")

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_mean_precipitation/1p0deg/precipitation.{year:04d}.nc"))

if len(files) > 0:
    ds = xr.open_dataset(files[0])
elif len(files) > 1:
    ds = xr.open_mfdataset(files)
else:
    sys.exit(f"No .nc files for year {year:04d}")

precipitation = ds["precipitation"]
precipitation = precipitation.transpose("time", "lat", "lon")
precipitation = precipitation[:, (precipitation["lat"].values > -21.0) & (precipitation["lat"].values < 21.0), :]

print(precipitation)

if year == 2000:
    time_start = f"{year:04d}-06-01 00:00:00"
else:
    time_start = f"{year:04d}-01-01 00:00:00"

time_end = f"{year:04d}-12-31 21:00:00"

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

fileo = f"{diro}precipitation.in.deep.tropics.a.w.mode.{year:04d}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
