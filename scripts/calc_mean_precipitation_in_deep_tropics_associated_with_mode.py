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

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_mean_precipitation/precipitation.{year:04d}.nc"))

if len(files) == 1:
    ds = xr.open_dataset(files[0])
elif len(files) > 1:
    ds = xr.open_mfdataset(files)
else:
    sys.exit(f"No .nc files for year {year:04d}")

precipitation = ds["precipitation"].sel(lat=slice(-20.5, 20.5))

precipitation *= 3.0
precipitation = precipitation.assign_attrs({"units": "mm"})

print(precipitation)

file_olr_moisture_mode = f"{SCRATCH_ISCCP_DIR}/txuptp/filtered/txuptp.moisture.mode.1984-2016.nc"
file_olr_mixed_system = f"{SCRATCH_ISCCP_DIR}/txuptp/filtered/txuptp.mixed.system.1984-2016.nc"
file_olr_ig_wave = f"{SCRATCH_ISCCP_DIR}/txuptp/filtered/txuptp.ig.wave.1984-2016.nc"
file_olr_tc = f"{SCRATCH_ISCCP_DIR}/txuptp/filtered/txuptp.tc.1984-2016.nc"
file_olr_anom = f"{SCRATCH_ISCCP_DIR}/txuptp/txuptp.clim.and.anom.1984-2016.nc"

if year == 2000:
    time_start = f"{year:04d}-06-01 00:00:00"
else:
    time_start = f"{year:04d}-01-01 00:00:00"

time_end = f"{year:04d}-12-31 21:00:00"

olr_moisture_mode = xr.open_dataset(file_olr_moisture_mode)["txuptp_moisture_mode"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))
olr_mixed_system = xr.open_dataset(file_olr_mixed_system)["txuptp_mixed_system"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))
olr_ig_wave = xr.open_dataset(file_olr_ig_wave)["txuptp_ig_wave"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))
olr_tc = xr.open_dataset(file_olr_tc)["txuptp_anom_tc"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))
olr_anom = xr.open_dataset(file_olr_anom)["txuptp_anom"].sel(time=slice(time_start, time_end), latitude=slice(-20.5, 20.5))

stddev_olr_moisture_mode = xr.open_dataset(f"{SCRATCH_ISCCP_DIR}/txuptp/stddev.txuptp.moisture.mode.1984-2016.nc")["stddev_txuptp_moisture_mode"].values
stddev_olr_mixed_system = xr.open_dataset(f"{SCRATCH_ISCCP_DIR}/txuptp/stddev.txuptp.mixed.system.1984-2016.nc")["stddev_txuptp_mixed_system"].values
stddev_olr_ig_wave = xr.open_dataset(f"{SCRATCH_ISCCP_DIR}/txuptp/stddev.txuptp.ig.wave.1984-2016.nc")["stddev_txuptp_ig_wave"].values
stddev_olr_tc = xr.open_dataset(f"{SCRATCH_ISCCP_DIR}/txuptp/stddev.txuptp.tc.1984-2016.nc")["stddev_txuptp_anom_tc"].values
stddev_olr_anom = xr.open_dataset(f"{SCRATCH_ISCCP_DIR}/txuptp/stddev.txuptp.anom.1984-2016.nc")["stddev_txuptp_anom"].values

olr_moisture_mode /= stddev_olr_moisture_mode
olr_mixed_system /= stddev_olr_mixed_system
olr_ig_wave /= stddev_olr_ig_wave
olr_tc /= stddev_olr_tc
olr_anom /= stddev_olr_anom

print(olr_moisture_mode)
print(olr_mixed_system)
print(olr_ig_wave)
print(olr_tc)
print(olr_anom)

total_precipitation = np.nansum(precipitation.values, axis=(1, 2))

total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc = np.zeros(len(precipitation["time"].values))

total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave = np.zeros(len(precipitation["time"].values))
total_precipitation_3_moisture_mode_and_mixed_system_and_tc = np.zeros(len(precipitation["time"].values))
total_precipitation_3_moisture_mode_and_ig_wave_and_tc = np.zeros(len(precipitation["time"].values))
total_precipitation_3_mixed_system_and_ig_wave_and_tc = np.zeros(len(precipitation["time"].values))

total_precipitation_2_moisture_mode_and_mixed_system = np.zeros(len(precipitation["time"].values))
total_precipitation_2_moisture_mode_and_ig_wave = np.zeros(len(precipitation["time"].values))
total_precipitation_2_moisture_mode_and_tc = np.zeros(len(precipitation["time"].values))
total_precipitation_2_mixed_system_and_ig_wave = np.zeros(len(precipitation["time"].values))
total_precipitation_2_mixed_system_and_tc = np.zeros(len(precipitation["time"].values))
total_precipitation_2_ig_wave_and_tc = np.zeros(len(precipitation["time"].values))

total_precipitation_moisture_mode = np.zeros(len(precipitation["time"].values))
total_precipitation_mixed_system = np.zeros(len(precipitation["time"].values))
total_precipitation_ig_wave = np.zeros(len(precipitation["time"].values))
total_precipitation_tc = np.zeros(len(precipitation["time"].values))
total_precipitation_anom = np.zeros(len(precipitation["time"].values))

for n in range(0, len(precipitation["time"].values)):
    for i in range(0, len(olr_anom["latitude"].values)):
        lat_i = olr_anom["latitude"].values[i]

        precipitation_i = precipitation[n, :, :].sel(lat=slice(lat_i-0.45, lat_i+0.45))

        lon_i = olr_anom["longitude"].values[(olr_moisture_mode[n, i, :].values < -2.0) & (olr_mixed_system[n, i, :].values < -2.0) & (olr_ig_wave[n, i, :].values < -2.0) & (olr_tc[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

        lon_i = olr_anom["longitude"].values[(olr_moisture_mode[n, i, :].values < -2.0) & (olr_mixed_system[n, i, :].values < -2.0) & (olr_ig_wave[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_moisture_mode[n, i, :].values < -2.0) & (olr_mixed_system[n, i, :].values < -2.0) & (olr_tc[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_3_moisture_mode_and_mixed_system_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_moisture_mode[n, i, :].values < -2.0) & (olr_ig_wave[n, i, :].values < -2.0) & (olr_tc[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_3_moisture_mode_and_ig_wave_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_mixed_system[n, i, :].values < -2.0) & (olr_ig_wave[n, i, :].values < -2.0) & (olr_tc[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_3_mixed_system_and_ig_wave_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

        lon_i = olr_anom["longitude"].values[(olr_moisture_mode[n, i, :].values < -2.0) & (olr_mixed_system[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_moisture_mode_and_mixed_system[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_moisture_mode[n, i, :].values < -2.0) & (olr_ig_wave[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_moisture_mode_and_ig_wave[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_moisture_mode[n, i, :].values < -2.0) & (olr_tc[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_moisture_mode_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_mixed_system[n, i, :].values < -2.0) & (olr_ig_wave[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_mixed_system_and_ig_wave[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_mixed_system[n, i, :].values < -2.0) & (olr_tc[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_mixed_system_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_ig_wave[n, i, :].values < -2.0) & (olr_tc[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_ig_wave_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

        lon_i = olr_anom["longitude"].values[(olr_moisture_mode[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_moisture_mode[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_mixed_system[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_mixed_system[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_ig_wave[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_ig_wave[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = olr_anom["longitude"].values[(olr_tc[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

        lon_i = olr_anom["longitude"].values[(olr_anom[n, i, :].values < -2.0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_anom[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

total_precipitation = xr.DataArray(total_precipitation, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics", "units": "mm"})

total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc = xr.DataArray(total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, inertio-gravity waves, and tropical cyclones", "units": "mm"})

total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave = xr.DataArray(total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, and inertio-gravity waves", "units": "mm"})
total_precipitation_3_moisture_mode_and_mixed_system_and_tc = xr.DataArray(total_precipitation_3_moisture_mode_and_mixed_system_and_tc, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, and tropical cyclones", "units": "mm"})
total_precipitation_3_moisture_mode_and_ig_wave_and_tc = xr.DataArray(total_precipitation_3_moisture_mode_and_ig_wave_and_tc, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes, inertio-gravity waves, and tropical cyclones", "units": "mm"})
total_precipitation_3_mixed_system_and_ig_wave_and_tc = xr.DataArray(total_precipitation_3_mixed_system_and_ig_wave_and_tc, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with mixed systems, inertio-gravity waves, and tropical cyclones", "units": "mm"})

total_precipitation_2_moisture_mode_and_mixed_system = xr.DataArray(total_precipitation_2_moisture_mode_and_mixed_system, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes and mixed systems", "units": "mm"})
total_precipitation_2_moisture_mode_and_ig_wave = xr.DataArray(total_precipitation_2_moisture_mode_and_ig_wave, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes and inertio-gravity waves", "units": "mm"})
total_precipitation_2_moisture_mode_and_tc = xr.DataArray(total_precipitation_2_moisture_mode_and_tc, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes and tropical cyclones", "units": "mm"})
total_precipitation_2_mixed_system_and_ig_wave = xr.DataArray(total_precipitation_2_mixed_system_and_ig_wave, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with mixed systems and inertio-gravity waves", "units": "mm"})
total_precipitation_2_mixed_system_and_tc = xr.DataArray(total_precipitation_2_mixed_system_and_tc, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with mixed systems and tropical cyclones", "units": "mm"})
total_precipitation_2_ig_wave_and_tc = xr.DataArray(total_precipitation_2_ig_wave_and_tc, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with inertio-gravity waves and tropical cyclones", "units": "mm"})

total_precipitation_moisture_mode = xr.DataArray(total_precipitation_moisture_mode, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes", "units": "mm"})
total_precipitation_mixed_system = xr.DataArray(total_precipitation_mixed_system, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with mixed systems", "units": "mm"})
total_precipitation_ig_wave = xr.DataArray(total_precipitation_ig_wave, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with inertio-gravity waves", "units": "mm"})
total_precipitation_tc = xr.DataArray(total_precipitation_tc, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with tropical cyclones", "units": "mm"})
total_precipitation_anom = xr.DataArray(total_precipitation_anom, dims=["time"], coords={"time": precipitation["time"]}, attrs={"long_name": "Total precipitation in the tropics associated with strong OLR anomalies", "units": "mm"})

print(f"Total precipitation in the deep tropics:                {np.nansum(total_precipitation.values)} mm")
print(f"Associated with moisture modes:                         {np.nansum(total_precipitation_moisture_mode.values)} mm")
print(f"Associated with mixed systems:                          {np.nansum(total_precipitation_mixed_system.values)} mm")
print(f"Associated with inertio-gravity waves:                  {np.nansum(total_precipitation_ig_wave.values)} mm")
print(f"Associated with tropical cyclones:                      {np.nansum(total_precipitation_tc.values)} mm")
print(f"Associated with strong OLR anomalies:                   {np.nansum(total_precipitation_anom.values)} mm")

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
