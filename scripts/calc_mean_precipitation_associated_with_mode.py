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

year = int(os.environ.get("YEAR"))
diro = os.environ.get("DIRO")

files = sorted(glob(f"/scratch/k10/mr4682/data/GPM/3hr_mean_precipitation/precipitation.{year:04d}.nc"))

if len(files) == 1:
    ds = xr.open_dataset(files[0])
elif len(files) > 1:
    ds = xr.open_mfdataset(files)
else:
    sys.exit(f"No .nc files for year {year:04d}")

precipitation = ds["precipitation"]

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

olr_moisture_mode = xr.open_dataset(file_olr_moisture_mode)["txuptp_moisture_mode"].sel(time=slice(time_start, time_end))
olr_mixed_system = xr.open_dataset(file_olr_mixed_system)["txuptp_mixed_system"].sel(time=slice(time_start, time_end))
olr_ig_wave = xr.open_dataset(file_olr_ig_wave)["txuptp_ig_wave"].sel(time=slice(time_start, time_end))
olr_tc = xr.open_dataset(file_olr_tc)["txuptp_anom_tc"].sel(time=slice(time_start, time_end))
olr_anom = xr.open_dataset(file_olr_anom)["txuptp_anom"].sel(time=slice(time_start, time_end))

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

olr_moisture_mode = olr_moisture_mode.assign_attrs({"units": "standard deviation"})
olr_mixed_system = olr_mixed_system.assign_attrs({"units": "standard deviation"})
olr_ig_wave = olr_ig_wave.assign_attrs({"units": "standard deviation"})
olr_tc = olr_tc.assign_attrs({"units": "standard deviation"})
olr_anom = olr_anom.assign_attrs({"units": "standard deviation"})

print(olr_moisture_mode)
print(olr_mixed_system)
print(olr_ig_wave)
print(olr_tc)
print(olr_anom)

file_track_moisture_mode = f"{SCRATCH_TRACK_DIR}/track.txuptp.moisture.mode.1984-2016.csv"
file_track_mixed_system = f"{SCRATCH_TRACK_DIR}/track.txuptp.mixed.system.1984-2016.csv"
file_track_ig_wave = f"{SCRATCH_TRACK_DIR}/track.txuptp.ig.wave.1984-2016.csv"
file_track_tc = f"{SCRATCH_TRACK_DIR}/track.txuptp.tc.1984-2016.csv"
file_track_anom = f"{SCRATCH_TRACK_DIR}/track.txuptp.anom.1984-2016.csv"

track_moisture_mode = pd.read_csv(file_track_moisture_mode)
track_mixed_system = pd.read_csv(file_track_mixed_system)
track_ig_wave = pd.read_csv(file_track_ig_wave)
track_tc = pd.read_csv(file_track_tc)
track_anom = pd.read_csv(file_track_anom)

track_moisture_mode = track_cell.track.quality_control(track_moisture_mode, spd=8)
track_mixed_system = track_cell.track.quality_control(track_mixed_system, spd=8)
track_ig_wave = track_cell.track.quality_control(track_ig_wave, spd=8)
track_tc = track_cell.track.quality_control(track_tc, spd=8)
track_anom = track_cell.track.quality_control(track_anom, spd=8)

track_moisture_mode = track_moisture_mode.loc[pd.to_datetime(track_moisture_mode["timestr"].values).year == year]
track_mixed_system = track_mixed_system.loc[pd.to_datetime(track_mixed_system["timestr"].values).year == year]
track_ig_wave = track_ig_wave.loc[pd.to_datetime(track_ig_wave["timestr"].values).year == year]
track_tc = track_tc.loc[pd.to_datetime(track_tc["timestr"].values).year == year]
track_anom = track_anom.loc[pd.to_datetime(track_anom["timestr"].values).year == year]

track_moisture_mode.index = np.arange(0, len(track_moisture_mode.index), 1, dtype=int)
track_mixed_system.index = np.arange(0, len(track_mixed_system.index), 1, dtype=int)
track_ig_wave.index = np.arange(0, len(track_ig_wave.index), 1, dtype=int)
track_tc.index = np.arange(0, len(track_tc.index), 1, dtype=int)
track_anom.index = np.arange(0, len(track_anom.index), 1, dtype=int)

time = olr_anom["time"]
latitude = olr_anom["latitude"]
longitude = olr_anom["longitude"]

dxy = np.radians(longitude.values[1] - longitude.values[0]) * 6.371e+06
dt = (time.values[1] - time.values[0]) / np.timedelta64(1, "s")

parameters_segmentation = {}
parameters_segmentation["threshold"] = -2.0
parameters_segmentation["target"] = "minimum"
parameters_segmentation["PBC_flag"] = "hdim_2"

mask_moisture_mode, track_moisture_mode = tobac.segmentation_2D(track_moisture_mode, olr_moisture_mode, dxy=dxy, **parameters_segmentation)
mask_mixed_system, track_mixed_system = tobac.segmentation_2D(track_mixed_system, olr_mixed_system, dxy=dxy, **parameters_segmentation)
mask_ig_wave, track_ig_wave = tobac.segmentation_2D(track_ig_wave, olr_ig_wave, dxy=dxy, **parameters_segmentation)
mask_tc, track_tc = tobac.segmentation_2D(track_tc, olr_tc, dxy=dxy, **parameters_segmentation)
mask_anom, track_anom = tobac.segmentation_2D(track_anom, olr_anom, dxy=dxy, **parameters_segmentation)

print(track_moisture_mode)
print(track_mixed_system)
print(track_ig_wave)
print(track_tc)
print(track_anom)

total_precipitation = np.nansum(precipitation.values, axis=(1, 2))

total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc = np.zeros(len(time.values))

total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave = np.zeros(len(time.values))
total_precipitation_3_moisture_mode_and_mixed_system_and_tc = np.zeros(len(time.values))
total_precipitation_3_moisture_mode_and_ig_wave_and_tc = np.zeros(len(time.values))
total_precipitation_3_mixed_system_and_ig_wave_and_tc = np.zeros(len(time.values))

total_precipitation_2_moisture_mode_and_mixed_system = np.zeros(len(time.values))
total_precipitation_2_moisture_mode_and_ig_wave = np.zeros(len(time.values))
total_precipitation_2_moisture_mode_and_tc = np.zeros(len(time.values))
total_precipitation_2_mixed_system_and_ig_wave = np.zeros(len(time.values))
total_precipitation_2_mixed_system_and_tc = np.zeros(len(time.values))
total_precipitation_2_ig_wave_and_tc = np.zeros(len(time.values))

total_precipitation_moisture_mode = np.zeros(len(time.values))
total_precipitation_mixed_system = np.zeros(len(time.values))
total_precipitation_ig_wave = np.zeros(len(time.values))
total_precipitation_tc = np.zeros(len(time.values))
total_precipitation_anom = np.zeros(len(time.values))

for n in range(0, len(time.values)):
    for i in range(0, len(latitude.values)):
        lat_i = latitude.values[i]

        precipitation_i = precipitation[n, :, :].sel(lat=slice(lat_i-0.45, lat_i+0.45))

        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values > 0) & (mask_mixed_system[n, i, :].values > 0) & (mask_ig_wave[n, i, :].values > 0) & (mask_tc[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values > 0) & (mask_mixed_system[n, i, :].values > 0) & (mask_ig_wave[n, i, :].values > 0) & (mask_tc[n, i, :].values < 1)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values > 0) & (mask_mixed_system[n, i, :].values > 0) & (mask_ig_wave[n, i, :].values < 1) & (mask_tc[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_3_moisture_mode_and_mixed_system_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values > 0) & (mask_mixed_system[n, i, :].values < 1) & (mask_ig_wave[n, i, :].values > 0) & (mask_tc[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_3_moisture_mode_and_ig_wave_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values < 1) & (mask_mixed_system[n, i, :].values > 0) & (mask_ig_wave[n, i, :].values > 0) & (mask_tc[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_3_mixed_system_and_ig_wave_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values > 0) & (mask_mixed_system[n, i, :].values > 0) & (mask_ig_wave[n, i, :].values < 1) & (mask_tc[n, i, :].values < 1)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_moisture_mode_and_mixed_system[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values > 0) & (mask_mixed_system[n, i, :].values < 1) & (mask_ig_wave[n, i, :].values > 0) & (mask_tc[n, i, :].values < 1)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_moisture_mode_and_ig_wave[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values > 0) & (mask_mixed_system[n, i, :].values < 1) & (mask_ig_wave[n, i, :].values < 1) & (mask_tc[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_moisture_mode_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values < 1) & (mask_mixed_system[n, i, :].values > 0) & (mask_ig_wave[n, i, :].values > 0) & (mask_tc[n, i, :].values < 1)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_mixed_system_and_ig_wave[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values < 1) & (mask_mixed_system[n, i, :].values > 0) & (mask_ig_wave[n, i, :].values < 1) & (mask_tc[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_mixed_system_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values < 1) & (mask_mixed_system[n, i, :].values < 1) & (mask_ig_wave[n, i, :].values > 0) & (mask_tc[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_2_ig_wave_and_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values > 0) & (mask_mixed_system[n, i, :].values < 1) & (mask_ig_wave[n, i, :].values < 1) & (mask_tc[n, i, :].values < 1)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_moisture_mode[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values < 1) & (mask_mixed_system[n, i, :].values > 0) & (mask_ig_wave[n, i, :].values < 1) & (mask_tc[n, i, :].values < 1)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_mixed_system[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values < 1) & (mask_mixed_system[n, i, :].values < 1) & (mask_ig_wave[n, i, :].values > 0) & (mask_tc[n, i, :].values < 1)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_ig_wave[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)
        lon_i = longitude.values[(mask_moisture_mode[n, i, :].values < 1) & (mask_mixed_system[n, i, :].values < 1) & (mask_ig_wave[n, i, :].values < 1) & (mask_tc[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_tc[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

        lon_i = longitude.values[(mask_anom[n, i, :].values > 0)]
        for j in lon_i:
            if j > 180.0:
                j -= 360.0
            total_precipitation_anom[n] += np.nansum(precipitation_i.sel(lon=slice(j-0.45, j+0.45)).values)

total_precipitation = xr.DataArray(total_precipitation, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics", "units": "mm"})

total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc = xr.DataArray(total_precipitation_4_moisture_mode_and_mixed_system_and_ig_wave_and_tc, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, inertio-gravity waves, and tropical cyclones", "units": "mm"})

total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave = xr.DataArray(total_precipitation_3_moisture_mode_and_mixed_system_and_ig_wave, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, and inertio-gravity waves", "units": "mm"})
total_precipitation_3_moisture_mode_and_mixed_system_and_tc = xr.DataArray(total_precipitation_3_moisture_mode_and_mixed_system_and_tc, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes, mixed systems, and tropical cyclones", "units": "mm"})
total_precipitation_3_moisture_mode_and_ig_wave_and_tc = xr.DataArray(total_precipitation_3_moisture_mode_and_ig_wave_and_tc, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes, inertio-gravity waves, and tropical cyclones", "units": "mm"})
total_precipitation_3_mixed_system_and_ig_wave_and_tc = xr.DataArray(total_precipitation_3_mixed_system_and_ig_wave_and_tc, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with mixed systems, inertio-gravity waves, and tropical cyclones", "units": "mm"})

total_precipitation_2_moisture_mode_and_mixed_system = xr.DataArray(total_precipitation_2_moisture_mode_and_mixed_system, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes and mixed systems", "units": "mm"})
total_precipitation_2_moisture_mode_and_ig_wave = xr.DataArray(total_precipitation_2_moisture_mode_and_ig_wave, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes and inertio-gravity waves", "units": "mm"})
total_precipitation_2_moisture_mode_and_tc = xr.DataArray(total_precipitation_2_moisture_mode_and_tc, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes and tropical cyclones", "units": "mm"})
total_precipitation_2_mixed_system_and_ig_wave = xr.DataArray(total_precipitation_2_mixed_system_and_ig_wave, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with mixed systems and inertio-gravity waves", "units": "mm"})
total_precipitation_2_mixed_system_and_tc = xr.DataArray(total_precipitation_2_mixed_system_and_tc, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with mixed systems and tropical cyclones", "units": "mm"})
total_precipitation_2_ig_wave_and_tc = xr.DataArray(total_precipitation_2_ig_wave_and_tc, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with inertio-gravity waves and tropical cyclones", "units": "mm"})

total_precipitation_moisture_mode = xr.DataArray(total_precipitation_moisture_mode, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with moisture modes", "units": "mm"})
total_precipitation_mixed_system = xr.DataArray(total_precipitation_mixed_system, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with mixed systems", "units": "mm"})
total_precipitation_ig_wave = xr.DataArray(total_precipitation_ig_wave, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with inertio-gravity waves", "units": "mm"})
total_precipitation_tc = xr.DataArray(total_precipitation_tc, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with tropical cyclones", "units": "mm"})
total_precipitation_anom = xr.DataArray(total_precipitation_anom, dims=["time"], coords={"time": time}, attrs={"long_name": "Total precipitation in the tropics associated with strong OLR anomalies", "units": "mm"})

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

fileo = f"{diro}precipitation.a.w.mode.{year:04d}.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
