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
box_size = float(os.environ.get("BOX_SIZE"))
diro = os.environ.get("DIRO")

def calc_box_mask(track, time, lat, lon, x_size, y_size, grid_res):
    lat3 = np.zeros(len(lat.values) + int(2 * y_size / grid_res))
    lat3[0:int(y_size / grid_res)] = np.min(lat.values) + np.arange(-y_size, 0.0 - grid_res, grid_res)
    lat3[int(y_size / grid_res):-int(y_size / grid_res)] = lat.values
    lat3[-int(y_size / grid_res):] = np.max(lat.values) + np.arange(grid_res, y_size, grid_res)
    
    lon3 = np.zeros(len(lon.values) * 3)
    lon3[0:len(lon.values)] = lon.values - 360.0
    lon3[len(lon.values):-len(lon.values)] = lon.values
    lon3[-len(lon.values):] = lon.values + 360.0
    
    box_mask = np.zeros((len(time.values), len(lat3), len(lon3)), dtype=int)
    
    for t in range(0, len(time.values)):
        track_t = track.loc[pd.to_datetime(track["timestr"].values) == pd.to_datetime(time.values[t])]
        
        for i in range(0, len(track_t["feature"].values)):
            lat_i = track_t["latitude"].values[i]
            lon_i = track_t["longitude"].values[i]
            
            lat_i_index = np.argmin(np.abs(lat3 - lat_i))
            lon_i_index = np.argmin(np.abs(lon3 - lon_i))
            
            box_mask[t, lat_i_index - int(y_size / grid_res):lat_i_index + int(y_size / grid_res) + 1, lon_i_index - int(x_size / grid_res):lon_i_index + int(x_size / grid_res) + 1] += 1
    
    box_mask = box_mask[:, int(y_size / grid_res):-int(y_size / grid_res), :]
    box_mask = box_mask[:, :, 0:len(lon.values)] + box_mask[:, :, len(lon.values):-len(lon.values)] + box_mask[:, :, -len(lon.values):]
    box_mask = np.where(box_mask > 0, 1, box_mask)
    
    box_mask = xr.DataArray(box_mask, dims=["time", "lat", "lon"], coords={"time": time, "lat": lat, "lon": lon})
    
    return box_mask

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

track1 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/track.txuptp.moisture.mode.1984-2016.csv")
track2 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/track.txuptp.mixed.system.1984-2016.csv")
track3 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/track.txuptp.ig.wave.1984-2016.csv")
track4 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/track.txuptp.tc.1984-2016.csv")
track5 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/track.txuptp.anom.1984-2016.csv")

max_activity1 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/max.activity.track.txuptp.moisture.mode.1984-2016.csv")
max_activity2 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/max.activity.track.txuptp.mixed.system.1984-2016.csv")
max_activity3 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/max.activity.track.txuptp.ig.wave.1984-2016.csv")
max_activity4 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/max.activity.track.txuptp.tc.1984-2016.csv")
max_activity5 = pd.read_csv("/scratch/k10/mr4682/data/track_isccp_olr/max.activity.track.txuptp.anom.1984-2016.csv")

track1 = track1.loc[np.isin(track1["cell"].values, max_activity1["cell"].values)]
track2 = track2.loc[np.isin(track2["cell"].values, max_activity2["cell"].values)]
track3 = track3.loc[np.isin(track3["cell"].values, max_activity3["cell"].values)]
track4 = track4.loc[np.isin(track4["cell"].values, max_activity4["cell"].values)]
track5 = track5.loc[np.isin(track5["cell"].values, max_activity5["cell"].values)]

track1 = track1.loc[(pd.to_datetime(track1["timestr"].values) >= pd.to_datetime(time_start)) & (pd.to_datetime(track1["timestr"].values) <= pd.to_datetime(time_end))]
track2 = track2.loc[(pd.to_datetime(track2["timestr"].values) >= pd.to_datetime(time_start)) & (pd.to_datetime(track2["timestr"].values) <= pd.to_datetime(time_end))]
track3 = track3.loc[(pd.to_datetime(track3["timestr"].values) >= pd.to_datetime(time_start)) & (pd.to_datetime(track3["timestr"].values) <= pd.to_datetime(time_end))]
track4 = track4.loc[(pd.to_datetime(track4["timestr"].values) >= pd.to_datetime(time_start)) & (pd.to_datetime(track4["timestr"].values) <= pd.to_datetime(time_end))]
track5 = track5.loc[(pd.to_datetime(track5["timestr"].values) >= pd.to_datetime(time_start)) & (pd.to_datetime(track5["timestr"].values) <= pd.to_datetime(time_end))]

time = precipitation["time"]
lat = precipitation["lat"]
lon = precipitation["lon"]

grid_res = lon.values[1] - lon.values[0]

x_size = box_size
y_size = box_size

mask_moisture_mode = calc_box_mask(track1, time, lat, lon, x_size, y_size, grid_res)[:, (lat.values >= -20.05) & (lat.values <= 20.05), :]
mask_mixed_system = calc_box_mask(track2, time, lat, lon, x_size, y_size, grid_res)[:, (lat.values >= -20.05) & (lat.values <= 20.05), :]
mask_ig_wave = calc_box_mask(track3, time, lat, lon, x_size, y_size, grid_res)[:, (lat.values >= -20.05) & (lat.values <= 20.05), :]
mask_tc = calc_box_mask(track4, time, lat, lon, x_size, y_size, grid_res)[:, (lat.values >= -20.05) & (lat.values <= 20.05), :]
mask_anom = calc_box_mask(track5, time, lat, lon, x_size, y_size, grid_res)[:, (lat.values >= -20.05) & (lat.values <= 20.05), :]

precipitation = precipitation[:, (lat.values >= -20.05) & (lat.values <= 20.05), :]

print(precipitation)

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

fileo = f"{diro}precipitation.in.deep.tropics.a.w.mode.w.t.{threshold:.2f}.{year:04d}{month:02d}.box.mask.{box_size:.1f}deg.nc"

dso.to_netcdf(path=fileo, format="NETCDF4")
