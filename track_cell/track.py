import numpy as np
import pandas as pd
import xarray as xr

def calc_max_activity(field_ref, track, lat, lon, margin): # Calculate the time instance when a defined track has minimum filtered OLR anomaly values around the centre (within margin)
    cells = np.unique(track["cell"].values)

    max_activity = pd.DataFrame()

    for cell in cells:
        time_n = track.loc[track["cell"] == cell]["timestr"].values
        lat_n = track.loc[track["cell"] == cell]["latitude"].values
        lon_n = track.loc[track["cell"] == cell]["longitude"].values
        
        if np.any(np.isnan(lat_n)) | np.any(np.isnan(lon_n)):
            continue

        if len(time_n) != len(field_ref["time"].sel(time=slice(time_n[0], time_n[-1])).values):
            print("##################################################")
            print(f"Cell #{cell}: ")
            print("time_n:")
            print(time_n)
            print("field_ref['time']:")
            print(field_ref["time"].sel(time=slice(time_n[0], time_n[-1])).values)
            continue

        grid_res = lon.values[1] - lon.values[0]
        
        lat3 = np.zeros(len(lat.values) + 2 * int(margin / grid_res))
        lat3[0:int(margin / grid_res)] = np.min(lat.values) + np.arange(-margin, 0.0, grid_res)
        lat3[int(margin / grid_res):-int(margin / grid_res)] = lat.values
        lat3[-int(margin / grid_res):] = np.max(lat.values) + np.arange(0.0 + grid_res, margin + grid_res, grid_res)
        
        lon3 = np.zeros(len(lon.values) * 3)
        lon3[0*len(lon.values):1*len(lon.values)] = lon.values - 360.0
        lon3[1*len(lon.values):2*len(lon.values)] = lon.values
        lon3[2*len(lon.values):3*len(lon.values)] = lon.values + 360.0
        
        field_ref_n = np.zeros((len(time_n), len(lat3), len(lon3))) * np.nan
        field_ref_n[:, int(margin / grid_res):-int(margin / grid_res), 0*len(lon.values):1*len(lon.values)] = field_ref.sel(time=slice(time_n[0], time_n[-1])).values
        field_ref_n[:, int(margin / grid_res):-int(margin / grid_res), 1*len(lon.values):2*len(lon.values)] = field_ref.sel(time=slice(time_n[0], time_n[-1])).values
        field_ref_n[:, int(margin / grid_res):-int(margin / grid_res), 2*len(lon.values):3*len(lon.values)] = field_ref.sel(time=slice(time_n[0], time_n[-1])).values
        
        activity = np.zeros(len(time_n)) * np.nan
        
        for i in range(0, len(time_n)):
            lat_i = lat_n[i]
            lon_i = lon_n[i]
        
            lat_i_index = np.argmin(np.abs(lat3 - lat_i))
            lon_i_index = np.argmin(np.abs(lon3 - lon_i))
        
            activity[i] = np.nanmean(field_ref_n[i, lat_i_index-int(margin / grid_res):lat_i_index+int(margin / grid_res)+1, lon_i_index-int(margin / grid_res):lon_i_index+int(margin / grid_res)+1])
        
        max_activity_index = np.nanargmin(activity)
        
        max_activity = pd.concat([max_activity, track.loc[(track["cell"] == cell) & (track["timestr"] == time_n[max_activity_index])]])
        
    return max_activity

def count_track(track):
    lat = np.arange(-30.5, 30.5 + 1.0, 1.0)
    lon = np.arange(0.5, 359.5 + 1.0, 1.0)
    
    x = np.zeros((len(lat), len(lon)), dtype=int)
    
    cells = np.unique(track["cell"].values)
    
    for cell in cells:
        time_n = track.loc[track["cell"] == cell]["timestr"].values
        lat_n = track.loc[track["cell"] == cell]["latitude"].values
        lon_n = track.loc[track["cell"] == cell]["longitude"].values

        if np.any(np.isnan(lat_n)) | np.any(np.isnan(lon_n)):
            continue

        if len(time_n) != int((pd.to_datetime(time_n[-1]) - pd.to_datetime(time_n[0])) / pd.Timedelta(3, "h") + 1):
            print("##################################################")
            print(f"Cell #{cell}: ")
            print("time_n:")
            print(time_n)
            continue
        
        for i in range(0, len(time_n)):
            lat_i = lat_n[i]
            lon_i = lon_n[i]
            
            lat_i_index = np.argmin(np.abs(lat - lat_i))
            lon_i_index = np.argmin(np.abs(lon - lon_i))
            
            x[lat_i_index, lon_i_index] += 1
    
    lat = xr.DataArray(lat, dims=["latitude"], coords={"latitude": lat}, attrs={"long_name": "latitude", "units": "degrees_north"})
    lon = xr.DataArray(lon, dims=["longitude"], coords={"longitude": lon}, attrs={"long_name": "longitude", "units": "degrees_east"})
    
    x = xr.DataArray(x, dims=["latitude", "longitude"], coords={"latitude": lat, "longitude": lon})
    
    return x

def count_max_activity(max_activity):
    lat = np.arange(-30.5, 30.5 + 1.0, 1.0)
    lon = np.arange(0.5, 359.5 + 1.0, 1.0)
    
    x = np.zeros((len(lat), len(lon)), dtype=int)
    
    lat_n = max_activity["latitude"].values
    lon_n = max_activity["longitude"].values
    
    for i in range(0, len(lat_n)):
        lat_i = lat_n[i]
        lon_i = lon_n[i]
    
        lat_i_index = np.argmin(np.abs(lat - lat_i))
        lon_i_index = np.argmin(np.abs(lon - lon_i))
    
        x[lat_i_index, lon_i_index] += 1

    lat = xr.DataArray(lat, dims=["latitude"], coords={"latitude": lat}, attrs={"long_name": "latitude", "units": "degrees_north"})
    lon = xr.DataArray(lon, dims=["longitude"], coords={"longitude": lon}, attrs={"long_name": "longitude", "units": "degrees_east"})

    x = xr.DataArray(x, dims=["latitude", "longitude"], coords={"latitude": lat, "longitude": lon})

    return x

def quality_control(track, spd):
    cells = np.unique(track["cell"].values)
    
    cells_to_use = []
    
    for cell in cells:
        time_n = track.loc[track["cell"] == cell]["timestr"].values
        lat_n = track.loc[track["cell"] == cell]["latitude"].values
        lon_n = track.loc[track["cell"] == cell]["longitude"].values
        
        time = pd.date_range(start=time_n[0], end=time_n[-1], freq=f"{24 // spd:d}h")
        
        if np.any(np.isnan(lat_n)) | np.any(np.isnan(lon_n)):
            continue
        
        if len(time_n) != len(time.values):
            continue
        
        cells_to_use.append(cell)
    
    track_new = track.loc[np.isin(track["cell"].values, cells_to_use)]
    
    return track_new
