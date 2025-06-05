import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

import sys

from config import *

def open_era5_TLL_per_year(folder_name, var_name, year, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0):
    files = sorted(glob(f"{ERA5_RT52_DIR}/single-levels/reanalysis/{folder_name}/{year:04d}/*.nc"))
    
    if len(files) == 0:
        sys.exit(f"No .nc files are found in {ERA5_RT52_DIR}/single-levels/reanalysis/{folder_name}/{year:04d}/")
    
    x = xr.open_dataset(files[0])[var_name].sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()
    
    for i in range(1, len(files)):
        xi = xr.open_dataset(files[i])[var_name].sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()
        
        x = xr.concat([x, xi], dim="time")
    
    return x

def open_era5_TLLL_per_year(folder_name, var_name, year, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0):
    files = sorted(glob(f"{ERA5_RT52_DIR}/pressure-levels/reanalysis/{folder_name}/{year:04d}/*.nc"))

    if len(files) == 0:
        sys.exit(f"No .nc files are found in {ERA5_RT52_DIR}/pressure-levels/reanalysis/{folder_name}/{year:04d}/")

    x = xr.open_dataset(files[0])[var_name].sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()

    for i in range(1, len(files)):
        xi = xr.open_dataset(files[i])[var_name].sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()

        x = xr.concat([x, xi], dim="time")

    return x

def open_era5_TLLL_per_level_per_year(folder_name, var_name, level, year, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0):
    files = sorted(glob(f"{ERA5_RT52_DIR}/pressure-levels/reanalysis/{folder_name}/{year:04d}/*.nc"))

    if len(files) == 0:
        sys.exit(f"No .nc files are found in {ERA5_RT52_DIR}/pressure-levels/reanalysis/{folder_name}/{year:04d}/")

    x = xr.open_dataset(files[0])[var_name].sel(level=slice(level, level), latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()

    for i in range(1, len(files)):
        xi = xr.open_dataset(files[i])[var_name].sel(level=slice(level, level), latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()

        x = xr.concat([x, xi], dim="time")

    return x

def open_era5_TLL(folder_name, var_name, year_start, year_end, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0):
    if year_end < year_start:
        sys.exit("year_end must be greater than year_start")
    elif year_end == year_start:
        sys.exit("year_end must be greater than year_start, use open_era5_TLL_per_year if you want to open only one year of data")
    
    years = np.arange(year_start, year_end + 1, 1, dtype=int)
    
    x = open_era5_TLL_per_year(folder_name, var_name, years[0], spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0)
    
    for year in years[1:]:
        xi = open_era5_TLL_per_year(folder_name, var_name, year, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0)
        
        x = xr.concat([x, xi], dim="time")
    
    return x

def open_era5_TLLL(folder_name, var_name, year_start, year_end, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0):
    if year_end < year_start:
        sys.exit("year_end must be greater than year_start")
    elif year_end == year_start:
        sys.exit("year_end must be greater than year_start, use open_era5_TLLL_per_year if you want to open only one year of data")

    years = np.arange(year_start, year_end + 1, 1, dtype=int)

    x = open_era5_TLLL_per_year(folder_name, var_name, years[0], spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0)

    for year in years[1:]:
        xi = open_era5_TLLL_per_year(folder_name, var_name, year, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0)

        x = xr.concat([x, xi], dim="time")

    return x

def open_era5_TLLL_per_level(folder_name, var_name, level, year_start, year_end, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0):
    if year_end < year_start:
        sys.exit("year_end must be greater than year_start")
    elif year_end == year_start:
        sys.exit("year_end must be greater than year_start, use open_era5_TLLL_per_level_per_year if you want to open only one year of data")

    years = np.arange(year_start, year_end + 1, 1, dtype=int)

    x = open_era5_TLLL_per_level_per_year(folder_name, var_name, level, years[0], spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0)

    for year in years[1:]:
        xi = open_era5_TLLL_per_level_per_year(folder_name, var_name, level, year, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0)

        x = xr.concat([x, xi], dim="time")

    return x

def convert_lon_180_to_360(x, lon_name):
    lon_new = np.where(x[lon_name].values < 0.0, x[lon_name].values + 360.0, x[lon_name].values)
    lon_new = sorted(lon_new)
    lon_new = xr.DataArray(lon_new, dims=[lon_name], coords={lon_name: lon_new}, attrs=x[lon_name].attrs)
    
    x_new = np.zeros(x.shape) * np.nan
    
    x_new[..., 0:len(x[lon_name].values) // 2] = x[..., len(x[lon_name].values) // 2:].values
    x_new[..., len(x[lon_name].values) // 2:] = x[..., 0:len(x[lon_name].values) // 2].values
    
    x_new = xr.DataArray(x_new, dims=x.dims, coords=x.coords, attrs=x.attrs)
    x_new = x_new.assign_coords({lon_name: lon_new})
    
    return x_new

def open_gpm_ia39_per_year(var_name, year, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0):
    if year == 2000:
        dates = pd.date_range(start=f"{year:04d}-06-01", end=f"{year:04d}-12-31", freq="1D")
    else:
        dates = pd.date_range(start=f"{year:04d}-01-01", end=f"{year:04d}-12-31", freq="1D")
    
    filei = sorted(glob(f"{GPM_IA39_DIR}/{year:04d}/3B-HHR.MS.MRG.3IMERG.{dates[0].year:04d}{dates[0].month:02d}{dates[0].day:02d}*"))
    
    if len(filei) == 0:
        sys.exit(f"No .nc file for {dates[0].year:04d}{dates[0].month:02d}{dates[0].day:02d}")
    
    x = xr.open_dataset(filei[0])[var_name].sel(lon=slice(lon_min, lon_max), lat=slice(lat_min, lat_max))
    
    if len(x["time"].values) != 48:
        print(f"Only {len(x['time'].values)} time steps!")
        
        filei = sorted(glob(f"/g/data/k10/mr4682/data/gpm/{year:04d}/3B-HHR.MS.MRG.3IMERG.{dates[0].year:04d}{dates[0].month:02d}{dates[0].day:02d}*"))
        
        if len(filei) == 0:
            sys.exit(f"No .nc file for {dates[0].year:04d}{dates[0].month:02d}{dates[0].day:02d}")
    
        x = xr.open_dataset(filei[0])[var_name].sel(lon=slice(lon_min, lon_max), lat=slice(lat_min, lat_max))
    
    x = x.resample(time=f"{24 // spd:d}h").mean()
    
    for i in range(1, len(dates)):
        filei = sorted(glob(f"{GPM_IA39_DIR}/{year:04d}/3B-HHR.MS.MRG.3IMERG.{dates[i].year:04d}{dates[i].month:02d}{dates[i].day:02d}*"))
        
        if len(filei) == 0:
            sys.exit(f"No .nc file for {dates[i].year:04d}{dates[i].month:02d}{dates[i].day:02d}")
    
        xi = xr.open_dataset(filei[0])[var_name].sel(lon=slice(lon_min, lon_max), lat=slice(lat_min, lat_max))
    
        if len(xi["time"].values) != 48:
            print(f"Only {len(x['time'].values)} time steps!")
            
            filei = sorted(glob(f"/g/data/k10/mr4682/data/gpm/{year:04d}/3B-HHR.MS.MRG.3IMERG.{dates[i].year:04d}{dates[i].month:02d}{dates[i].day:02d}*"))
            
            if len(filei) == 0:
                sys.exit(f"No .nc file for {dates[i].year:04d}{dates[i].month:02d}{dates[i].day:02d}")
    
            xi = xr.open_dataset(filei[0])[var_name].sel(lon=slice(lon_min, lon_max), lat=slice(lat_min, lat_max))
    
        xi = xi.resample(time=f"{24 // spd:d}h").mean()
    
        x = xr.concat([x, xi], dim="time")
    
    return x
