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
    
    if spd == 48:
        pass
    elif 24 % spd == 0:
        x = x.resample(time=f"{24 // spd:d}h").mean()
    else:
        sys.exit("spd must be a factor of 24 or spd must be 48")
    
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
    
        if spd == 48:
            pass
        elif 24 % spd == 0:
            xi = xi.resample(time=f"{24 // spd:d}h").mean()
        else:
            sys.exit("spd must be a factor of 24 or spd must be 48")
    
        x = xr.concat([x, xi], dim="time")
    
    return x

def open_gpm_ia39_per_month(var_name, year, month, spd, lat_min=-90.0, lat_max=90.0, lon_min=-180.0, lon_max=180.0):
    month_length = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    
    if (year % 4) == 0:
        month_length[2] = 29
    
    dates = pd.date_range(start=f"{year:04d}-{month:02d}-01", end=f"{year:04d}-{month:02d}-{month_length[month]:02d}", freq="1D")

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

    if spd == 48:
        pass
    elif 24 % spd == 0:
        x = x.resample(time=f"{24 // spd:d}h").mean()
    else:
        sys.exit("spd must be a factor of 24 or spd must be 48")
    
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

        if spd == 48:
            pass
        elif 24 % spd == 0:
            xi = xi.resample(time=f"{24 // spd:d}h").mean()
        else:
            sys.exit("spd must be a factor of 24 or spd must be 48")

        x = xr.concat([x, xi], dim="time")

    return x

def regrid_era5_TLL_per_year(folder_name, var_name, year, spd, res_new, lat_min, lat_max, lon_min, lon_max, method="bilinear", periodic=True):
    import xesmf as xe
    
    files = sorted(glob(f"{ERA5_RT52_DIR}/single-levels/reanalysis/{folder_name}/{year:04d}/*.nc"))

    if len(files) == 0:
        sys.exit(f"No .nc files are found in {ERA5_RT52_DIR}/single-levels/reanalysis/{folder_name}/{year:04d}/")
    
    if periodic:
        ds_0 = xr.open_dataset(files[0]).sel(latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
    else:
        ds_0 = xr.open_dataset(files[0]).sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()
    
    attrs = ds_0[var_name].attrs

    ds_new = xr.Dataset(
        {
            "latitude": (["latitude"], np.arange(lat_min, lat_max + res_new, res_new), {"units": "degrees_north"}),
            "longitude": (["longitude"], np.arange(lon_min, lon_max + res_new, res_new), {"units": "degrees_east"})
        }
    )

    regridder_0 = xe.Regridder(ds_0, ds_new, method, periodic=periodic)

    ds_new = regridder_0(ds_0)
    
    for i in range(1, len(files)):
        if periodic:
            ds_i = xr.open_dataset(files[i]).sel(latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
        else:
            ds_i = xr.open_dataset(files[i]).sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()
        
        ds_i_new = xr.Dataset(
                {
                    "latitude": (["latitude"], np.arange(lat_min, lat_max + res_new, res_new), {"units": "degrees_north"}),
                    "longitude": (["longitude"], np.arange(lon_min, lon_max + res_new, res_new), {"units": "degrees_east"})
                }
        )
        
        regridder_i = xe.Regridder(ds_i, ds_i_new, method, periodic=periodic)
        
        ds_i_new = regridder_i(ds_i)
        
        ds_new = xr.concat([ds_new, ds_i_new], dim="time")
    
    return ds_new[var_name].assign_attrs(attrs)

def regrid_era5_TLLL_per_year(folder_name, var_name, year, spd, res_new, lat_min, lat_max, lon_min, lon_max, method="bilinear", periodic=True):
    import xesmf as xe

    files = sorted(glob(f"{ERA5_RT52_DIR}/pressure-levels/reanalysis/{folder_name}/{year:04d}/*.nc"))

    if len(files) == 0:
        sys.exit(f"No .nc files are found in {ERA5_RT52_DIR}/pressure-levels/reanalysis/{folder_name}/{year:04d}/")

    if periodic:
        ds_0 = xr.open_dataset(files[0]).sel(latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
    else:
        ds_0 = xr.open_dataset(files[0]).sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()

    attrs = ds_0[var_name].attrs

    ds_new = xr.Dataset(
        {
            "latitude": (["latitude"], np.arange(lat_min, lat_max + res_new, res_new), {"units": "degrees_north"}),
            "longitude": (["longitude"], np.arange(lon_min, lon_max + res_new, res_new), {"units": "degrees_east"})
        }
    )

    regridder_0 = xe.Regridder(ds_0, ds_new, method, periodic=periodic)

    ds_new = regridder_0(ds_0)

    for i in range(1, len(files)):
        if periodic:
            ds_i = xr.open_dataset(files[i]).sel(latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
        else:
            ds_i = xr.open_dataset(files[i]).sel(latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()

        ds_i_new = xr.Dataset(
                {
                    "latitude": (["latitude"], np.arange(lat_min, lat_max + res_new, res_new), {"units": "degrees_north"}),
                    "longitude": (["longitude"], np.arange(lon_min, lon_max + res_new, res_new), {"units": "degrees_east"})
                }
        )

        regridder_i = xe.Regridder(ds_i, ds_i_new, method, periodic=periodic)
        
        ds_i_new = regridder_i(ds_i)

        ds_new = xr.concat([ds_new, ds_i_new], dim="time")

    return ds_new[var_name].assign_attrs(attrs)

def regrid_era5_TLLL_per_level_per_year(folder_name, var_name, level, year, spd, res_new, lat_min, lat_max, lon_min, lon_max, method="bilinear", periodic=True):
    import xesmf as xe

    files = sorted(glob(f"{ERA5_RT52_DIR}/pressure-levels/reanalysis/{folder_name}/{year:04d}/*.nc"))

    if len(files) == 0:
        sys.exit(f"No .nc files are found in {ERA5_RT52_DIR}/pressure-levels/reanalysis/{folder_name}/{year:04d}/")

    if periodic:
        ds_0 = xr.open_dataset(files[0]).sel(level=slice(level, level), latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
    else:
        ds_0 = xr.open_dataset(files[0]).sel(level=slice(level, level), latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()

    attrs = ds_0[var_name].attrs

    ds_new = xr.Dataset(
        {
            "latitude": (["latitude"], np.arange(lat_min, lat_max + res_new, res_new), {"units": "degrees_north"}),
            "longitude": (["longitude"], np.arange(lon_min, lon_max + res_new, res_new), {"units": "degrees_east"})
        }
    )

    regridder_0 = xe.Regridder(ds_0, ds_new, method, periodic=periodic)

    ds_new = regridder_0(ds_0)

    for i in range(1, len(files)):
        if periodic:
            ds_i = xr.open_dataset(files[i]).sel(level=slice(level, level), latitude=slice(lat_max, lat_min)).resample(time=f"{24 // spd:d}h").mean()
        else:
            ds_i = xr.open_dataset(files[i]).sel(level=slice(level, level), latitude=slice(lat_max, lat_min), longitude=slice(lon_min, lon_max)).resample(time=f"{24 // spd:d}h").mean()

        ds_i_new = xr.Dataset(
                {
                    "latitude": (["latitude"], np.arange(lat_min, lat_max + res_new, res_new), {"units": "degrees_north"}),
                    "longitude": (["longitude"], np.arange(lon_min, lon_max + res_new, res_new), {"units": "degrees_east"})
                }
        )

        regridder_i = xe.Regridder(ds_i, ds_i_new, method, periodic=periodic)
        
        ds_i_new = regridder_i(ds_i)

        ds_new = xr.concat([ds_new, ds_i_new], dim="time")

    return ds_new[var_name].assign_attrs(attrs)

def add_clim_to_anom(clim, anom, spd):
    x = np.zeros(anom.shape) * np.nan
    
    for i in range(0, spd):
        clim_i = clim[i::spd, ...]
        clim_i = clim_i.assign_coords({"dayofyear": clim_i["dayofyear"].astype(int)})
        
        anom_i = anom[i::spd, ...]
        
        x_i = anom_i.groupby("time.dayofyear") + clim_i
        
        x[i::spd, ...] = x_i.values
    
    x = xr.DataArray(x, dims=anom.dims, coords=anom.coords, attrs=anom.attrs)
    
    return x

def calc_dse(t, z):
    dse = t.values * 1005.0 + z.values
    dse = xr.DataArray(dse, dims=t.dims, coords=t.coords, attrs={"long_name": "Dry static energy", "units": "J kg**-1"})

    return dse

def calc_mse(t, z, q):
    mse = t.values * 1005.0 + z.values + q.values * 2.5e+06
    mse = xr.DataArray(mse, dims=t.dims, coords=t.coords, attrs={"long_name": "Moist static energy", "units": "J kg**-1"})

    return mse
