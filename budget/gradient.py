import numpy as np
import pandas as pd
import xarray as xr

def calc_df_dt_TLL(f):
    long_name = ""
    units = ""
    
    time = f[f.dims[0]]
    
    df_dt = np.gradient(f.values, time.values.astype(float) * 1.0e-09, axis=0)
    
    df_dt = xr.DataArray(df_dt, dims=f.dims, coords=f.coords)
    
    if "long_name" in f.attrs:
        long_name = f.attrs["long_name"]
    if "units" in f.attrs:
        units = f.attrs["units"]
    
    df_dt = df_dt.assign_attrs({"long_name": f"Temporal tendency of {long_name}", "units": f"{units} s**-1"})
    
    return df_dt

def calc_df_dt_TLLL(f):
    return calc_df_dt_TLL(f)

def calc_df_dx_TLL(f, cyclic=False):
    long_name = ""
    units = ""
    
    a = 6.371e+06
    
    lat = f[f.dims[1]]
    lon = f[f.dims[2]]

    if cyclic:
        dlon = lon.values[1] - lon.values[0]

        lon_new = np.zeros(len(lon.values) + 2, dtype=lon.values.dtype)

        lon_new[1:-1] = np.copy(lon.values)
        lon_new[0] = lon_new[1] - dlon
        lon_new[-1] = lon_new[-2] + dlon

        f_new = np.zeros((f.values.shape[0], f.values.shape[1], f.values.shape[2] + 2), dtype=f.values.dtype)

        f_new[:, :, 1:-1] = np.copy(f.values)
        f_new[:, :, 0] = np.copy(f.values[:, :, -1])
        f_new[:, :, -1] = np.copy(f.values[:, :, 0])

        df_dx = np.gradient(f_new, a * np.radians(lon_new), axis=2)[:, :, 1:-1] / (np.broadcast_to(np.cos(np.radians(lat.values)), (len(lon.values), len(lat.values)))).T
    else:
        df_dx = np.gradient(f.values, a * np.radians(lon.values), axis=2) / (np.broadcast_to(np.cos(np.radians(lat.values)), (len(lon.values), len(lat.values)))).T
    
    if "long_name" in f.attrs:
        long_name = f.attrs["long_name"]
    if "units" in f.attrs:
        units = f.attrs["units"]

    df_dx = xr.DataArray(df_dx, dims=f.dims, coords=f.coords)
    
    df_dx = df_dx.assign_attrs({"long_name": f"Zonal gradient of {long_name}", "units": f"{units} m**-1"})
    
    return df_dx

def calc_df_dx_TLLL(f, cyclic=False):
    long_name = ""
    units = ""
    
    a = 6.371e+06
    
    lat = f[f.dims[2]]
    lon = f[f.dims[3]]

    if cyclic:
        dlon = lon.values[1] - lon.values[0]

        lon_new = np.zeros(len(lon.values) + 2, dtype=lon.values.dtype)

        lon_new[1:-1] = np.copy(lon.values)
        lon_new[0] = lon_new[1] - dlon
        lon_new[-1] = lon_new[-2] + dlon

        f_new = np.zeros((f.values.shape[0], f.values.shape[1], f.values.shape[2], f.values.shape[3] + 2), dtype=f.values.dtype)

        f_new[:, :, :, 1:-1] = np.copy(f.values)
        f_new[:, :, :, 0] = np.copy(f.values[:, :, :, -1])
        f_new[:, :, :, -1] = np.copy(f.values[:, :, :, 0])

        df_dx = np.gradient(f_new, a * np.radians(lon_new), axis=3)[:, :, :, 1:-1] / (np.broadcast_to(np.cos(np.radians(lat.values)), (len(lon.values), len(lat.values)))).T
    else:
        df_dx = np.gradient(f.values, a * np.radians(lon.values), axis=3) / (np.broadcast_to(np.cos(np.radians(lat.values)), (len(lon.values), len(lat.values)))).T
    
    if "long_name" in f.attrs:
        long_name = f.attrs["long_name"]
    if "units" in f.attrs:
        units = f.attrs["units"]

    df_dx = xr.DataArray(df_dx, dims=f.dims, coords=f.coords)
    
    df_dx = df_dx.assign_attrs({"long_name": f"Zonal gradient of {long_name}", "units": f"{units} m**-1"})
    
    return df_dx

def calc_df_dy_TLL(f):
    long_name = ""
    units = ""
    
    a = 6.371e+06
    
    lat = f[f.dims[1]]

    df_dy = np.gradient(f.values, a * np.radians(lat.values), axis=1)
    
    if "long_name" in f.attrs:
        long_name = f.attrs["long_name"]
    if "units" in f.attrs:
        units = f.attrs["units"]

    df_dy = xr.DataArray(df_dy, dims=f.dims, coords=f.coords)
    
    df_dy = df_dy.assign_attrs({"long_name": f"Meridional gradient of {long_name}", "units": f"{units} m**-1"})
    
    return df_dy

def calc_df_dy_TLLL(f):
    long_name = ""
    units = ""
    
    a = 6.371e+06
    
    lat = f[f.dims[2]]

    df_dy = np.gradient(f.values, a * np.radians(lat.values), axis=2)
    
    if "long_name" in f.attrs:
        long_name = f.attrs["long_name"]
    if "units" in f.attrs:
        units = f.attrs["units"]

    df_dy = xr.DataArray(df_dy, dims=f.dims, coords=f.coords)
    
    df_dy = df_dy.assign_attrs({"long_name": f"Meridional gradient of {long_name}", "units": f"{units} m**-1"})
    
    return df_dy

def calc_df_dp_TLLL(f):
    long_name = ""
    units = ""
    
    level = f[f.dims[1]]
    
    df_dp = np.gradient(f.values, level.values * 100.0, axis=1)
    
    df_dp = xr.DataArray(df_dp, dims=f.dims, coords=f.coords)
    
    if "long_name" in f.attrs:
        long_name = f.attrs["long_name"]
    if "units" in f.attrs:
        units = f.attrs["units"]
    
    df_dp = df_dp.assign_attrs({"long_name": f"Vertical gradient of {long_name}", "units": f"{units} Pa**-1"})
    
    return df_dp