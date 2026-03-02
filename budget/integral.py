import numpy as np
import pandas as pd
import xarray as xr

def calc_dp(p, sp):
    p_mid = np.zeros(len(p) + 1) * np.nan
    
    p_mid[0] = np.min(p)
    p_mid[1:-1] = (p[0:-1] + p[1:]) / 2.0
    
    if np.isscalar(sp):
        p_mid[-1] = sp
        p_mid[p_mid > sp] = np.nan
    else:
        p_mid = np.copy(np.broadcast_to(p_mid, sp.shape + (len(p_mid),)))
        p_mid[..., -1] = sp
        p_mid = xr.where(p_mid <= sp[..., None], p_mid, np.nan)
    
    p_mid = np.sort(p_mid)
    
    dp = np.diff(p_mid)
    
    return dp

def calc_vertical_integral_TLLL(f, sp):
    long_name = ""
    units = ""
    
    if np.isscalar(sp):
        dp = calc_dp(f[f.dims[1]].values * 100.0, sp)
    else:
        dp = calc_dp(f[f.dims[1]].values * 100.0, sp.values)
    
    f_dp = f.transpose(f.dims[0], f.dims[2], f.dims[3], f.dims[1]).values * dp
    f_dp = np.moveaxis(f_dp, -1, 1)
    vi_f = np.nansum(f_dp, axis=1) / 9.81
    vi_f = np.where(np.all(np.isnan(f_dp), axis=1), np.nan, vi_f)
    
    if "long_name" in f.attrs:
        long_name = f.attrs["long_name"]
    if "units" in f.attrs:
        units = f.attrs["units"]
    
    vi_f = xr.DataArray(vi_f, dims=[f.dims[0], f.dims[2], f.dims[3]], coords={f.dims[0]: f[f.dims[0]], f.dims[2]: f[f.dims[2]], f.dims[3]: f[f.dims[3]]}, attrs={"long_name": f"Vertical integral of {long_name}", "units": f"{units} kg m**-2"})
    
    return vi_f