import numpy as np
import pandas as pd
import xarray as xr

# For all functions defined below, TLL means 3D shaped (time, lat, lon) and TLLL means 4D shaped (time, lev, lat, lon)

def calcAnomTLL(x, clim, spd):  # Remove the mean value (clim) from the value for each day of year and each subdaily time interval (e.g., 00Z 1 January, 03Z 1 January)
    dayofyear = np.arange(1, clim.shape[0] // spd + 1, 1, dtype=int)

    x0 = x[0::spd, :, :]
    clim0 = clim[0::spd, :, :]
    clim0 = clim0.assign_coords({"dayofyear": dayofyear})
    anom0 = x0.groupby("time.dayofyear") - clim0

    anom = np.zeros(x.shape)

    anom[0::spd, :, :] = anom0.data

    for i in range(1, spd):
        xi = x[i::spd, :, :]
        climi = clim[i::spd, :, :]
        climi = climi.assign_coords({"dayofyear": dayofyear})
        anomi = xi.groupby("time.dayofyear") - climi

        anom[i::spd, :, :] = anomi.data

    anom = xr.DataArray(anom, dims=x.dims, coords=x.coords, attrs=x.attrs)

    return anom

def calcAnomTLLL(x, clim, spd):
    dayofyear = np.arange(1, clim.shape[0] // spd + 1, 1, dtype=int)

    x0 = x[0::spd, :, :, :]
    clim0 = clim[0::spd, :, :, :]
    clim0 = clim0.assign_coords({"dayofyear": dayofyear})
    anom0 = x0.groupby("time.dayofyear") - clim0

    anom = np.zeros(x.shape)

    anom[0::spd, :, :, :] = anom0.data

    for i in range(1, spd):
        xi = x[i::spd, :, :, :]
        climi = clim[i::spd, :, :, :]
        climi = climi.assign_coords({"dayofyear": dayofyear})
        anomi = xi.groupby("time.dayofyear") - climi

        anom[i::spd, :, :, :] = anomi.data

    anom = xr.DataArray(anom, dims=x.dims, coords=x.coords, attrs=x.attrs)

    return anom
