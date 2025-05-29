import numpy as np
import pandas as pd
import xarray as xr

# For all functions defined below, TLL means 3D shaped (time, lat, lon) and TLLL means 4D shaped (time, lev, lat, lon)

def calcDayClimTLL(x):  # Calculate the mean value for each day of year (e.g., 1 January = 001)
    clim = x.groupby("time.dayofyear").mean()

    return clim

def smthDayClimTLL(clim, nsmth):    # Retain the first nsmth harmonics of the clim resulted by calcDayClimTLL
    cf = np.fft.rfft(clim.data, axis=0)
    frq = np.fft.rfftfreq(clim.shape[0])

    cf[frq > nsmth / clim.shape[0], :, :] = 0.0

    clim_smth = np.real(np.fft.irfft(cf, n=clim.shape[0], axis=0))
    clim_smth = xr.DataArray(clim_smth, dims=clim.dims, coords=clim.coords, attrs=clim.attrs)

    return clim_smth

def calcClimTLL(x, spd, smooth=False, nsmth=None):  # Calculate the mean value for each day of year and each subdaily time interval (e.g., mean values of 00Z 1 January, 03Z 1 January, etc.)
    # spd = samples per day (i.e., 3-hourly data: spd = 8, 12-hourly data: spd = 2)
    clim0 = calcDayClimTLL(x[0::spd, :, :])

    clim = np.zeros((clim0.shape[0] * spd, clim0.shape[1], clim0.shape[2]))

    if smooth:
        if nsmth != None:
            clim[0::spd, :, :] = smthDayClimTLL(clim0, nsmth).data
        else:
            clim[0::spd, :, :] = smthDayClimTLL(clim0, nsmth=3).data
    else:
        clim[0::spd, :, :] = clim0.data

    for i in range(1, spd):
        climi = calcDayClimTLL(x[i::spd, :, :])

        if smooth:
            if nsmth != None:
                clim[i::spd, :, :] = smthDayClimTLL(climi, nsmth).data
            else:
                clim[i::spd, :, :] = smthDayClimTLL(climi, nsmth=3).data
        else:
            clim[i::spd, :, :] = climi.data

    doy = np.arange(1.0, clim0.shape[0] + 1, 1.0 / spd)
    doy = xr.DataArray(doy, dims=["dayofyear"], coords={"dayofyear": doy})

    clim = xr.DataArray(clim, dims=["dayofyear", clim0.dims[1], clim0.dims[2]], coords={"dayofyear": doy, clim0.dims[1]: clim0.coords[clim0.dims[1]], clim0.dims[2]: clim0.coords[clim0.dims[2]]}, attrs=clim0.attrs)

    return clim

def smthClimTLL(clim, spd, nsmth):  # Retain the first nsmth harmonics (i.e., seasonal cycle) and all the subdaily harmonics (i.e., diurnal cycle) of the clim resulted by calcClimTLL
    cf = np.fft.rfft(clim.data, axis=0)
    frq = np.fft.rfftfreq(clim.shape[0], d=1.0 / spd)

    cf[(frq > nsmth * spd / clim.shape[0]) & (frq < 1.0 / spd), :, :] = 0.0

    clim_smth = np.real(np.fft.irfft(cf, n=clim.shape[0], axis=0))
    clim_smth = xr.DataArray(clim_smth, dims=clim.dims, coords=clim.coords, attrs=clim.attrs)

    return clim_smth

# Functions below are basically the same as above, but for 4D shaped arrays

def calcDayClimTLLL(x):
    return calcDayClimTLL(x)

def smthDayClimTLLL(clim, nsmth):
    cf = np.fft.rfft(clim.data, axis=0)
    frq = np.fft.rfftfreq(clim.shape[0])

    cf[frq > nsmth / clim.shape[0], :, :, :] = 0.0

    clim_smth = np.real(np.fft.irfft(cf, n=clim.shape[0], axis=0))
    clim_smth = xr.DataArray(clim_smth, dims=clim.dims, coords=clim.coords, attrs=clim.attrs)

    return clim_smth

def calcClimTLLL(x, spd, smooth=False, nsmth=None):
    clim0 = calcDayClimTLLL(x[0::spd, :, :, :])

    clim = np.zeros((clim0.shape[0] * spd, clim0.shape[1], clim0.shape[2], clim0.shape[3]))

    if smooth:
        if nsmth != None:
            clim[0::spd, :, :, :] = smthDayClimTLLL(clim0, nsmth).data
        else:
            clim[0::spd, :, :, :] = smthDayClimTLLL(clim0, nsmth=3).data
    else:
        clim[0::spd, :, :, :] = clim0.data

    for i in range(1, spd):
        climi = calcDayClimTLLL(x[i::spd, :, :, :])

        if smooth:
            if nsmth != None:
                clim[i::spd, :, :, :] = smthDayClimTLLL(climi, nsmth).data
            else:
                clim[i::spd, :, :, :] = smthDayClimTLLL(climi, nsmth=3).data
        else:
            clim[i::spd, :, :, :] = climi.data

    doy = np.arange(1.0, clim0.shape[0] + 1, 1.0 / spd)
    doy = xr.DataArray(doy, dims=["dayofyear"], coords={"dayofyear": doy})

    clim = xr.DataArray(clim, dims=["dayofyear", clim0.dims[1], clim0.dims[2], clim0.dims[3]], coords={"dayofyear": doy, clim0.dims[1]: clim0.coords[clim0.dims[1]], clim0.dims[2]: clim0.coords[clim0.dims[2]], clim0.dims[3]: clim0.coords[clim0.dims[3]]}, attrs=clim0.attrs)

    return clim

def smthClimTLLL(clim, spd, nsmth):
    cf = np.fft.rfft(clim.data, axis=0)
    frq = np.fft.rfftfreq(clim.shape[0], d=1.0 / spd)

    cf[(frq > nsmth * spd / clim.shape[0]) & (frq < 1.0 / spd), :, :, :] = 0.0

    clim_smth = np.real(np.fft.irfft(cf, n=clim.shape[0], axis=0))
    clim_smth = xr.DataArray(clim_smth, dims=clim.dims, coords=clim.coords, attrs=clim.attrs)

    return clim_smth
