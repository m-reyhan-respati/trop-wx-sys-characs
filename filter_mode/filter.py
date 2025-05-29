import numpy as np
import pandas as pd
import xarray as xr
from glob import glob

from config import *

# For all functions defined below, TLL means 3D shaped (time, lat, lon) and TLLL means 4D shaped (time, lev, lat, lon)

def next2n(x):
    return int(2.0 ** (int(np.log(x) / np.log(2.0)) + 1))

def pad_zeros_TLL(x, spd):
    ntime = len(x["time"].values)
    
    diff = next2n(ntime) - ntime
    
    ntime_before = diff // 2
    
    
    if diff % 2 == 0:
        ntime_after = ntime_before
    else:
        ntime_after = diff // 2 + 1
    
    time_before = pd.date_range(end=pd.to_datetime(x["time"].values[0])-pd.Timedelta(24 // spd, "h"), periods=ntime_before, freq=f"{24 // spd:d}h")
    time_after = pd.date_range(start=pd.to_datetime(x["time"].values[ntime-1])+pd.Timedelta(24 // spd, "h"), periods=ntime_after, freq=f"{24 // spd:d}h")
    
    time_new = pd.to_datetime(np.concatenate((time_before.values, x["time"].values, time_after.values)))
    
    x_new = np.zeros((len(time_new.values), x.shape[1], x.shape[2]))
    x_new[ntime_before:-ntime_after, :, :] = x.values
    
    x_new = xr.DataArray(x_new, dims=x.dims, coords={"time": time_new, x.dims[1]: x[x.dims[1]], x.dims[2]: x[x.dims[2]]}, attrs=x.attrs)
    
    return x_new

def pad_zeros_TLLL(x, spd):
    ntime = len(x["time"].values)
    
    diff = next2n(ntime) - ntime
    
    ntime_before = diff // 2
    
    
    if diff % 2 == 0:
        ntime_after = ntime_before
    else:
        ntime_after = diff // 2 + 1
    
    time_before = pd.date_range(end=pd.to_datetime(x["time"].values[0])-pd.Timedelta(24 // spd, "h"), periods=ntime_before, freq=f"{24 // spd:d}h")
    time_after = pd.date_range(start=pd.to_datetime(x["time"].values[ntime-1])+pd.Timedelta(24 // spd, "h"), periods=ntime_after, freq=f"{24 // spd:d}h")
    
    time_new = pd.to_datetime(np.concatenate((time_before.values, x["time"].values, time_after.values)))
    
    x_new = np.zeros((len(time_new.values), x.shape[1], x.shape[2], x.shape[3]))
    x_new[ntime_before:-ntime_after, :, :, :] = x.values
    
    x_new = xr.DataArray(x_new, dims=x.dims, coords={"time": time_new, x.dims[1]: x[x.dims[1]], x.dims[2]: x[x.dims[2]], x.dims[3]: x[x.dims[3]]}, attrs=x.attrs)
    
    return x_new

def remove_tc_TLL(x, time, lat, lon):
    y = np.copy(x.data)
    
    lon2d, lat2d = np.meshgrid(lon.data, lat.data)
    
    f = xr.open_dataset(str(IBTrACS_FILE))
    
    time_tc = f["time"]
    lat_tc = f["lat"]
    lon_tc = f["lon"]
    
    storm = f["storm"]
    
    for i in range(0, len(storm)):
        time_tc_i = time_tc[i, :].values
        len_time_tc_i = len(time_tc_i) - np.count_nonzero(np.isnan(time_tc_i))
        
        if ((time_tc_i[0] >= time.values[0]) & (time_tc_i[0] <= time.values[-1])) | ((time_tc_i[len_time_tc_i - 1] >= time.values[0]) & (time_tc_i[len_time_tc_i - 1] <= time.values[-1])):
            time_tc_i_list = np.asarray([], dtype=np.datetime64)
            for n in range(0, len_time_tc_i):
                time_tc_i_n = np.datetime64(np.datetime_as_string(time_tc_i[n], unit="s"))
                
                if (time_tc_i_n in time.values) & (time_tc_i_n not in time_tc_i_list):
                    time_tc_i_list = np.append(time_tc_i_list, time_tc_i_n)
                    
                    lat_tc_i_n = lat_tc[i, n].values
                    lon_tc_i_n = lon_tc[i, n].values
                    
                    c = np.sin(np.radians(lat2d)) * np.sin(np.radians(lat_tc_i_n)) + np.cos(np.radians(lat2d)) * np.cos(np.radians(lat_tc_i_n)) * np.cos(np.radians(lon2d - lon_tc_i_n))
                    c = np.where((c >= -1) & (c <= 1), c, np.nan)
                    
                    r = np.arccos(c) * 6371
                    
                    w = 1 - np.exp(-(r / 500) * (r / 500) * np.log(4) / 2)
                    
                    y[time.values == time_tc_i_n, :, :] = y[time.values == time_tc_i_n, :, :] * w
    
    y = xr.DataArray(y, dims=x.dims, coords=x.coords, attrs=x.attrs)
    
    return y

def remove_tc_TLLL(x, time, lat, lon):
    y = np.copy(x.data)

    lon2d, lat2d = np.meshgrid(lon.data, lat.data)

    f = xr.open_dataset(str(IBTrACS_FILE))

    time_tc = f["time"]
    lat_tc = f["lat"]
    lon_tc = f["lon"]

    storm = f["storm"]

    for i in range(0, len(storm)):
        time_tc_i = time_tc[i, :].values
        len_time_tc_i = len(time_tc_i) - np.count_nonzero(np.isnan(time_tc_i))

        if ((time_tc_i[0] >= time.values[0]) & (time_tc_i[0] <= time.values[-1])) | ((time_tc_i[len_time_tc_i - 1] >= time.values[0]) & (time_tc_i[len_time_tc_i - 1] <= time.values[-1])):
            time_tc_i_list = np.asarray([], dtype=np.datetime64)
            for n in range(0, len_time_tc_i):
                time_tc_i_n = np.datetime64(np.datetime_as_string(time_tc_i[n], unit="s"))

                if (time_tc_i_n in time.values) & (time_tc_i_n not in time_tc_i_list):
                    time_tc_i_list = np.append(time_tc_i_list, time_tc_i_n)

                    lat_tc_i_n = lat_tc[i, n].values
                    lon_tc_i_n = lon_tc[i, n].values

                    c = np.sin(np.radians(lat2d)) * np.sin(np.radians(lat_tc_i_n)) + np.cos(np.radians(lat2d)) * np.cos(np.radians(lat_tc_i_n)) * np.cos(np.radians(lon2d - lon_tc_i_n))
                    c = np.where((c >= -1) & (c <= 1), c, np.nan)

                    r = np.arccos(c) * 6371

                    w = 1 - np.exp(-(r / 500) * (r / 500) * np.log(4) / 2)

                    y[time.values == time_tc_i_n, :, :, :] = y[time.values == time_tc_i_n, :, :, :] * w

    y = xr.DataArray(y, dims=x.dims, coords=x.coords, attrs=x.attrs)

    return y

def calcfft2dfTLL(x, alpha=0.1, norm="forward"):    # Calculate forward 2D FFT (i.e., from temporal space to spectral space)
    # alpha is the proportion of data to be tapered (alpha = 0.1 means 5% at the beginning and 5% at the end of data is tapered to zero)
    from scipy.signal import detrend
    from scipy.signal.windows import tukey

    x = detrend(x, axis=0)
    x = (x.T * tukey(x.shape[0], alpha=alpha)).T

    cf = np.fft.rfft2(x, axes=(2, 0), norm=norm)

    return cf

def calcfft2dbTLL(x, ntime, norm="forward"):    # Calculate backward 2D FFT (i.e., from spectral space back to temporal space)
    y = np.real(np.fft.irfft2(x, s=[x.shape[2], ntime], axes=(2, 0), norm=norm))

    return y

# Two functions below are basically the same as the two functions above but for 4D shaped array

def calcfft2dfTLLL(x, alpha=0.1, norm="forward"):
    from scipy.signal import detrend
    from scipy.signal.windows import tukey

    x = detrend(x, axis=0)
    x = (x.T * tukey(x.shape[0], alpha=alpha)).T

    cf = np.fft.rfft2(x, axes=(3, 0), norm=norm)

    return cf

def calcfft2dbTLLL(x, ntime, norm="forward"):
    y = np.real(np.fft.irfft2(x, s=[x.shape[3], ntime], axes=(3, 0), norm=norm))

    return y

def calc_frq_Nmode(k, Nmode, alpha=0.9, c=50):  # At zonal wavenumber k, calculate the frequency corresponding to the Nmode value (phase speed = frequency / wavenumber)
    a = 6.371e+06

    frq = (24 * 3600) / (2 * np.pi * a) * np.sqrt(Nmode * alpha * (1 - alpha)) * c * k

    return frq

def filter_mode_TLL(x, mode, ntime, spd, frqmin=1/120, kmax=20):    # Retain wavenumber and frequency belonging to a defined mode and set the rest to zero, by default only synoptic to planetary scale signals are considered (frequency > 1/120 cycles per day and wavenumber < 20)
    cf = calcfft2dfTLL(x.fillna(0).data)
    frq = np.fft.rfftfreq(x.data.shape[0], d=1/spd)

    cf[frq < frqmin, :, :] = 0
    cf[:, :, kmax+1:-kmax] = 0
    cf[:, :, 0] = 0

    if mode == "Moisture Mode":
        for k in range(-kmax, kmax+1):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[frq >= frq_top, :, k] = 0
    elif mode == "Mixed System":
        for k in range(-kmax, kmax+1):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[(frq >= frq_top) | (frq < frq_bottom), :, k] = 0
    elif mode == "IG Wave":
        for k in range(-kmax, kmax+1):
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            cf[frq < frq_bottom, :, k] = 0
    elif mode == "Westward Moisture Mode":
        for k in range(-kmax, 0):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[frq >= frq_top, :, k] = 0
    elif mode == "Eastward Moisture Mode":
        for k in range(1, kmax+1):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[frq >= frq_top, :, k] = 0
    elif mode == "Westward Mixed System":
        for k in range(-kmax, 0):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[(frq >= frq_top) | (frq < frq_bottom), :, k] = 0
    elif mode == "Eastward Mixed System":
        for k in range(1, kmax+1):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[(frq >= frq_top) | (frq < frq_bottom), :, k] = 0
    elif mode == "Westward IG Wave":
        for k in range(-kmax, 0):
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            cf[frq < frq_bottom, :, k] = 0
    elif mode == "Eastward IG Wave":
        for k in range(1, kmax+1):
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            cf[frq < frq_bottom, :, k] = 0

    y = calcfft2dbTLL(cf, ntime)

    y = xr.DataArray(y, dims=x.dims, coords=x.coords, attrs=dict(long_name=mode+" in "+x.attrs["long_name"], units=x.attrs["units"]))

    return y

def filter_mode_TLLL(x, mode, ntime, spd, frqmin=1/120, kmax=20):
    cf = calcfft2dfTLLL(x.fillna(0).data)
    frq = np.fft.rfftfreq(x.data.shape[0], d=1/spd)

    cf[frq < frqmin, :, :, :] = 0
    cf[:, :, :, kmax+1:-kmax] = 0
    cf[:, :, :, 0] = 0

    if mode == "Moisture Mode":
        for k in range(-kmax, kmax+1):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[frq >= frq_top, :, :, k] = 0
    elif mode == "Mixed System":
        for k in range(-kmax, kmax+1):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[(frq >= frq_top) | (frq < frq_bottom), :, :, k] = 0
    elif mode == "IG Wave":
        for k in range(-kmax, kmax+1):
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            cf[frq < frq_bottom, :, :, k] = 0
    elif mode == "Westward Moisture Mode":
        for k in range(-kmax, 0):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[frq >= frq_top, :, :, k] = 0
    elif mode == "Eastward Moisture Mode":
        for k in range(1, kmax+1):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[frq >= frq_top, :, :, k] = 0
    elif mode == "Westward Mixed System":
        for k in range(-kmax, 0):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[(frq >= frq_top) | (frq < frq_bottom), :, :, k] = 0
    elif mode == "Eastward Mixed System":
        for k in range(1, kmax+1):
            frq_top = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** -0.5)
            cf[(frq >= frq_top) | (frq < frq_bottom), :, :, k] = 0
    elif mode == "Westward IG Wave":
        for k in range(-kmax, 0):
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            cf[frq < frq_bottom, :, :, k] = 0
    elif mode == "Eastward IG Wave":
        for k in range(1, kmax+1):
            frq_bottom = calc_frq_Nmode(np.abs(k), 10 ** 0.5)
            cf[frq < frq_bottom, :, :, k] = 0

    y = calcfft2dbTLLL(cf, ntime)

    y = xr.DataArray(y, dims=x.dims, coords=x.coords, attrs=dict(long_name=mode+" in "+x.attrs["long_name"], units=x.attrs["units"]))

    return y
