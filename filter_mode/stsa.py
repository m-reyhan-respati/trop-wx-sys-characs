import numpy as np
import pandas as pd
import xarray as xr

import filter_mode.filter

def decSymAsym(field, lat):
    field_nh = field[:, lat > 0, :]
    field_sh = field[:, lat < 0, :][:, ::-1, :]
    
    field_sym = (field_nh + field_sh) / 2.0
    field_asy = (field_nh - field_sh) / 2.0
    
    return field_sym, field_asy

def calc2dfftfTLL(field, alpha=0.1, norm="forward"):
    from scipy.signal import detrend
    from scipy.signal.windows import tukey
    
    x = detrend(field, axis=0)
    x = (x.T * tukey(field.shape[0], alpha)).T
    
    cf = np.fft.rfft2(x, axes=(2, 0), norm=norm)
    
    return cf

def calcPowerTLL(field, spd, nDayWin, nDayOvl, alpha=0.1, norm="forward"):
    nWin = (field.shape[0] - nDayWin * spd) // (nDayWin * spd - nDayOvl * spd) + 1
    
    tStart = 0
    tEnd = tStart + nDayWin * spd - 1
    
    for i in range(0, nWin):
        x = field[tStart:tEnd+1, :, :]
        cf = calc2dfftfTLL(x, alpha, norm)
        
        if i == 0:
            power = np.abs(cf) ** 2
        else:
            power += np.abs(cf) ** 2
        
        tStart = tEnd - nDayOvl * spd + 1
        tEnd = tStart + nDayWin * spd - 1
    
    power /= nWin
    power = power.sum(axis=1)
    
    return power

def shift_k(power):
    sts = np.zeros((power.shape[0], power.shape[1] + 1), dtype=power.dtype)
    
    sts[:, 0:sts.shape[1]//2+1] = power[:, 0:power.shape[1]//2+1][:, ::-1]
    sts[:, sts.shape[1]//2+1:] = power[:, power.shape[1]//2:][:, ::-1]
    
    return sts

def smooth121(x):
    w = [0.25, 0.5, 0.25]

    smooth = np.copy(x)
    
    for i in range(0, len(x)):
        if i==0:
            smooth[i] = w[0] * x[i+1] + w[1] * x[i] + w[2] * x[i+1]
        elif i==(len(x)-1):
            smooth[i] = w[0] * x[i-1] + w[1] * x[i] + w[2] * x[i-1]
        else:
            smooth[i] = w[0] * x[i-1] + w[1] * x[i] + w[2] * x[i+1]
    
    return smooth

def stsaTLL(field, lat, spd, nDayWin, nDayOvl, decompose=True, divideByBG=True, returnBG=False, alpha=0.1, norm="forward"):
    field_sym, field_asy = decSymAsym(field, lat)
    
    power_sym = calcPowerTLL(field_sym, spd, nDayWin, nDayOvl, alpha, norm) * 2
    power_asy = calcPowerTLL(field_asy, spd, nDayWin, nDayOvl, alpha, norm) * 2
    
    power_sym = shift_k(power_sym)
    power_asy = shift_k(power_asy)
    
    power_bg = (power_sym + power_asy) / 2
    
    frq = np.fft.rfftfreq(nDayWin * spd, d=1/spd)
    k = np.linspace(-field.shape[2]//2, field.shape[2]//2, field.shape[2]+1)
    
    for n in range(0, 40):
        for i in range(0, len(frq)):
            power_bg[i, :] = smooth121(power_bg[i, :])
        for i in range(0, len(k)):
            power_bg[:, i] = smooth121(power_bg[:, i])
    
    if decompose:
        if divideByBG:
            power_sym /= power_bg
            power_asy /= power_bg

        if returnBG:
            return power_sym, power_asy, power_bg, frq, k
        else:
            return power_sym, power_asy, frq, k
    else:
        power = calcPowerTLL(field, spd, nDayWin, nDayOvl, alpha, norm)
        
        power = shift_k(power)

        if divideByBG:
            power /= power_bg

        if returnBG:
            return power, power_bg, frq, k
        else:
            return power, frq, k

def calc_frq_dispersion(k, h, wave):
    a = 6.371e+06
    g = 9.81
    Omega = 2 * np.pi / (24 * 3600)
    beta = 2 * Omega / a
    
    k_dim = 2 * np.pi * k / (2 * np.pi * a)
    
    c = np.sqrt(g * h)
    k_nondim = -k_dim * np.sqrt(c / beta)
    
    frq_nondim = np.empty(k_nondim.shape, dtype=k_nondim.dtype)

    if wave == "Kelvin":
        frq_nondim = -k_nondim
    elif wave == "MRG":
        frq_nondim = (-k_nondim + np.sqrt(k_nondim ** 2 + 4)) / 2
    elif wave == "ER":
        frq_nondim = k_nondim / (3 + k_nondim ** 2)
    elif wave == "IG1":
        frq_nondim = np.sqrt(3 + k_nondim ** 2)
    elif wave == "IG2":
        frq_nondim = np.sqrt(5 + k_nondim ** 2)
    else:
        sys.exit("Wave type unknown! Accepted wave type: Kelvin, MRG, ER, IG1, IG2")
    
    frq_dim = frq_nondim * np.sqrt(c * beta)
    
    frq = frq_dim * 24 * 3600 / (2 * np.pi)
    
    return frq

def calc_wave_region(k_min, k_max, frq_min, frq_max, h_min, h_max, wave):
    k = np.arange(k_min, k_max + 0.1, 0.1)
    k[-1] = k_max
    k = np.concatenate((k, k[::-1]))
    
    frq = np.zeros(len(k))
    frq[0:len(k) // 2] = frq_min
    frq[len(k) // 2:] = frq_max
    
    if h_min != None:
        if wave in ["Kelvin", "MRG", "ER", "IG1", "IG2"]:
            for i in range(0, len(k) // 2):
                frq[i] = np.max([frq[i], calc_frq_dispersion(k[i:i + 1], h=h_min, wave=wave)[0]])
    
    if h_max != None:
        if wave in ["Kelvin", "MRG", "ER", "IG1", "IG2"]:
            for i in range(len(k) // 2, len(k)):
                frq[i] = np.min([frq[i], calc_frq_dispersion(k[i:i + 1], h=h_max, wave=wave)[0]])
    
    if wave == "Moisture Mode":
        frq[len(k) // 2:] = np.abs(filter_mode.filter.calc_frq_Nmode(k[len(k) // 2:], Nmode=10.0 ** -0.5))
    elif wave == "Mixed System":
        frq[0:len(k) // 2] = np.abs(filter_mode.filter.calc_frq_Nmode(k[0:len(k) // 2], Nmode=10.0 ** -0.5))
        frq[len(k) // 2:] = np.abs(filter_mode.filter.calc_frq_Nmode(k[len(k) // 2:], Nmode=10.0 ** 0.5))
    elif wave == "IG Wave":
        frq[0:len(k) // 2] = np.abs(filter_mode.filter.calc_frq_Nmode(k[0:len(k) // 2], Nmode=10.0 ** 0.5))
    
    frq = np.where(frq < frq_min, frq_min, frq)
    frq = np.where(frq > frq_max, frq_max, frq)
    
    return np.stack((k, frq), axis=-1)