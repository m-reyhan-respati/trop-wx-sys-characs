import numpy as np
import pandas as pd
import xarray as xr
import scipy

import os
import sys

def stackTLL(field, track, max_activity, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res):
    if season == "ALL":
        max_activity_in_season = max_activity
    elif season == "DJF":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month == 12) | (pd.to_datetime(max_activity["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 3) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 6) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 8)]
    elif season == "SON":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 9) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 11)]

    if domain == "EQ":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] > -10.0) & (max_activity_in_season["latitude"] < 10.0)]
        long_name = "Instances of Equatorial " + field.attrs["long_name"] + " (" + season + ")"
    elif domain == "NH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= 10.0) & (max_activity_in_season["latitude"] <= 20.0)]
        long_name = "Instances of Northern Hemisphere " + field.attrs["long_name"] + " (" + season + ")"
    elif domain == "SH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= -20.0) & (max_activity_in_season["latitude"] <= -10.0)]
        long_name = "Instances of Southern Hemisphere " + field.attrs["long_name"] + " (" + season + ")"

    n_Features = len(max_activity_in_domain["timestr"].values)

    x = np.arange(-x_size, x_size + grid_res, grid_res)
    y = np.arange(-y_size, y_size + grid_res, grid_res)

    instances = np.zeros((n_Features, 2 * lagmax + 1, len(y), len(x))) * np.nan

    lat3 = np.zeros(len(lat.values) + int(2*y_size/grid_res))
    lat3[0:int(y_size/grid_res)] = np.min(lat.values) + y[0:int(y_size/grid_res)]
    lat3[int(y_size/grid_res):-int(y_size/grid_res)] = lat.values
    lat3[-int(y_size/grid_res):] = np.max(lat.values) + y[-int(y_size/grid_res):]

    lon3 = np.zeros(len(lon.values) * 3)
    lon3[0*len(lon.values):1*len(lon.values)] = lon.values - 360.0
    lon3[1*len(lon.values):2*len(lon.values)] = lon.values
    lon3[2*len(lon.values):3*len(lon.values)] = lon.values + 360.0
    
    for i in range(0, n_Features):
        track_i = track.loc[track["cell"] == max_activity_in_domain["cell"].values[i]]
        max_activity_i = max_activity_in_domain.loc[max_activity_in_domain["cell"] == max_activity_in_domain["cell"].values[i]]
        max_activity_index = np.where(track_i["timestr"].values == max_activity_i["timestr"].values)[0][0]

        time_i = track_i["timestr"].values
        lat_i = track_i["latitude"].values
        lon_i = track_i["longitude"].values

        field_i = np.zeros((len(time_i), len(lat3), len(lon3))) * np.nan
        field_i[:, int(y_size/grid_res):-int(y_size/grid_res), 0*len(lon.values):1*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values
        field_i[:, int(y_size/grid_res):-int(y_size/grid_res), 1*len(lon.values):2*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values
        field_i[:, int(y_size/grid_res):-int(y_size/grid_res), 2*len(lon.values):3*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values

        for lag in range(-lagmax, lagmax + 1):
            if (max_activity_index + lag >= 0) & (max_activity_index + lag < len(time_i)):
                lat_i_lag = lat_i[max_activity_index + lag]
                lon_i_lag = lon_i[max_activity_index + lag]

                lat_i_lag_index = np.argmin(np.abs(lat3 - lat_i_lag))
                lon_i_lag_index = np.argmin(np.abs(lon3 - lon_i_lag))

                instances[i, lag + lagmax, :, :] = field_i[max_activity_index + lag, lat_i_lag_index-int(y_size/grid_res):lat_i_lag_index+int(y_size/grid_res)+1, lon_i_lag_index-int(x_size/grid_res):lon_i_lag_index+int(x_size/grid_res)+1]

    n = np.arange(1, n_Features + 1)
    l = np.arange(-lagmax, lagmax + 1, 1) * time_res

    x = xr.DataArray(x, dims=["lon"], coords={"lon": x}, attrs={"long_name": "Relative Longitude", "units": "degrees_east"})
    y = xr.DataArray(y, dims=["lat"], coords={"lat": y}, attrs={"long_name": "Relative Latitude", "units": "degrees_north"})
    l = xr.DataArray(l, dims=["lag"], coords={"lag": l}, attrs={"long_name": "Lag", "units": "hour"})
    n = xr.DataArray(n, dims=["n"], coords={"n": n}, attrs={"long_name": "Instance #"})

    instances = xr.DataArray(instances, dims=["n", "lag", "lat", "lon"], coords={"n": n, "lag": l, "lat": y, "lon": x}, attrs={"long_name": long_name, "units": field.attrs["units"]})

    return instances

def stackTLLL(field, track, max_activity, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res):
    if season == "ALL":
        max_activity_in_season = max_activity
    elif season == "DJF":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month == 12) | (pd.to_datetime(max_activity["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 3) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 6) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 8)]
    elif season == "SON":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 9) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 11)]

    if domain == "EQ":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] > -10.0) & (max_activity_in_season["latitude"] < 10.0)]
        long_name = "Instances of Equatorial " + field.attrs["long_name"] + " (" + season + ")"
    elif domain == "NH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= 10.0) & (max_activity_in_season["latitude"] <= 20.0)]
        long_name = "Instances of Northern Hemisphere " + field.attrs["long_name"] + " (" + season + ")"
    elif domain == "SH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= -20.0) & (max_activity_in_season["latitude"] <= -10.0)]
        long_name = "Instances of Southern Hemisphere " + field.attrs["long_name"] + " (" + season + ")"

    n_Features = len(max_activity_in_domain["timestr"].values)

    x = np.arange(-x_size, x_size + grid_res, grid_res)
    y = np.arange(-y_size, y_size + grid_res, grid_res)

    instances = np.zeros((n_Features, 2 * lagmax + 1, len(field[field.dims[1]]), len(y), len(x))) * np.nan

    lat3 = np.zeros(len(lat.values) + int(2*y_size/grid_res))
    lat3[0:int(y_size/grid_res)] = np.max(lat.values) - y[0:int(y_size/grid_res)]
    lat3[int(y_size/grid_res):-int(y_size/grid_res)] = lat.values
    lat3[-int(y_size/grid_res):] = np.min(lat.values) - y[-int(y_size/grid_res):]

    lon3 = np.zeros(len(lon.values) * 3)
    lon3[0*len(lon.values):1*len(lon.values)] = lon.values - 360.0
    lon3[1*len(lon.values):2*len(lon.values)] = lon.values
    lon3[2*len(lon.values):3*len(lon.values)] = lon.values + 360.0
    
    for i in range(0, n_Features):
        track_i = track.loc[track["cell"] == max_activity_in_domain["cell"].values[i]]
        max_activity_i = max_activity_in_domain.loc[max_activity_in_domain["cell"] == max_activity_in_domain["cell"].values[i]]
        max_activity_index = np.where(track_i["timestr"].values == max_activity_i["timestr"].values)[0][0]

        time_i = track_i["timestr"].values
        lat_i = track_i["latitude"].values
        lon_i = track_i["longitude"].values

        field_i = np.zeros((len(time_i), len(field[field.dims[1]]), len(lat3), len(lon3))) * np.nan
        field_i[:, :, int(y_size/grid_res):-int(y_size/grid_res), 0*len(lon.values):1*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values
        field_i[:, :, int(y_size/grid_res):-int(y_size/grid_res), 1*len(lon.values):2*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values
        field_i[:, :, int(y_size/grid_res):-int(y_size/grid_res), 2*len(lon.values):3*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values

        for lag in range(-lagmax, lagmax + 1):
            if (max_activity_index + lag >= 0) & (max_activity_index + lag < len(time_i)):
                lat_i_lag = lat_i[max_activity_index + lag]
                lon_i_lag = lon_i[max_activity_index + lag]

                lat_i_lag_index = np.argmin(np.abs(lat3 - lat_i_lag))
                lon_i_lag_index = np.argmin(np.abs(lon3 - lon_i_lag))

                instances[i, lag + lagmax, :, :, :] = field_i[max_activity_index + lag, :, lat_i_lag_index-int(y_size/grid_res):lat_i_lag_index+int(y_size/grid_res)+1, lon_i_lag_index-int(x_size/grid_res):lon_i_lag_index+int(x_size/grid_res)+1]

    n = np.arange(1, n_Features + 1)
    l = np.arange(-lagmax, lagmax + 1, 1) * time_res

    x = xr.DataArray(x, dims=["lon"], coords={"lon": x}, attrs={"long_name": "Relative Longitude", "units": "degrees_east"})
    y = xr.DataArray(y, dims=["lat"], coords={"lat": y}, attrs={"long_name": "Relative Latitude", "units": "degrees_north"})
    l = xr.DataArray(l, dims=["lag"], coords={"lag": l}, attrs={"long_name": "Lag", "units": "hour"})
    n = xr.DataArray(n, dims=["n"], coords={"n": n}, attrs={"long_name": "Instance #"})

    instances = xr.DataArray(instances, dims=["n", "lag", field.dims[1], "lat", "lon"], coords={"n": n, "lag": l, field.dims[1]: field[field.dims[1]], "lat": y, "lon": x}, attrs={"long_name": long_name, "units": field.attrs["units"]})

    return instances

def compositeTLL(field, track, max_activity, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res):
    x = stackTLL(field, track, max_activity, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res)

    composite = x.mean(dim="n", skipna=True, keep_attrs=True)
    composite = composite.assign_attrs({"long_name": "Composite" + x.attrs["long_name"][9:]})

    ttest = scipy.stats.ttest_1samp(x.values, np.zeros((len(x[x.dims[1]]), len(x[x.dims[2]]), len(x[x.dims[3]])), dtype=x.values.dtype), axis=0, nan_policy="omit")
    pvalue = xr.DataArray(ttest.pvalue, dims=composite.dims, coords=composite.coords, attrs={"long_name": "p-value of the Two Sided t Statistics of " + composite.attrs["long_name"]})

    return composite, pvalue

def compositeTLLL(field, track, max_activity, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res):
    x = stackTLLL(field, track, max_activity, lat, lon, season, domain, x_size, y_size, grid_res, lagmax, time_res)

    composite = x.mean(dim="n", skipna=True, keep_attrs=True)
    composite = composite.assign_attrs({"long_name": "Composite" + x.attrs["long_name"][9:]})

    ttest = scipy.stats.ttest_1samp(x.values, np.zeros((len(x[x.dims[1]]), len(x[x.dims[2]]), len(x[x.dims[3]]), len(x[x.dims[4]])), dtype=x.values.dtype), axis=0, nan_policy="omit")
    pvalue = xr.DataArray(ttest.pvalue, dims=composite.dims, coords=composite.coords, attrs={"long_name": "p-value of the Two Sided t Statistics of " + composite.attrs["long_name"]})

    return composite, pvalue

def stackTLL_gpm(field, track, max_activity, lat, lon, x_size, y_size, grid_res, lagmax, time_res):
    if "long_name" in field.attrs:
        long_name = field.attrs["long_name"]
    else:
        long_name = ""
    
    long_name = "Instances of " + long_name

    n_Features = len(max_activity["timestr"].values)

    x = np.arange(-x_size, x_size + grid_res, grid_res)
    y = np.arange(-y_size, y_size + grid_res, grid_res)
    
    instances = np.zeros((n_Features, 2 * lagmax + 1, len(y), len(x))) * np.nan

    lat3 = np.zeros(len(lat.values) + int(2*y_size/grid_res))
    lat3[0:int(y_size/grid_res)] = np.min(lat.values) + y[0:int(y_size/grid_res)]
    lat3[int(y_size/grid_res):-int(y_size/grid_res)] = lat.values
    lat3[-int(y_size/grid_res):] = np.max(lat.values) + y[-int(y_size/grid_res):]
    
    lon3 = np.zeros(len(lon.values) * 3)
    lon3[0*len(lon.values):1*len(lon.values)] = lon.values - 360.0
    lon3[1*len(lon.values):2*len(lon.values)] = lon.values
    lon3[2*len(lon.values):3*len(lon.values)] = lon.values + 360.0
    
    for i in range(0, n_Features):
        track_i = track.loc[track["cell"] == max_activity["cell"].values[i]]
        max_activity_i = max_activity.loc[max_activity["cell"] == max_activity["cell"].values[i]]
        max_activity_index = np.where(track_i["timestr"].values == max_activity_i["timestr"].values)[0][0]
        
        time_i = track_i["timestr"].values
        lat_i = track_i["latitude"].values
        lon_i = track_i["longitude"].values

        field_i = np.zeros((len(time_i), len(lat3), len(lon3))) * np.nan
        field_i[:, int(y_size/grid_res):-int(y_size/grid_res), 0*len(lon.values):1*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values
        field_i[:, int(y_size/grid_res):-int(y_size/grid_res), 1*len(lon.values):2*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values
        field_i[:, int(y_size/grid_res):-int(y_size/grid_res), 2*len(lon.values):3*len(lon.values)] = field.sel(time=slice(time_i[0], time_i[-1])).values

        for lag in range(-lagmax, lagmax + 1):
            if (max_activity_index + lag >= 0) & (max_activity_index + lag < len(time_i)):
                lat_i_lag = lat_i[max_activity_index + lag]
                lon_i_lag = lon_i[max_activity_index + lag]
                
                lat_i_lag_index = np.argmin(np.abs(lat3 - lat_i_lag))
                lon_i_lag_index = np.argmin(np.abs(lon3 - lon_i_lag))
                
                instances[i, lag + lagmax, :, :] = field_i[max_activity_index + lag, lat_i_lag_index-int(y_size/grid_res):lat_i_lag_index+int(y_size/grid_res)+1, lon_i_lag_index-int(x_size/grid_res):lon_i_lag_index+int(x_size/grid_res)+1]

    n = max_activity["cell"].values
    l = np.arange(-lagmax, lagmax + 1, 1) * time_res

    x = xr.DataArray(x, dims=["lon"], coords={"lon": x}, attrs={"long_name": "Relative Longitude", "units": "degrees_east"})
    y = xr.DataArray(y, dims=["lat"], coords={"lat": y}, attrs={"long_name": "Relative Latitude", "units": "degrees_north"})
    l = xr.DataArray(l, dims=["lag"], coords={"lag": l}, attrs={"long_name": "Lag", "units": "hour"})
    n = xr.DataArray(n, dims=["n"], coords={"n": n}, attrs={"long_name": "Instance #"})

    instances = xr.DataArray(instances, dims=["n", "lag", "lat", "lon"], coords={"n": n, "lag": l, "lat": y, "lon": x}, attrs={"long_name": long_name, "units": field.attrs["units"]})

    return instances

def compositeTLL_gpm(x, max_activity, season, domain):
    if "long_name" in x.attrs:
        long_name = x.attrs["long_name"]
    else:
        long_name = ""
    
    if season == "ALL":
        max_activity_in_season = max_activity
    elif season == "DJF":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month == 12) | (pd.to_datetime(max_activity["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 3) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 6) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 8)]
    elif season == "SON":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 9) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 11)]

    if domain == "EQ":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] > -10.0) & (max_activity_in_season["latitude"] < 10.0)]
        long_name = "Composite of Equatorial " + long_name + " (" + season + ")"
    elif domain == "NH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= 10.0) & (max_activity_in_season["latitude"] <= 20.0)]
        long_name = "Composite of Northern Hemisphere " + long_name + " (" + season + ")"
    elif domain == "SH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= -20.0) & (max_activity_in_season["latitude"] <= -10.0)]
        long_name = "Composite of Southern Hemisphere " + long_name + " (" + season + ")"

    composite = x[np.isin(x["n"].values, max_activity_in_domain["cell"].values), ...].mean(dim="n", skipna=True, keep_attrs=True)
    composite = composite.assign_attrs({"long_name": long_name})

    ttest = scipy.stats.ttest_1samp(x[np.isin(x["n"].values, max_activity_in_domain["cell"].values), ...].values, np.zeros((len(x[x.dims[1]]), len(x[x.dims[2]]), len(x[x.dims[3]])), dtype=x.values.dtype), axis=0, nan_policy="omit")
    pvalue = xr.DataArray(ttest.pvalue, dims=composite.dims, coords=composite.coords, attrs={"long_name": "p-value of the Two Sided t Statistics of " + composite.attrs["long_name"]})

    return composite, pvalue

'''
def calc_extreme_mask_gpm(field, track, max_activity, threshold, lat, lon, x_size, y_size, grid_res, lagmax, time_res, seasonal_threshold=False, raining=False):
    seasons = {1: "DJF", 2: "DJF", 3: "MAM", 4: "MAM", 5: "MAM", 6: "JJA", 7: "JJA", 8: "JJA", 9: "SON", 10: "SON", 11: "SON", 12: "DJF"}
    
    if "long_name" in field.attrs:
        long_name = field.attrs["long_name"]
    else:
        long_name = ""
    
    long_name = "Extreme Event of " + long_name

    n_Features = len(max_activity["timestr"].values)

    x = np.arange(-x_size, x_size + grid_res, grid_res)
    y = np.arange(-y_size, y_size + grid_res, grid_res)
    
    instances = np.zeros((n_Features, 2 * lagmax + 1, len(y), len(x))) * np.nan

    lat3 = np.zeros(len(lat.values) + int(2*y_size/grid_res))
    lat3[0:int(y_size/grid_res)] = np.min(lat.values) + y[0:int(y_size/grid_res)]
    lat3[int(y_size/grid_res):-int(y_size/grid_res)] = lat.values
    lat3[-int(y_size/grid_res):] = np.max(lat.values) + y[-int(y_size/grid_res):]
    
    lon3 = np.zeros(len(lon.values) * 3)
    lon3[0*len(lon.values):1*len(lon.values)] = lon.values - 360.0
    lon3[1*len(lon.values):2*len(lon.values)] = lon.values
    lon3[2*len(lon.values):3*len(lon.values)] = lon.values + 360.0
    
    for i in range(0, n_Features):
        track_i = track.loc[track["cell"] == max_activity["cell"].values[i]]
        
        max_activity_i = max_activity.loc[max_activity["cell"] == max_activity["cell"].values[i]]
        max_activity_index = np.where(track_i["timestr"].values == max_activity_i["timestr"].values)[0][0]
        
        time_i = track_i["timestr"].values
        lat_i = track_i["latitude"].values
        lon_i = track_i["longitude"].values

        for lag in range(-lagmax, lagmax + 1):
            if (max_activity_index + lag >= 0) & (max_activity_index + lag < len(time_i)):
                time_i_lag = time_i[max_activity_index + lag]
                lat_i_lag = lat_i[max_activity_index + lag]
                lon_i_lag = lon_i[max_activity_index + lag]
                
                field_i_lag = np.zeros((len(lat3), len(lon3))) * np.nan
                
                field_i_lag[int(y_size/grid_res):-int(y_size/grid_res), 0*len(lon.values):1*len(lon.values)] = field.sel(time=time_i_lag).values
                field_i_lag[int(y_size/grid_res):-int(y_size/grid_res), 1*len(lon.values):2*len(lon.values)] = field.sel(time=time_i_lag).values
                field_i_lag[int(y_size/grid_res):-int(y_size/grid_res), 2*len(lon.values):3*len(lon.values)] = field.sel(time=time_i_lag).values
                
                lat_i_lag_index = np.argmin(np.abs(lat3 - lat_i_lag))
                lon_i_lag_index = np.argmin(np.abs(lon3 - lon_i_lag))
                
                instances_i_lag = field_i_lag[lat_i_lag_index-int(y_size/grid_res):lat_i_lag_index+int(y_size/grid_res)+1, lon_i_lag_index-int(x_size/grid_res):lon_i_lag_index+int(x_size/grid_res)+1]
                
                if seasonal_threshold == True:
                    season_i_lag = seasons[pd.to_datetime(time_i_lag).month]
                else:
                    season_i_lag = "ALL"
                
                threshold3 = np.zeros((len(lat3), len(lon3))) * np.nan
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 0*len(lon.values):1*len(lon.values)] = threshold.sel(season=season_i_lag).values
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 1*len(lon.values):2*len(lon.values)] = threshold.sel(season=season_i_lag).values
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 2*len(lon.values):3*len(lon.values)] = threshold.sel(season=season_i_lag).values
                
                threshold_i_lag = threshold3[lat_i_lag_index-int(y_size/grid_res):lat_i_lag_index+int(y_size/grid_res)+1, lon_i_lag_index-int(x_size/grid_res):lon_i_lag_index+int(x_size/grid_res)+1]
                
                if raining == True:
                    instances_i_lag = np.where(instances_i_lag > 0.0, instances_i_lag, np.nan)
                else:
                    instances_i_lag = np.where(threshold_i_lag > 0.0, instances_i_lag, np.nan)
                
                instances_i_lag = np.where((~np.isnan(instances_i_lag)) & (instances_i_lag < threshold_i_lag), 0.0, instances_i_lag)
                instances_i_lag = np.where(instances_i_lag >= threshold_i_lag, 1.0, instances_i_lag)
                
                instances[i, lag + lagmax, :, :] = instances_i_lag

    n = max_activity["cell"].values
    l = np.arange(-lagmax, lagmax + 1, 1) * time_res

    x = xr.DataArray(x, dims=["lon"], coords={"lon": x}, attrs={"long_name": "Relative Longitude", "units": "degrees_east"})
    y = xr.DataArray(y, dims=["lat"], coords={"lat": y}, attrs={"long_name": "Relative Latitude", "units": "degrees_north"})
    l = xr.DataArray(l, dims=["lag"], coords={"lag": l}, attrs={"long_name": "Lag", "units": "hour"})
    n = xr.DataArray(n, dims=["n"], coords={"n": n}, attrs={"long_name": "Instance #"})

    instances = xr.DataArray(instances, dims=["n", "lag", "lat", "lon"], coords={"n": n, "lag": l, "lat": y, "lon": x}, attrs={"long_name": long_name})

    return instances

def calc_extreme_probability_gpm(x, max_activity, season, domain, H_0):
    if "long_name" in x.attrs:
        long_name = x.attrs["long_name"]
    else:
        long_name = ""

    if season == "ALL":
        max_activity_in_season = max_activity
    elif season == "DJF":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month == 12) | (pd.to_datetime(max_activity["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 3) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 6) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 8)]
    elif season == "SON":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 9) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 11)]

    if domain == "EQ":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] > -10.0) & (max_activity_in_season["latitude"] < 10.0)]
        long_name = "Probability of " + long_name + " (" + season + ", EQ)"
    elif domain == "NH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= 10.0) & (max_activity_in_season["latitude"] <= 20.0)]
        long_name = "Probability of " + long_name + " (" + season + ", NH)"
    elif domain == "SH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= -20.0) & (max_activity_in_season["latitude"] <= -10.0)]
        long_name = "Probability of " + long_name + " (" + season + ", SH)"

    composite = x[np.isin(x["n"].values, max_activity_in_domain["cell"].values), ...].mean(dim="n", skipna=True, keep_attrs=True)
    #composite = x[np.isin(x["n"].values, max_activity_in_domain["cell"].values), ...].mean(dim="n", skipna=False, keep_attrs=True)
    composite = composite.assign_attrs({"long_name": long_name})

    ttest = scipy.stats.ttest_1samp(x[np.isin(x["n"].values, max_activity_in_domain["cell"].values), ...].values, np.ones((len(x[x.dims[1]]), len(x[x.dims[2]]), len(x[x.dims[3]])), dtype=x.values.dtype) * H_0, axis=0, nan_policy="omit")
    #ttest = scipy.stats.ttest_1samp(x[np.isin(x["n"].values, max_activity_in_domain["cell"].values), ...].values, np.ones((len(x[x.dims[1]]), len(x[x.dims[2]]), len(x[x.dims[3]])), dtype=x.values.dtype) * H_0, axis=0, nan_policy="propagate")
    pvalue = xr.DataArray(ttest.pvalue, dims=composite.dims, coords=composite.coords, attrs={"long_name": "p-value of the Two Sided t Statistics of " + composite.attrs["long_name"]})

    return composite, pvalue
'''

def calc_extreme_probability_gpm(stack, track, max_activity, season, domain, threshold, lat, lon, x_size, y_size, grid_res, time_res, H_0, seasonal_threshold=False, raining=False):
    seasons = {1: "DJF", 2: "DJF", 3: "MAM", 4: "MAM", 5: "MAM", 6: "JJA", 7: "JJA", 8: "JJA", 9: "SON", 10: "SON", 11: "SON", 12: "DJF"}
    
    if "long_name" in stack.attrs:
        long_name = stack.attrs["long_name"][13:]
    else:
        long_name = ""
    
    if season == "ALL":
        max_activity_in_season = max_activity
    elif season == "DJF":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month == 12) | (pd.to_datetime(max_activity["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 3) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 6) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 8)]
    elif season == "SON":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 9) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 11)]

    if domain == "EQ":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] > -10.0) & (max_activity_in_season["latitude"] < 10.0)]
    elif domain == "NH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= 10.0) & (max_activity_in_season["latitude"] <= 20.0)]
    elif domain == "SH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= -20.0) & (max_activity_in_season["latitude"] <= -10.0)]
     
    long_name = f"Probability of Extreme Event of {long_name} ({season}, {domain})"
    
    stack = stack[np.isin(stack["n"].values, max_activity_in_domain["cell"].values), ...]
    
    lags = stack["lag"].values / time_res

    n_Features = len(stack["n"].values)
    
    instances = np.zeros(stack.values.shape) * np.nan

    lat3 = np.zeros(len(lat.values) + int(2*y_size/grid_res))
    lat3[0:int(y_size/grid_res)] = np.min(lat.values) + stack["lat"].values[0:int(y_size/grid_res)]
    lat3[int(y_size/grid_res):-int(y_size/grid_res)] = lat.values
    lat3[-int(y_size/grid_res):] = np.max(lat.values) + stack["lat"].values[-int(y_size/grid_res):]
    
    lon3 = np.zeros(len(lon.values) * 3)
    lon3[0*len(lon.values):1*len(lon.values)] = lon.values - 360.0
    lon3[1*len(lon.values):2*len(lon.values)] = lon.values
    lon3[2*len(lon.values):3*len(lon.values)] = lon.values + 360.0
    
    for i in range(0, n_Features):
        track_i = track.loc[track["cell"] == stack["n"].values[i]]
        
        max_activity_i = max_activity_in_domain.loc[max_activity_in_domain["cell"] == stack["n"].values[i]]
        max_activity_index = np.where(track_i["timestr"].values == max_activity_i["timestr"].values)[0][0]
        
        time_i = track_i["timestr"].values
        lat_i = track_i["latitude"].values
        lon_i = track_i["longitude"].values

        for j, lag in enumerate(lags):
            if (max_activity_index + lag >= 0) & (max_activity_index + lag < len(time_i)):
                time_i_lag = time_i[max_activity_index + int(lag)]
                lat_i_lag = lat_i[max_activity_index + int(lag)]
                lon_i_lag = lon_i[max_activity_index + int(lag)]
                
                lat_i_lag_index = np.argmin(np.abs(lat3 - lat_i_lag))
                lon_i_lag_index = np.argmin(np.abs(lon3 - lon_i_lag))
                
                instances_i_lag = stack.sel(n=stack["n"].values[i], lag=lag).values
                
                if seasonal_threshold == True:
                    season_i_lag = seasons[pd.to_datetime(time_i_lag).month]
                else:
                    season_i_lag = "ALL"
                
                threshold3 = np.zeros((len(lat3), len(lon3))) * np.nan
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 0*len(lon.values):1*len(lon.values)] = threshold.sel(season=season_i_lag).values
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 1*len(lon.values):2*len(lon.values)] = threshold.sel(season=season_i_lag).values
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 2*len(lon.values):3*len(lon.values)] = threshold.sel(season=season_i_lag).values
                
                threshold_i_lag = threshold3[lat_i_lag_index-int(y_size/grid_res):lat_i_lag_index+int(y_size/grid_res)+1, lon_i_lag_index-int(x_size/grid_res):lon_i_lag_index+int(x_size/grid_res)+1]
                
                if raining == True:
                    instances_i_lag = np.where(instances_i_lag > 0.0, instances_i_lag, np.nan)
                else:
                    instances_i_lag = np.where(threshold_i_lag > 0.0, instances_i_lag, np.nan)
                
                instances_i_lag = np.where((~np.isnan(instances_i_lag)) & (instances_i_lag < threshold_i_lag), 0.0, instances_i_lag)
                instances_i_lag = np.where(instances_i_lag >= threshold_i_lag, 1.0, instances_i_lag)
                
                instances[i, j, :, :] = instances_i_lag
    
    probability = np.nanmean(instances, axis=0)
    
    ttest = scipy.stats.ttest_1samp(instances, np.ones((instances.shape[1], instances.shape[2], instances.shape[3]), dtype=instances.dtype) * H_0, axis=0, nan_policy="omit")
    
    probability = xr.DataArray(probability, dims=[stack.dims[1], stack.dims[2], stack.dims[3]], coords={stack.dims[1]: stack[stack.dims[1]], stack.dims[2]: stack[stack.dims[2]], stack.dims[3]: stack[stack.dims[3]]}, attrs={"long_name": long_name})
    pvalue = xr.DataArray(ttest.pvalue, dims=probability.dims, coords=probability.coords, attrs={"long_name": "p-value of the Two Sided t Statistics of " + probability.attrs["long_name"]})

    return probability, pvalue

def calc_extreme_instance_gpm(stack, track, max_activity, season, domain, threshold, lat, lon, x_size, y_size, grid_res, time_res, H_0, seasonal_threshold=False, raining=False):
    seasons = {1: "DJF", 2: "DJF", 3: "MAM", 4: "MAM", 5: "MAM", 6: "JJA", 7: "JJA", 8: "JJA", 9: "SON", 10: "SON", 11: "SON", 12: "DJF"}
    
    if "long_name" in stack.attrs:
        long_name = stack.attrs["long_name"][13:]
    else:
        long_name = ""
    
    if season == "ALL":
        max_activity_in_season = max_activity
    elif season == "DJF":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month == 12) | (pd.to_datetime(max_activity["timestr"]).dt.month <= 2)]
    elif season == "MAM":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 3) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 5)]
    elif season == "JJA":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 6) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 8)]
    elif season == "SON":
        max_activity_in_season = max_activity.loc[(pd.to_datetime(max_activity["timestr"]).dt.month >= 9) & (pd.to_datetime(max_activity["timestr"]).dt.month <= 11)]

    if domain == "EQ":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] > -10.0) & (max_activity_in_season["latitude"] < 10.0)]
    elif domain == "NH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= 10.0) & (max_activity_in_season["latitude"] <= 20.0)]
    elif domain == "SH":
        max_activity_in_domain = max_activity_in_season.loc[(max_activity_in_season["latitude"] >= -20.0) & (max_activity_in_season["latitude"] <= -10.0)]
     
    long_name = f"Extreme Instances of {long_name} ({season}, {domain})"
    
    stack = stack[np.isin(stack["n"].values, max_activity_in_domain["cell"].values), ...]
    
    lags = stack["lag"].values / time_res

    n_Features = len(stack["n"].values)
    
    x = np.arange(-x_size, x_size + grid_res, grid_res)
    y = np.arange(-y_size, y_size + grid_res, grid_res)
    
    instances = np.zeros(stack.values.shape) * np.nan

    lat3 = np.zeros(len(lat.values) + int(2*y_size/grid_res))
    lat3[0:int(y_size/grid_res)] = np.min(lat.values) + y[0:int(y_size/grid_res)]
    lat3[int(y_size/grid_res):-int(y_size/grid_res)] = lat.values
    lat3[-int(y_size/grid_res):] = np.max(lat.values) + y[-int(y_size/grid_res):]
    
    lon3 = np.zeros(len(lon.values) * 3)
    lon3[0*len(lon.values):1*len(lon.values)] = lon.values - 360.0
    lon3[1*len(lon.values):2*len(lon.values)] = lon.values
    lon3[2*len(lon.values):3*len(lon.values)] = lon.values + 360.0
    
    for i in range(0, n_Features):
        track_i = track.loc[track["cell"] == stack["n"].values[i]]
        
        max_activity_i = max_activity_in_domain.loc[max_activity_in_domain["cell"] == stack["n"].values[i]]
        max_activity_index = np.where(track_i["timestr"].values == max_activity_i["timestr"].values)[0][0]
        
        time_i = track_i["timestr"].values
        lat_i = track_i["latitude"].values
        lon_i = track_i["longitude"].values

        for j, lag in enumerate(lags):
            if (max_activity_index + lag >= 0) & (max_activity_index + lag < len(time_i)):
                time_i_lag = time_i[max_activity_index + int(lag)]
                lat_i_lag = lat_i[max_activity_index + int(lag)]
                lon_i_lag = lon_i[max_activity_index + int(lag)]
                
                lat_i_lag_index = np.argmin(np.abs(lat3 - lat_i_lag))
                lon_i_lag_index = np.argmin(np.abs(lon3 - lon_i_lag))
                
                instances_i_lag = stack.sel(n=stack["n"].values[i], lag=lag).values
                
                if seasonal_threshold == True:
                    season_i_lag = seasons[pd.to_datetime(time_i_lag).month]
                else:
                    season_i_lag = "ALL"
                
                threshold3 = np.zeros((len(lat3), len(lon3))) * np.nan
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 0*len(lon.values):1*len(lon.values)] = threshold.sel(season=season_i_lag).values
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 1*len(lon.values):2*len(lon.values)] = threshold.sel(season=season_i_lag).values
                threshold3[int(y_size/grid_res):-int(y_size/grid_res), 2*len(lon.values):3*len(lon.values)] = threshold.sel(season=season_i_lag).values
                
                threshold_i_lag = threshold3[lat_i_lag_index-int(y_size/grid_res):lat_i_lag_index+int(y_size/grid_res)+1, lon_i_lag_index-int(x_size/grid_res):lon_i_lag_index+int(x_size/grid_res)+1]
                threshold_i_lag = threshold_i_lag[(y > np.min(stack["lat"].values) - 0.05) & (y < np.max(stack["lat"].values) + 0.05), :]
                
                if raining == True:
                    instances_i_lag = np.where(instances_i_lag > 0.0, instances_i_lag, np.nan)
                else:
                    instances_i_lag = np.where(threshold_i_lag > 0.0, instances_i_lag, np.nan)
                
                instances_i_lag = np.where((~np.isnan(instances_i_lag)) & (instances_i_lag < threshold_i_lag), 0.0, instances_i_lag)
                instances_i_lag = np.where(instances_i_lag >= threshold_i_lag, 1.0, instances_i_lag)
                
                instances[i, j, :, :] = instances_i_lag
    
    instances = xr.DataArray(instances, dims=stack.dims, coords=stack.coords, attrs={"long_name": long_name})
    
    return instances

def bootstrap_mean_ci(x, n=1000):
    BootStrap = scipy.stats.bootstrap((x,), np.nanmean, n_resamples=n, vectorized=True, axis=0)
    
    ci_low, ci_high = BootStrap.confidence_interval
    
    ci_low = xr.DataArray(ci_low, dims=[x.dims[1], x.dims[2], x.dims[3]], coords={x.dims[1]: x[x.dims[1]], x.dims[2]: x[x.dims[2]], x.dims[3]: x[x.dims[3]]})
    ci_high = xr.DataArray(ci_high, dims=[x.dims[1], x.dims[2], x.dims[3]], coords={x.dims[1]: x[x.dims[1]], x.dims[2]: x[x.dims[2]], x.dims[3]: x[x.dims[3]]})
    
    return ci_low, ci_high

def calc_core_stat_gpm(stack, max_activity, margin, stat):
    stack = stack[:, :, (stack["lat"].values > -margin - 0.05) & (stack["lat"].values < margin + 0.05), (stack["lon"].values > -margin - 0.05) & (stack["lon"].values < margin + 0.05)]
    
    print(stack)
    
    if stat == "mean":
        core_stat = stack.mean(dim=["lat", "lon"])
    elif stat == "median":
        core_stat = stack.median(dim=["lat", "lon"])
    elif stat == "min":
        core_stat = stack.min(dim=["lat", "lon"])
    elif stat == "max":
        core_stat = stack.max(dim=["lat", "lon"])
    else:
        sys.exit("stat unknown!")
    
    return core_stat
