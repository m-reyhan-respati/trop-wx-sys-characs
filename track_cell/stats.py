import numpy as np
import pandas as pd
import xarray as xr
import scipy

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
