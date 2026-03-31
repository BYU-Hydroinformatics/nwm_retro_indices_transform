#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import apache_beam as beam
import pandas as pd
import numpy as np
from scipy.stats import pearson3
from itertools import groupby
from typing import Union
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, WorkerOptions, GoogleCloudOptions, DebugOptions
import xarray_beam as xbeam
import logging
import argparse
# from streamflow_indices_computation_utils import compute_all_indices, convert_to_water_year_data
import typing
import datetime
from itertools import groupby
from typing import Union, List, Optional
import baseflow


def get_water_years(date_series: Union[pd.Series, np.ndarray, list], starting_month: int = 10) -> Union[pd.Series]:
    date_series = pd.to_datetime(date_series)
    years, months = date_series.dt.year, date_series.dt.month
    if starting_month < 7:
        water_year_series = np.where(months < starting_month, years - 1, years)
    else:
        water_year_series = np.where(months < starting_month, years, years + 1)
    water_year_series = pd.Series(water_year_series, index=date_series.index)
    return water_year_series.astype('int32')


def check_leap_year(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)   


def convert_to_water_year_data(streamflow_df: pd.DataFrame, starting_month: int = 10) -> pd.DataFrame:
    streamflow_df = streamflow_df.sort_values(by='date').reset_index(drop=True)
    streamflow_df['water_year'] = get_water_years(streamflow_df['date'], starting_month)
    
    end_water_years = (streamflow_df['water_year'].iloc[0], 
                       streamflow_df['water_year'].iloc[-1])
    
    grouped_streamflow_df = streamflow_df.groupby('water_year')

    days_in_end_water_years = (grouped_streamflow_df.get_group(end_water_years[0]).shape[0],
                                         grouped_streamflow_df.get_group(end_water_years[1]).shape[0])

    expected_days_in_end_water_years = (366 if check_leap_year(end_water_years[0]) else 365,
                                                 366 if check_leap_year(end_water_years[1]) else 365)

    if days_in_end_water_years[0] < expected_days_in_end_water_years[0]:
        streamflow_df = streamflow_df[streamflow_df['water_year'] != end_water_years[0]]
    if days_in_end_water_years[1] < expected_days_in_end_water_years[1]:
        streamflow_df = streamflow_df[streamflow_df['water_year'] != end_water_years[1]]
    
    return streamflow_df

def monthwise_mean_and_cov(streamflow_df: pd.DataFrame):
    try:
        monthwise_mean = streamflow_df.groupby(streamflow_df.index.month)['streamflow'].mean()
    except:
        monthwise_mean = [np.nan] * 12
    try:
        monthwise_std = streamflow_df.groupby(streamflow_df.index.month)['streamflow'].std()
        monthwise_cov = (monthwise_std / monthwise_mean * 100).round(2)
    except:
        monthwise_cov = [np.nan] * 12
    return monthwise_mean.round(2).to_list(), monthwise_cov.to_list()

def prob_adjustment_for_zero_flow_years(streamflow_dataframe):
    n_total_years = streamflow_dataframe['water_year'].nunique()
    n_zero_flow_years = streamflow_dataframe[streamflow_dataframe['streamflow'] < 0.001]['water_year'].nunique()
    p_zero = n_zero_flow_years / n_total_years if n_total_years > 0 else 0.0
    p_non_zero = 1 - p_zero
    if p_non_zero == 0:
        return 0.0
    p_target = 0.1
    if p_target <= p_zero:
        return 0.0
    p_adjusted = (p_target - p_zero) / p_non_zero
    p_adjusted = max(min(p_adjusted, 1.0 - 1e-6), 1e-6)
    return p_adjusted
    
def compute_7Q10_and_MAM7(streamflow_dataframe):
    rolling_window = 7
    streamflow_dataframe = streamflow_dataframe.sort_values(by='date').copy()
    streamflow_dataframe['rolling_window_mean'] = streamflow_dataframe['streamflow'].rolling(
        window=rolling_window, 
        min_periods=rolling_window, 
        center=False
    ).mean()
    yearly_min_of_rolling_window_mean = (
        streamflow_dataframe.assign(year=streamflow_dataframe['water_year'])
        .groupby('water_year')
        .agg(
            min_Q=('rolling_window_mean', 'min'),
            non_nan_count_streamflow=('streamflow', 'size'),
            nan_count_rolling_window_mean=('rolling_window_mean', lambda x: x.isnull().sum())
        )
        .reset_index()
        .query('non_nan_count_streamflow > 328 and (nan_count_rolling_window_mean / non_nan_count_streamflow) < 0.1')
    )
    mean_min_Q = yearly_min_of_rolling_window_mean['min_Q'].mean()
    try:
        p_target = prob_adjustment_for_zero_flow_years(streamflow_dataframe)
        yearly_min_of_rolling_window_mean = yearly_min_of_rolling_window_mean['min_Q'].dropna()
        yearly_min_of_rolling_window_mean = yearly_min_of_rolling_window_mean[yearly_min_of_rolling_window_mean > 0]
        logged_Q = np.log10(yearly_min_of_rolling_window_mean)
        mean_U = logged_Q.mean()
        std_S = logged_Q.std(ddof=1)
        skew_G = pd.Series(logged_Q).skew()
        freq_factor_K = pearson3.ppf(p_target, skew_G)
        z_score = mean_U + std_S * freq_factor_K
        sevenQ10 = np.power(10, z_score)
    except:
        sevenQ10 = np.nan
    return sevenQ10, mean_min_Q


def compute_bfi(streamflow_df):
    bfi = baseflow.separation(streamflow_df[['streamflow']], method="Eckhardt", return_bfi=True)[1].values[0][0]
    #total_streamflow_volume = streamflow_df['streamflow'].sum()
    #total_baseflow_volume = np.sum(baseflow_series)
    #bfi = total_baseflow_volume / total_streamflow_volume
    return bfi


def compute_percentiles(streamflow_series, percentiles_to_calculate):
    try:
        nth_percentile_flows = np.nanpercentile(streamflow_series, percentiles_to_calculate, method='weibull')
        nth_percentile_flows = nth_percentile_flows.tolist()
    except:
        nth_percentile_flows = np.full(shape=len(percentiles_to_calculate), fill_value=np.nan)
    return nth_percentile_flows


def compute_variability_index(nth_percentile_flows, percentiles_to_calculate):
    percentiles_for_vi = np.arange(5,100,5)
    nth_flow_percentiles_for_vi = [nth_percentile_flows[percentiles_to_calculate.index(p)] for p in percentiles_for_vi]
    nth_flow_percentiles_for_vi = np.array(nth_flow_percentiles_for_vi)
    nth_flow_percentiles_for_vi = [flow if flow > 0.001 else 0.001 for flow in nth_flow_percentiles_for_vi] #accomodating intermittent flows
    try:
        variability_idx = np.std(np.log10(nth_flow_percentiles_for_vi), ddof=1)
    except:
        variability_idx = np.nan
    return variability_idx

def compute_slope_fdc(nth_percentile_flows, percentiles_to_calculate):
    percentiles_for_slope = (33,66)
    try:
        Q33, Q66 = [nth_percentile_flows[percentiles_to_calculate.index(p)] for p in percentiles_for_slope]
        Q33, Q66 = np.array((Q33, Q66)).clip(0.001, None)
        slope_fdc = (np.log(Q66) - np.log(Q33)) / (percentiles_for_slope[-1] - percentiles_for_slope[0])
    except:
        slope_fdc = np.nan
    return slope_fdc

def compute_yearly_flashiness_index(streamflow_df, q_col, wy_col):
    def annual_flashiness_index(water_year_group):
        q = water_year_group[q_col].values.astype(float)
        if len(q) < 2:
            return np.nan
        fi_numerator = (np.abs(np.diff(q))).sum()
        fi_denom = q.sum()
        return fi_numerator / fi_denom if fi_denom > 0 else np.nan
    yearly_fi_values = streamflow_df.groupby(wy_col).apply(annual_flashiness_index).rename("annual_FI")
    return yearly_fi_values

def compute_flashiness_index(streamflow_df, q_col="streamflow", wy_col="water_year"):
    yearly_fi_values = compute_yearly_flashiness_index(streamflow_df, q_col=q_col, wy_col=wy_col)
    flashiness_index_group1 = yearly_fi_values.loc[1980:1989].mean(skipna=True)
    flashiness_index_group2 = yearly_fi_values.loc[1990:1999].mean(skipna=True)
    flashiness_index_group3 = yearly_fi_values.loc[2000:2009].mean(skipna=True)
    group4 = yearly_fi_values.loc[2010:2022]
    flashiness_index_group4 = group4.mean(skipna=True)
    cov_flashiness_index_group4 = group4.std(skipna=True) * 100 / group4.mean(skipna=True) if group4.mean(skipna=True) != 0 else np.nan
    return [flashiness_index_group1, flashiness_index_group2, 
            flashiness_index_group3, flashiness_index_group4, 
            cov_flashiness_index_group4]

def get_half_flow_date(yearly_series: pd.DataFrame):
    half_flow = yearly_series['streamflow'].sum() / 2
    yearly_series = yearly_series.copy()
    yearly_series['cumulative_flow'] = yearly_series['streamflow'].cumsum()
    half_flow_date = yearly_series[yearly_series['cumulative_flow'] >= half_flow].sort_index().index[0]
    return half_flow_date
def compute_half_flow_date(streamflow_df: pd.DataFrame):
    yearly_half_flow_dates = (
        streamflow_df
        .groupby('water_year')[['streamflow']]
        .apply(get_half_flow_date)
    )
    start_dates = [pd.to_datetime(f"{wy}-01-01") for wy in yearly_half_flow_dates.index]
    yearly_half_flow_day_of_the_year = [(half_flow_date - wy_start).days + 1 for half_flow_date, wy_start in zip(yearly_half_flow_dates, start_dates)]
    mean_half_flow_date = np.mean(yearly_half_flow_day_of_the_year)
    sd_half_flow_date = np.std(yearly_half_flow_day_of_the_year)
    cov_half_flow_date = sd_half_flow_date * 100 / mean_half_flow_date if mean_half_flow_date != 0 else np.nan
    return mean_half_flow_date, cov_half_flow_date


def count_zero_flow_days(streamflow_df):
    try:
        is_zero_flow = (streamflow_df['streamflow'] < 0.001)
        streamflow_df['is_zero_flow'] = is_zero_flow.astype(int)
        yearly_zero_flow_counts = streamflow_df.groupby('water_year')['is_zero_flow'].sum()
        zero_flow_days_n = np.mean(yearly_zero_flow_counts)
    except:
        zero_flow_days_n = np.nan
    return zero_flow_days_n

def get_event_durations(flow_event_series):
    event_groups = [(key, sum(1 for _ in group))
                    for key, group in groupby(flow_event_series)]
    durations = [duration for status, duration in event_groups if status == 1]
    return durations

def compute_low_flow_count_and_duration(streamflow_df, mlqf_threshold):
    is_low_flow = (streamflow_df['streamflow'] < mlqf_threshold)
    try:
        streamflow_df['is_low_flow'] = is_low_flow.astype(int)
        yearly_low_flow_counts = streamflow_df.groupby('water_year')['is_low_flow'].sum()
        low_flow_days_n = np.mean(yearly_low_flow_counts)
    except:
        low_flow_days_n = np.nan
    try:
        if low_flow_days_n == 0:
            duration_low_flow_event = 0
        else:
            duration_low_flow_event = np.mean(get_event_durations(is_low_flow))
    except:
        duration_low_flow_event = np.nan
    return low_flow_days_n, duration_low_flow_event

def compute_high_flow_count_and_duration(streamflow_df, mhqf_threshold):
    if mhqf_threshold < 0.001:
        mhqf_threshold = 0.001
    is_high_flow = (streamflow_df['streamflow'] > mhqf_threshold)
    try:
        streamflow_df['is_high_flow'] = is_high_flow.astype(int)
        yearly_high_flow_counts = streamflow_df.groupby('water_year')['is_high_flow'].sum()
        high_flow_days_n = np.mean(yearly_high_flow_counts)
    except:
        high_flow_days_n = np.nan
    try:
        if high_flow_days_n == 0:
            duration_high_flow_event = 0
        else:
            duration_high_flow_event = np.mean(get_event_durations(is_high_flow))
    except:
        duration_high_flow_event = np.nan
    return high_flow_days_n, duration_high_flow_event


def get_median_starting_date_of_flood_season(streamflow_df):
    streamflow_series = streamflow_df['streamflow'].asfreq('D')
    sliding_avg_180_days = streamflow_series.rolling(window=180, min_periods=150).mean().rename('sliding_average')
    sliding_avg_180_days_with_start_roll = sliding_avg_180_days.shift(-(180 - 1))
    sliding_avg_180_days_with_start_roll = sliding_avg_180_days_with_start_roll[~sliding_avg_180_days_with_start_roll.isna().groupby(sliding_avg_180_days_with_start_roll.index.year).transform('any')]
    sliding_avg_180_days_df = sliding_avg_180_days_with_start_roll.dropna().to_frame().loc["1980-01-01":"2021-12-31",:]
    indices_of_yearly_max_sliding_avg = sliding_avg_180_days_df.resample('YS')['sliding_average'].idxmax()
    starting_dates_of_flood_season = [(date).dayofyear for (date) in indices_of_yearly_max_sliding_avg]
    median_starting_date_of_flood_season = np.median(starting_dates_of_flood_season)
    if np.isnan(median_starting_date_of_flood_season):
        return np.nan
    else:
        return round(median_starting_date_of_flood_season)

def compute_all_indices(streamflow_df):
    streamflow_df = streamflow_df.set_index('date')
    streamflow_df.index = pd.to_datetime(streamflow_df.index)
    streamflow_df = streamflow_df.sort_values(['water_year', streamflow_df.index.name])
    indices_dict = {}
    indices_dict['monthwise_mean'], indices_dict['monthwise_cov'] = monthwise_mean_and_cov(streamflow_df)
    percentiles_to_calculate = [2,5,10,15,20,25,30,33,35,40,45,50,55,60,65,66,70,75,80,85,90,95,99]
    nth_percentile_flows = compute_percentiles(streamflow_df['streamflow'], percentiles_to_calculate)
    min_flow = np.nanmin(streamflow_df['streamflow'])
    max_flow = np.nanmax(streamflow_df['streamflow'])
    # Matching flow classifications used by USGS
    indices_dict['nth_percentile_flows'] = [min_flow,
                                     nth_percentile_flows[0],
                                        nth_percentile_flows[1],
                                        nth_percentile_flows[2],
                                        nth_percentile_flows[4],
                                        nth_percentile_flows[5],
                                        nth_percentile_flows[6],
                                        nth_percentile_flows[11],
                                        nth_percentile_flows[17],
                                        nth_percentile_flows[20],
                                        nth_percentile_flows[21],
                                        nth_percentile_flows[22],
                                     max_flow]

    indices_dict['variability_index'] = compute_variability_index(nth_percentile_flows, percentiles_to_calculate)
    indices_dict['slope_fdc'] = compute_slope_fdc(nth_percentile_flows, percentiles_to_calculate)
    indices_dict['flashiness_index'] = compute_flashiness_index(streamflow_df, q_col='streamflow', wy_col='water_year')

    try:
        indices_dict['sevenQ10'], indices_dict['mean_annual_7_day_min'] = compute_7Q10_and_MAM7(streamflow_df)
    except:
        indices_dict['sevenQ10'], indices_dict['mean_annual_7_day_min'] = (np.nan, np.nan)
    try:
        indices_dict['baseflow_index'] = compute_bfi(streamflow_df)
    except:
        indices_dict['baseflow_index'] = np.nan

    indices_dict['zero_flow_days_n'] = count_zero_flow_days(streamflow_df)

    mlqf_threshold = 0.2*streamflow_df['streamflow'].mean()
    indices_dict['low_flow_days_n'], indices_dict['duration_low_flow_event'] = compute_low_flow_count_and_duration(streamflow_df, mlqf_threshold)

    mhqf_threshold = 9*nth_percentile_flows[11]
    indices_dict['high_flow_days_n'], indices_dict['duration_high_flow_event'] = compute_high_flow_count_and_duration(streamflow_df, mhqf_threshold)

    indices_dict['half_flow_date'] = compute_half_flow_date(streamflow_df)
    indices_dict['start_date_flood_season'] = get_median_starting_date_of_flood_season(streamflow_df)

    converted_indices_dict = {k: float(v) if isinstance(v, (np.float32, np.float64)) else v for k, v in indices_dict.items()}
    return converted_indices_dict


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

AWS_ZARR_PATH = "s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr"
BIGQUERY_DATASET = 'national_water_model'
BIGQUERY_RETRO_TABLE = 'nwm_retrospective_3_0'
BIGQUERY_INDICES_TABLE = 'nwm_streamflow_indices'
BIGQUERY_RETRO_SCHEMA = {
    'fields': [
        {'name': 'feature_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'streamflow', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'velocity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'qBtmVertRunoff', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'qBucket', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'qSfcLatRunoff', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'q_lateral', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ]
}

BIGQUERY_INDICES_SCHEMA = {
    'fields': [
        {'name': 'reach_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'monthwise_mean', 'type': 'FLOAT', 'mode': 'REPEATED'},
        {'name': 'monthwise_cov', 'type': 'FLOAT', 'mode': 'REPEATED'},
        {'name': 'nth_percentile_flows', 'type': 'FLOAT', 'mode': 'REPEATED'},
        {'name': 'variability_index', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'slope_fdc', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'flashiness_index', 'type': 'FLOAT', 'mode': 'REPEATED'},
        {'name': 'sevenQ10', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'mean_annual_7_day_min', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'baseflow_index', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'zero_flow_days_n', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'low_flow_days_n', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'duration_low_flow_event', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'high_flow_days_n', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'duration_high_flow_event', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'half_flow_date', 'type': 'FLOAT', 'mode': 'REPEATED'},
        {'name': 'start_date_flood_season', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]
}


PROJECT_ID = '<PROJECT_ID>'
REGION = 'us-central1'
JOB_IDENTIFIER = 'nwm-transform-job-2600000-rest'
TEMP_LOCATION = 'gs://nwm-retro-temp'
VPC_NETWORK = 'base-network'
VPC_SUBNET = 'base-network'

class FlattenZarrChunkFn(beam.DoFn):
    def process(self, element):
        key, chunk_ds = element
        if chunk_ds is None: 
            return
        required_vars = [
            'streamflow', 'velocity', 'qBtmVertRunoff', 'qBucket',
            'qSfcLatRunoff', 'q_lateral'
        ]
        final_column_set = ['feature_id', 'time'] + required_vars

        try:
            retro_df = chunk_ds.to_dataframe().reset_index()
            for col in final_column_set:
                if col not in retro_df.columns:
                    retro_df[col] = np.nan
            retro_df = retro_df[final_column_set]
            if retro_df.empty: 
                return
            
            nwm_fill_mapping = {"streamflow": -999900, 
                                "velocity": -999900,
                                "qBtmVertRunoff": -9999000, 
                                "qBucket": -999900000, 
                                "qSfcLatRunoff": -999900000, 
                                "q_lateral": -99990 
                                }
            retro_df = retro_df.replace(nwm_fill_mapping, np.nan)

            retro_df['feature_id'] = retro_df['feature_id'].astype(np.int64)
            retro_df['time'] = pd.to_datetime(retro_df['time'])
            retro_df[required_vars] = retro_df[required_vars].astype(np.float32)

            for record in retro_df.to_dict('records'):
                yield record

        except Exception as e:
            logging.error(f"Chunk {key} processing failed: {str(e)}", exc_info=True)

class ComputeIndicesFn(beam.DoFn):
    def process(self, element):
        feature_id, data_list = element
        streamflow_df = pd.DataFrame(list(data_list))
        streamflow_df = streamflow_df.loc[:, ["date", "streamflow"]]
        streamflow_df['date'] = pd.to_datetime(streamflow_df['date'])
        streamflow_df = convert_to_water_year_data(streamflow_df, starting_month=10)
        logging.info(f"Reach ID {feature_id}: The complete daily streamflow dataset is loaded. Computation of indices starts...")
        try:
            streamflow_indices = compute_all_indices(streamflow_df)
            streamflow_indices['reach_id'] = feature_id
            logging.info(f"Reach ID {feature_id}: All data processing and computation finished.")
        except:
            streamflow_indices = {
                'reach_id': feature_id,
                'monthwise_mean': [np.nan] * 12,
                'monthwise_cov': [np.nan] * 12,
                'nth_percentile_flows': [np.nan] * 13,
                'variability_index': np.nan,
                'slope_fdc': np.nan,
                'flashiness_index': [np.nan] * 5,
                'sevenQ10': np.nan, 
                'baseflow_index': np.nan,
                'mean_annual_7_day_min': np.nan,
                'zero_flow_days_n': np.nan,
                'low_flow_days_n': np.nan,
                'duration_low_flow_event': np.nan,
                'high_flow_days_n': np.nan,
                'duration_high_flow_event': np.nan,
                'half_flow_date': [np.nan, np.nan],
                'start_date_flood_season': np.nan,
                }
            logging.info(f"Reach ID {feature_id}: All data processing and computation finished.")
        yield streamflow_indices
        

class SanitizeNaNsDoFn(beam.DoFn):   
    def sanitize_nan(self, v, is_inside_list=False):
        try:
            if v is None or (isinstance(v, (float, np.floating)) and np.isnan(v)):
                return -99.0 if is_inside_list else None
            if isinstance(v, (np.floating,)):
                return float(v)
            if isinstance(v, (np.integer,)):
                return int(v)
            if isinstance(v, (pd.Timestamp,)):
                return v.isoformat()
            if isinstance(v, (pd.Timedelta,)):
                return str(v)
            if isinstance(v, (np.ndarray,)):
                return [self.sanitize_nan(i, is_inside_list=True) for i in v.tolist()]
            if isinstance(v, (list, tuple)):
                return [self.sanitize_nan(i, is_inside_list=True) for i in v]
            if isinstance(v, dict):
                return {k: self.sanitize_nan(val, is_inside_list=False) for k, val in v.items()}
            return v
        except Exception:
            try:
                return str(v)
            except Exception:
                return -99.0 if is_inside_list else None
    def process(self, element):
        yield self.sanitize_nan(element)


class retrospective_rowdict_schema(typing.NamedTuple):
    feature_id: int
    time: datetime.datetime
    streamflow: Optional[float]
    velocity: Optional[float]
    qBtmVertRunoff: Optional[float]
    qBucket: Optional[float]
    qSfcLatRunoff: Optional[float]
    q_lateral: Optional[float]


class indices_rowdict_schema(typing.NamedTuple):
    reach_id: int
    monthwise_mean: List[float]
    monthwise_cov: List[float]
    nth_percentile_flows: List[float]
    variability_index: Optional[float]
    slope_fdc: Optional[float]
    flashiness_index: List[float]
    sevenQ10: Optional[float]
    mean_annual_7_day_min: Optional[float]
    baseflow_index: Optional[float]
    zero_flow_days_n: Optional[float]
    low_flow_days_n: Optional[float]
    duration_low_flow_event: Optional[float]
    high_flow_days_n: Optional[float]
    duration_high_flow_event: Optional[float]
    half_flow_date: List[float]
    start_date_flood_season: Optional[int]


def extract_date_key(element):
    timestamp_str = str(element['time']) + "Z" # Z for UTC
    value = element['streamflow']
    dt_object = datetime.datetime.fromisoformat(timestamp_str)
    date_key = dt_object.date().isoformat()
    key = (element.get('feature_id', 'GLOBAL'), date_key)
    return (key, value)


def run_pipeline(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(
        description=('Tranform the NWM Retrospective 3.0 Zarr dataset from S3 to BigQuery Dataset and Compute Streamflow Indices '))
    parser.add_argument('--zarr_path', dest='zarr_path', default=AWS_ZARR_PATH)
    parser.add_argument('--runner', default='DataflowRunner')
    parser.add_argument('--project')
    parser.add_argument('--region')
    parser.add_argument('--temp_location')
    parser.add_argument('--staging_location')
    parser.add_argument('--machine_type')
    parser.add_argument('--num_workers', type=int)
    parser.add_argument('--max_num_workers', type=int)
    parser.add_argument('--disk_size_gb', type=int)

    known_args, pipeline_args = parser.parse_known_args()
    options = PipelineOptions(pipeline_args, save_main_session=True)
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(GoogleCloudOptions).project = PROJECT_ID
    options.view_as(GoogleCloudOptions).temp_location = "gs://nwm-retro-staging/temp"
    options.view_as(GoogleCloudOptions).staging_location = "gs://nwm-retro-staging/staging"
    options.view_as(GoogleCloudOptions).region = REGION
    options.view_as(GoogleCloudOptions).job_name = JOB_IDENTIFIER
    options.view_as(GoogleCloudOptions).service_account_email = "sa-dataflow-worker@nwm-retro-migration.iam.gserviceaccount.com"
    options.view_as(DebugOptions).experiments = ['use_runner_v2', 
                                                 'use_unified_worker', 
                                                 'shuffle_mode=service']
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = 'n2-standard-16'
    worker_options.num_workers = 20
    worker_options.max_num_workers = 100
    worker_options.network = VPC_NETWORK #comment out for using default
    worker_options.subnetwork=f"https://www.googleapis.com/compute/v1/projects/{PROJECT_ID}/regions/{REGION}/subnetworks/{VPC_SUBNET}" #comment out for using default
    worker_options.disk_size_gb = 250
    worker_options.sdk_container_image = 'us-central1-docker.pkg.dev/nwm-retro-migration/beam-images/nwm-transformation-img:v1' #'us-central1-docker.pkg.dev/nwm-ciroh/beam-images/nwm-data-transformation'

    nwm_ds, input_chunks = xbeam.open_zarr(known_args.zarr_path,
                         storage_options= {"anon": True},
                         consolidated=True)
    logging.info("Sizes of overall retrospective zarr: %s", dict(nwm_ds.sizes))
    logging.info("Initial coordinates of the retrospective zarr: %s", list(nwm_ds.coords.keys()))

    chunks_per_worker = {'time': 672, 'feature_id': 2000}
    LIST_OF_REACHES = [2849991, 6010106, 942030011, 10329013, 10376596, 23997388, 15111451, 23763337, 18836857, 8212843, 13309677, 12068830, 3232789, 22702940, 8467225, 15907959, 1314651, 6269248, 11757960, 3764246]
    SELECTION_MODE = "slice" # alternatives: ["slice", "reach_list", None]
    if SELECTION_MODE=="slice":
        logging.info("SLICING MODE enabled: taking a smaller isel slice")
        try:
            nwm_ds = nwm_ds.isel(feature_id=slice(2600000,None), time=slice(0,nwm_ds.sizes['time']))
            logging.info("Reduced sizes=%s", dict(nwm_ds.sizes))
        except Exception as e:
            logging.exception("SLICING MODE isel failed: %s", e)
    elif SELECTION_MODE=="reach_list":
        logging.info("FIXED REACHES MODE enabled: taking a smaller sel slice on feature_id")
        try:
            nwm_ds = nwm_ds.sel(feature_id=LIST_OF_REACHES)
            logging.info("Reduced sizes=%s", dict(nwm_ds.sizes))
        except Exception as e:
            logging.exception("FIXED REACHES MODE isel failed: %s", e)
    elif SELECTION_MODE==None:
        logging.info("Proceeding with the whole zarr without any slicing")
          
    logging.info(f"Zarr Dataset Chunks: {chunks_per_worker}")

    
    with beam.Pipeline(options=options) as p:

        zarr_chunks_pcoll = (
            p
            | 'SplitZarrIntoChunks' >> xbeam.DatasetToChunks(nwm_ds, chunks=chunks_per_worker)
            )
        dict_records = (
            zarr_chunks_pcoll
            | 'FlattenZarrChunk' >> beam.ParDo(FlattenZarrChunkFn())
            )
        dict_records_sanitized = (
            dict_records 
            | 'SanitizeRetroNaNs' >> beam.ParDo(SanitizeNaNsDoFn()).with_output_types(indices_rowdict_schema)
            )
        daily_data = (
            dict_records
            | 'ExtractDateKey' >> beam.Map(extract_date_key)
            | 'AggregateDailyData' >> beam.combiners.Mean.PerKey()
            )
        reach_grouped_data = (
            daily_data
            | 'MapToKV' >> beam.Map(lambda KV: (KV[0][0], {'date': KV[0][1], 'streamflow': KV[1]}))
            | 'GroupByFeatureID' >> beam.GroupByKey()
            )
        indices_dict = (
            reach_grouped_data 
            | 'ComputeIndices' >> beam.ParDo(ComputeIndicesFn())
            | 'SanitizeIndicesNaNs' >> beam.ParDo(SanitizeNaNsDoFn()).with_output_types(indices_rowdict_schema)
            )
        
        _ = (dict_records_sanitized | 'WriteRetrospectiveToBigQuery' >> beam.io.WriteToBigQuery(
            table=BIGQUERY_RETRO_TABLE,
            dataset=BIGQUERY_DATASET,
            project=PROJECT_ID,
            schema=BIGQUERY_RETRO_SCHEMA,
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location="gs://nwm-retro-staging/temp_bq"
            ))
        
        _ = (indices_dict | 'WriteIndicesToBigQuery' >> beam.io.WriteToBigQuery(
            table=BIGQUERY_INDICES_TABLE,
            dataset=BIGQUERY_DATASET,
            project=PROJECT_ID,
            schema=BIGQUERY_INDICES_SCHEMA,
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location="gs://nwm-retro-staging/temp_indices"
            ))
    return None


if __name__ == '__main__':
    print("Starting NWM Data Transformation and Computation Dataflow pipeline...")
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
    print("Pipeline run complete. Check the generated outputs in respective repositories.")




