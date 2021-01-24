# encoding: UTF-8

from .data_ref import PRODUCT_GROUP_NAME, CLOSE_PRICE_NAME, \
    HV_20_NAME, HV_250_NAME, HV_20_250_NAME, IV_NAME, \
    HV_PER, HV_MIN, HV_MAX, IV_PER, IV_MIN, IV_MAX
import pandas as pd
import numpy as np
import sys


# about 1 years
HV_DISTRIBUTION_PERIODS = 250


#----------------------------------------------------------------------
def historical_volatility(close: pd.Series, n: int):
    """calculate the historical volatility"""
    # NYSE = 252 trading days; Shanghai Stock Exchange = 242; Tokyo Stock Exchange = 246 days?
    # options strategy use 260
    log_close = np.log(close / np.roll(close, 1))
    log_close[0] = np.nan
    return np.sqrt(np.multiply(HV_DISTRIBUTION_PERIODS, log_close.rolling(n).var()))


#----------------------------------------------------------------------
def index_distribution_of_per(size: int):
    """calculate the index to split a list"""
    min_delta = int(np.floor(size / 100))
    remain = size - min_delta * 100
    # remain to the bottom
    li = []
    for idx in range(99):
        delta = min_delta
        if remain > 0:
            delta += 1
            remain -= 1
        if 0 == idx:
            li.append(delta)
        else:
            li.append(li[-1] + delta)
    return li


#----------------------------------------------------------------------
def percent_distribution_list(hvs: pd.Series):
    """calculate the percentage value list"""
    sorted_hvs = hvs.dropna().sort_values()
    size = sorted_hvs.shape[0]
    if size < HV_DISTRIBUTION_PERIODS:
        # not enough size,  return empty list
        return []
    else:
        sorted_hvs = sorted_hvs[-HV_DISTRIBUTION_PERIODS:]
        idxes = index_distribution_of_per(HV_DISTRIBUTION_PERIODS)
        return map(lambda idx: sorted_hvs.iloc[idx], idxes)


#----------------------------------------------------------------------
def percent_distribution(vix: pd.Series, val: float = None):
    """calculate the percentage of the value at"""
    dis = percent_distribution_list(vix)
    if [] == dis:
        # not enough distribution, return 50
        return 50
    if val is None:
        val = vix.iloc[-1]
    ret = 0
    for tval in dis:
        if val > tval:
            ret += 1
        else:
            break
    return ret


#----------------------------------------------------------------------
def calc_percentage(volatility: pd.Series):
    """"""
    return volatility.rolling(HV_DISTRIBUTION_PERIODS).apply(lambda rows: percent_distribution(rows))


#----------------------------------------------------------------------
def historical_max_min(df: pd.DataFrame, column_name: str, max_key: str, min_key: str):
    """mark the historical max an min in the dataframe"""
    # according to:
    # https://stackoverflow.com/questions/61759149/apply-a-function-on-a-dataframe-that-depend-on-the-previous-row-values
    # NOTE: I rename the variables with _ to avoid using builtin method names
    max_ = sys.float_info.min
    min_ = sys.float_info.max
    # list for the results
    l_res = []
    for value in df[column_name].to_numpy():
        # iterate over the values
        if value >= max_:
            max_ = value
        if value <= min_:
            min_ = value
            # append the results in the list
        if sys.float_info.min == max_ and sys.float_info.max == min_:
            l_res.append(['-', '-'])
        else:
            l_res.append([max_, min_])
        # create the three columns outside of the loop
    df[[max_key, min_key]] = pd.DataFrame(l_res, index = df.index)


#----------------------------------------------------------------------
def sort_hv20250(dfs: list):
    """df list"""
    date = None
    products = []
    for idx, df in enumerate(dfs):
        if 0 == idx:
            date = df.index[-1]
        products.append(df[df.index == date])
    final = pd.concat(products)
    final = final[[PRODUCT_GROUP_NAME, HV_20_NAME, HV_MIN, HV_MAX, HV_250_NAME,
                   IV_NAME, IV_MIN, IV_MAX, HV_PER, IV_PER, HV_20_250_NAME]]
    # final[HV_PER].fillna('', inplace = True)
    final.dropna(subset = [HV_20_250_NAME], inplace = True)
    final.sort_values(by = HV_20_250_NAME, ascending = False, inplace = True)
    return date, final
