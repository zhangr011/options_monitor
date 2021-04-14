# encoding: UTF-8

import os, sys, datetime, hashlib, glob, re
import pandas as pd
import numpy as np
from .data_ref import DATE_FORMAT, DATE_FORMAT_PATTERN, DATA_ROOT, \
    INDEX_KEY, PRODUCT_GROUP_NAME, FUTURE_HV_NAMES, HV_250_NAME, HV_PER, IV_PER

from functools import reduce, cached_property
from .logger import logger


# markdown head and \n replace
MD_HEAD_PATTERN = re.compile(r'\|(:?-{3,}:?\|){4,}\n')
MD_FORMAT_PATTERN = re.compile(r'\n')
MD_FORMAT_TO = r'\n\n'


#----------------------------------------------------------------------
def is_business_day(input_date, schedule_days):
    input_date_str = input_date.strftime(DATE_FORMAT)
    holiday_check = cboe_calendar.open_at_time(schedule_days, pd.Timestamp(input_date_str + ' 12:00', tz=TZ_INFO))
    return holiday_check


#----------------------------------------------------------------------
def get_day_index(last_day: datetime, hour: int):
    if last_day.hour < hour:
        last_day = last_day + datetime.timedelta(days = -1)
    return datetime.datetime.strftime(last_day, DATE_FORMAT)


#----------------------------------------------------------------------
def get_file_path(filename: str) -> str:
    return os.path.join(DATA_ROOT, filename)


#----------------------------------------------------------------------
def check_data_integrity(path: str, date: str):
    """check data's integrity"""
    data = pd.read_csv(path)
    if not data.empty and data[INDEX_KEY].iloc[-1] == date:
        return True
    return False


#----------------------------------------------------------------------
def dup_futures_info(futures_info: pd.DataFrame, sdate: str, edate: str):
    """duplicate a date schedule between sdate and edate"""
    return pd.DataFrame(futures_info[(futures_info.index > sdate) &
                                     (futures_info.index <= edate)],
                        columns = [CLOSE_PRICE_NAME, SETTLE_PRICE_NAME])


#----------------------------------------------------------------------
def mark_by_date(row: pd.Series, sdate: str, edate: str = None):
    """"""
    date = row.name
    if isinstance(date, datetime.datetime):
        date = datetime.datetime.strftime(row.name, DATE_FORMAT)
    if date > sdate and (edate is None or date <= edate):
        # use the settle price
        if row[SETTLE_PRICE_NAME]:
            return row[SETTLE_PRICE_NAME]
        else:
            return row[CLOSE_PRICE_NAME]
    else:
        return 0


#----------------------------------------------------------------------
def generate_term_structure(delivery_dates: list,
                            futures_info: pd.DataFrame, tdate: str):
    """generate tdate's mask of term structure"""
    if futures_info.empty:
        return None
    # remain last one year's trade date
    filtered_delivery = filter_delivery_dates(delivery_dates, tdate)
    if [] == filtered_delivery:
        filtered_delivery = [INIT_DATE]
    sdate = filtered_delivery[0]
    # duplicate the target frame
    term = dup_futures_info(futures_info, sdate, tdate)
    # generate the term structure according to tdate
    reversed_delivery = reversed(filtered_delivery)
    edate = None
    for idx, sdate in enumerate(reversed_delivery):
        term.insert(loc = idx, column = idx,
                    value = term.apply(
                        lambda x: mark_by_date(x, sdate, edate), axis = 1))
        # mark the end date for the next item check
        edate = sdate
    # drop the settle price column
    term.drop(columns = [CLOSE_PRICE_NAME, SETTLE_PRICE_NAME], inplace = True)
    return term


#----------------------------------------------------------------------
def combine_data(futures_info_a: pd.DataFrame, futures_info_b: pd.DataFrame):
    """combine futures close price according to the front to later"""
    if futures_info_b is None or futures_info_b.empty:
        return futures_info_a
    return futures_info_a.add(futures_info_b, fill_value = 0)


#----------------------------------------------------------------------
def combine_all(delivery_dates: list, path: str, max_times: int = 12):
    """combine the last n futures info, default to the last 12 futures"""
    paths = sorted(glob.glob(os.path.join(path, '*.csv')), reverse = True)
    filtered_infos = []
    times = 0
    for path in paths:
        res, date = is_futures_file(path)
        if res:
            info = load_futures_by_csv(path)
            term = generate_term_structure(delivery_dates, info, date)
            if term is None:
                continue
            filtered_infos.append(term)
            times += 1
            if times >= max_times:
                break
    # combine the all infos
    final = reduce(combine_data, filtered_infos)
    return final.fillna(0)


#----------------------------------------------------------------------
def analyze_diff_percent(info: pd.DataFrame):
    """calculate the diff percent"""
    delta = info.diff(periods = 1, axis = 1)
    delta_p = delta.div(info.shift(periods = 1, axis = 1))
    return delta_p


#----------------------------------------------------------------------
def close_ma5_ma10_ma20(df: pd.DataFrame):
    """calculate the ma5 ma10 and ma20 of the close price"""
    close_seq = df[CLOSE_PRICE_NAME]
    ma5 = close_seq.rolling(5).mean()
    ma10 = close_seq.rolling(10).mean()
    ma20 = close_seq.rolling(20).mean()
    df['ma5'] = ma5
    df['ma10'] = ma10
    df['ma20'] = ma20
    return df


#----------------------------------------------------------------------
def format_index(df: pd.DataFrame, delivery_dates: list = []):
    """format the index of DataFrame"""
    if delivery_dates != []:
        df['d'] = df.apply(lambda row: 'd' if row.name in delivery_dates else '', axis = 1)
    df.index = df.index.str.replace(r'\d{4}-', '', regex = True)
    df.index = df.index.str.replace('-', '', regex = True)
    df.index.rename('Date', inplace = True)


#----------------------------------------------------------------------
def calc_percentage(vx: pd.DataFrame):
    """calculate the percentage"""
    historical_max_min_per(vx)
    vx['per'] = vx.Close.rolling(HV_DISTRIBUTION_PERIODS).apply(lambda rows: percent_distribution(rows))
    vx_51 = vx.iloc[-5:].loc[:, [CLOSE_PRICE_NAME, 'mper', 'per']]
    format_index(vx_51)
    return vx_51, vx.iloc[-1].loc['Max'], vx.iloc[-1].loc['Min']


#----------------------------------------------------------------------
def notify_format(df: pd.DataFrame):
    """format the dataframe for notification"""
    return df.to_markdown()


#----------------------------------------------------------------------
def notify_format_content(content: str):
    """format the string"""
    # clear the table head
    content = MD_HEAD_PATTERN.subn('', content)[0]
    # make sure two space besides the \n
    return MD_FORMAT_PATTERN.subn(MD_FORMAT_TO, content)[0]


#----------------------------------------------------------------------
def mk_notification(df: pd.DataFrame):
    """make the notification msg from the params"""
    # drop the index
    df[PRODUCT_GROUP_NAME] = df[PRODUCT_GROUP_NAME].apply(lambda x: FUTURE_HV_NAMES.get(x, np.nan))
    df = df[df[PRODUCT_GROUP_NAME].notnull()]
    df.set_index(PRODUCT_GROUP_NAME, inplace = True)
    df.index.rename('name', inplace = True)
    df2 = df.applymap(lambda x: f"{x:.1%}".strip('%') if isinstance(x, float) else x,
                      na_action = 'ignore')
    # for test
    df = df[~df[HV_PER].isnull()]
    df2[HV_PER] = df[HV_PER].apply(lambda x: f"{int(x)}" if isinstance(x, float) else x)
    df2[IV_PER] = pd.to_numeric(df[IV_PER], errors = 'coerce')
    df2.fillna('-', inplace = True)
    df2[IV_PER] = df2[IV_PER].apply(lambda x: f"{int(x)}" if isinstance(x, float) else x)
    return df2


#----------------------------------------------------------------------
def load_futures_by_csv(path: str):
    """load futures info by csv"""
    df = pd.read_csv(path)
    df.set_index(INDEX_KEY, inplace = True)
    return df


#----------------------------------------------------------------------
def is_futures_file(path: str):
    """check the file then return the delivery date"""
    res = DATE_FORMAT_PATTERN.search(path)
    if res:
        return True, res.group(1)
    return False, None
