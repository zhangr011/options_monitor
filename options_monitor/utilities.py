# encoding: UTF-8

import os, sys, datetime, hashlib, glob, re
import pandas as pd
import numpy as np
import pandas_market_calendars as market_cal
from functools import reduce, cached_property

from .logger import logger


# Date format: Year-Month-Day
DATE_FORMAT ='%Y-%m-%d'
DATE_FORMAT_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2})')


# shanghai stock exchange
sse_calendar = market_cal.get_calendar('SSE')
# timezone('Asia/Shanghai')
TZ_INFO = sse_calendar.tz.zone

SCHEDULE_HOUR = 11

ONE_DAY = datetime.timedelta(days = 1)
SEVEN_DAYS = datetime.timedelta(days = 7)

# data root
DATA_ROOT = './data'
TEST_DATA_ROOT = './test/data'

# index key for file check
INDEX_KEY = 'Trade Date'
PRODUCT_ID_NAME = 'ProductId'
PRODUCT_GROUP_NAME = 'GroupId'
PRE_SETTLE_PRICE_NAME = 'PreSettle'
SETTLE_PRICE_NAME = 'Settle'
OPEN_PRICE_NAME = 'Open'
HIGH_PRICE_NAME = 'High'
LOW_PRICE_NAME = 'Low'
CLOSE_PRICE_NAME = 'Close'
OPEN_INTEREST_NAME = 'OI'
OI_CHG_NAME = 'OIChange'
VOLUME_NAME = 'Volume'

TOTAL_ROW_KEY = 'total'

COLUMN_NAMES = [PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, PRE_SETTLE_PRICE_NAME,
                OPEN_PRICE_NAME, HIGH_PRICE_NAME, LOW_PRICE_NAME, CLOSE_PRICE_NAME, SETTLE_PRICE_NAME,
                OPEN_INTEREST_NAME, OI_CHG_NAME, VOLUME_NAME]

# about 3 years
HV_DISTRIBUTION_PERIODS = 260 * 3

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
def get_last_trade_dates(delta: int = 400):
    delta = int(delta)
    end_time = datetime.datetime.now(sse_calendar.tz)
    utc_end_time = end_time.astimezone(datetime.timezone.utc)
    if utc_end_time.hour < SCHEDULE_HOUR or utc_end_time.day != end_time.day:
        # before the schedule timestamp, back 1 day
        end_time += datetime.timedelta(days = -1)
    # about 13 months ago
    start_time = end_time + datetime.timedelta(days = -delta)
    dates = pd.date_range(start = start_time, end = end_time, freq = 'B')
    return dates.strftime(DATE_FORMAT)


#----------------------------------------------------------------------
def check_date_in(date_str: str, dates: pd.Index):
    return date_str in dates


#----------------------------------------------------------------------
def make_sure_dirs_exist(path):
    """确保目录存在"""
    is_exist = os.path.exists(path)
    if not is_exist:
        os.makedirs(path)
    if not os.path.exists(path):
        return False
    return True


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
def format_index(df: pd.DataFrame, delivery_dates: list = []):
    """format the index of DataFrame"""
    if delivery_dates != []:
        df['d'] = df.apply(lambda row: 'd' if row.name in delivery_dates else '', axis = 1)
    df.index = df.index.str.replace(r'\d{4}-', '')
    df.index = df.index.str.replace('-', '')
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
def historical_max_min_per(df: pd.DataFrame):
    """mark the historical max an min in the dataframe"""
    # according to:
    # https://stackoverflow.com/questions/61759149/apply-a-function-on-a-dataframe-that-depend-on-the-previous-row-values
    # NOTE: I rename the variables with _ to avoid using builtin method names
    max_ = sys.float_info.min
    min_ = sys.float_info.max
    # list for the results
    l_res = []
    for value in df.Close.to_numpy():
        # iterate over the values
        if value >= max_:
            max_ = value
        if value <= min_:
            min_ = value
        # append the results in the list
        ratio = 100
        if max_ - min_ > 0:
            ratio = round((value - min_) * 100 / (max_ - min_))
        l_res.append([max_, min_, ratio])
    # create the three columns outside of the loop
    df[['Max', 'Min', 'mper']] = pd.DataFrame(l_res, index = df.index)


#----------------------------------------------------------------------
def mk_notification_params(vix_futures: pd.DataFrame,
                           delivery_dates: list,
                           rets_vix: dict,
                           rets_gvz: dict, rets_ovx: dict):
    """make the notification's params"""
    rets = {'vix_futures': vix_futures, 'delivery_dates': delivery_dates}
    rets.update(rets_vix)
    rets.update(rets_gvz)
    rets.update(rets_ovx)
    return rets


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
def mk_notification(vix_futures: pd.DataFrame,
                    delivery_dates: list,
                    vix_diff: pd.DataFrame,
                    vix: pd.DataFrame,
                    gvz: pd.DataFrame = None,
                    ovx: pd.DataFrame = None):
    """make the notification msg from the params"""
    if np.alltrue(vix_diff.iloc[-5:][1] > 0.02):
        per_msg = 'vix 2/1 is safe now. '
    elif np.any(vix_diff.iloc[-5:][1] < -0.02):
        per_msg = 'vix 2/1 warning!!!! '
    else:
        per_msg = 'vix is ok. '
    # combine the result
    futures_521 = vix_futures.iloc[-5:, [0, 1]]
    vix_diff_51 = vix_diff.iloc[-5:, [0]]
    futures_521 = futures_521.applymap(lambda x: f"{x:.1f}")
    futures_521['f2/1'] = vix_diff_51[1].apply(lambda x: f"{x:.1%}")
    # clear the year info of Trade Date
    format_index(futures_521, delivery_dates)
    # calculate the vix percentage
    vix_51, vmax, vmin = calc_percentage(vix)
    # calculate the gvz percentage
    gvz_51, gmax, gmin = calc_percentage(gvz)
    # calculate the ovx percentage
    ovx_51, omax, omin = calc_percentage(ovx)
    content = f"""{notify_format(futures_521)}
------------------------
#### **vix:** {vmin:.2f} - {vmax:.2f}
{notify_format(vix_51)}
------------------------
#### **gvz:** {gmin:.2f} - {gmax:.2f}
{notify_format(gvz_51)}
------------------------
#### **ovx:** {omin:.2f} - {omax:.2f}
{notify_format(ovx_51)}"""
    return per_msg, notify_format_content(content)


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
