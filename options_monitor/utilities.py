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

# for options
IV_NAME = 'iv'
U_PRODUCT_ID_NAME = 'u_id' # 标的物
OPTION_TYPE_NAME = 'otype' # 期权类型 c call p put
S_PRICE_NAME = 's_price'   # 行权价
U_PRICE_NAME = 'u_price'   # 标的价


# hv key
HV_20_NAME = 'hvm'
HV_250_NAME = 'hvy'
HV_20_250_NAME = 'hvm/y'
HV_PER = 'per'

# total key for future's index
TOTAL_ROW_KEY = 'total'

COLUMN_NAMES = [PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, PRE_SETTLE_PRICE_NAME,
                OPEN_PRICE_NAME, HIGH_PRICE_NAME, LOW_PRICE_NAME, CLOSE_PRICE_NAME, SETTLE_PRICE_NAME,
                OPEN_INTEREST_NAME, OI_CHG_NAME, VOLUME_NAME]

O_COLUMN_NAMES = [PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, OPTION_TYPE_NAME, S_PRICE_NAME,
                  U_PRICE_NAME, CLOSE_PRICE_NAME, IV_NAME, VOLUME_NAME, OPEN_INTEREST_NAME]

# markdown head and \n replace
MD_HEAD_PATTERN = re.compile(r'\|(:?-{3,}:?\|){4,}\n')
MD_FORMAT_PATTERN = re.compile(r'\n')
MD_FORMAT_TO = r'\n\n'

# futures names for hv notification
FUTURE_HV_NAMES = {
    # cs
    'csidx300' : '300',
    # shfe
    'cu_f' : u'沪铜',
    'al_f' : u'沪铝',
    'zn_f' : u'沪锌',
    'pb_f' : u'沪铅',
    'ni_f' : u'沪镍',
    'sn_f' : u'沪锡',
    'au_f' : u'沪金',
    'ag_f' : u'沪银',
    'rb_f' : u'螺纹',
    'hc_f' : u'热卷',
    'ss_f' : u'ss',
    'sc_f' : u'原油',
    'fu_f' : u'燃油',
    'lu_f' : u'LU',
    'bu_f' : u'沥青',
    'ru_f' : u'橡胶',
    'nr_f' : u'20胶',
    'sp_f' : u'纸浆',
    # DCE
    u'豆一' : u'豆一',
    u'玉米' : u'玉米',
    u'苯乙烯' : u'EB',
    u'乙二醇' : u'eg',
    u'铁矿石' : u'铁',
    u'焦炭' : u'焦炭',
    u'鸡蛋' : u'鸡蛋',
    u'焦煤' : u'焦煤',
    u'聚乙烯' : u'塑料',
    u'生猪' : u'生猪',
    u'豆粕' : u'豆粕',
    u'棕榈油' : u'棕榈',
    u'液化石油气' : u'LPG',
    u'聚丙烯' : u'pp',
    u'粳米' : u'粳米',
    u'聚氯乙烯' : u'pvc',
    u'豆油' : u'豆油',
    # CZCE
    'AP' : u'苹果',
    'CF' : u'郑棉',
    'CJ' : u'红枣',
    # 'CY' : u'棉纱',
    'FG' : u'玻璃',
    # 'LR' : u'晚稻',
    'MA' : u'郑醇',
    'OI' : u'郑油',
    # 'PM' : u'普麦',
    # 'RI' : u'早稻',
    'RM' : u'菜粕',
    # 'RS' : u'菜籽',
    'SA' : u'纯碱',
    'SF' : u'硅铁',
    'SM' : u'锰硅',
    'SR' : u'白糖',
    'TA' : u'TA',
    'UR' : u'尿素',
    # 'WH' : u'郑麦',
    'ZC' : u'动煤'
}


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
def get_last_trade_dates(delta: int = 500):
    delta = int(delta)
    end_time = datetime.datetime.now(sse_calendar.tz)
    utc_end_time = end_time.astimezone(datetime.timezone.utc)
    if utc_end_time.hour < SCHEDULE_HOUR or utc_end_time.day != end_time.day:
        # before the schedule timestamp, back 1 day
        end_time += datetime.timedelta(days = -1)
    # about 13 months ago
    start_time = end_time + datetime.timedelta(days = -delta)
    dates = pd.date_range(start = start_time, end = end_time, freq = 'B')
    dates = dates.strftime(DATE_FORMAT)
    from .data_manager import calendar_manager
    fdates = dates[dates.map(calendar_manager.check_open)]
    return fdates


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
    df.drop([HV_250_NAME], axis = 1, inplace = True)
    df2 = df.applymap(lambda x: f"{x:.1%}".strip('%') if isinstance(x, float) else x)
    df2[HV_PER] = df[HV_PER].apply(lambda x: f"{int(x)}" if isinstance(x, float) else x)
    content = f'{notify_format(df2)}'
    return notify_format_content(content)


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
