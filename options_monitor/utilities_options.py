# encoding: UTF-8

from .logger import logger

try:
    # 欧式期权
    import black_76_cython as black_76
    # 美式期货期权
    import binomial_tree_cython as binomial_tree
    # 欧式股票期权
    import black_scholes_cython as black_scholes
except ImportError:
    from .pricing import (
        black_76, binomial_tree, black_scholes
    )
    logger.info("Faile to import cython option pricing model, please rebuild with cython in cmd.")
    logger.info('pip3 install ./pricing/cython_model/binomial_tree_cython')
    logger.info('pip3 install ./pricing/cython_model/black_76_cython')
    logger.info('pip3 install ./pricing/cython_model/black_scholes_cython')


from .singleton import Singleton
from .data_ref import DATE_FORMAT
from .utilities_calendar import get_next_trading_day_str

from enum import Enum
from functools import lru_cache
from datetime import datetime, timedelta
import pandas as pd
import re


class OptionsType(Enum):
    CALL = 1
    PUT  = -1


OPTIONS_TYPE_CALL = 'C'
OPTIONS_TYPE_PUT  = 'P'


OPTIONS_TYPE_MAP = {
    OPTIONS_TYPE_CALL : OptionsType.CALL.value,
    OPTIONS_TYPE_PUT  : OptionsType.PUT.value
}


class PriceModelIndex(Enum):
    EUROPEAN_FUTURES = 1
    EUROPEAN_STOCKS  = 2
    AMERICAN_FUTURES = 3


PRICING_MODELS_MAP = {
    PriceModelIndex.EUROPEAN_FUTURES : black_76,
    PriceModelIndex.EUROPEAN_STOCKS  : black_scholes,
    PriceModelIndex.AMERICAN_FUTURES : binomial_tree
}

OPTIONS_MODELS_MAP = {
    'csidx300' : PriceModelIndex.EUROPEAN_FUTURES,  # 沪深300
    'au_f'     : PriceModelIndex.EUROPEAN_FUTURES,  # 沪金
    'remained' : PriceModelIndex.AMERICAN_FUTURES
}

ANNUAL_DAYS   = 365
INTEREST_RATE = 0.12


OPTIONS_NAME_PATTERN = r'^([a-z|A-Z]+)([0-9]+)([0-9]{2})(C|P|-C-|-P-)([0-9]+)$'
COMPILED_OPTIONS_NAME_PATTERN = re.compile(OPTIONS_NAME_PATTERN)


START_DATE = '2017-01-01'
endtime = datetime.now() + timedelta(days = 732)
THIRD_FRIDAYS = pd.date_range(start = START_DATE, end = endtime, freq = "WOM-3FRI")
FIRST_BUSINESS_DAY = pd.date_range(start = START_DATE, end = endtime, freq = 'BMS')
LAST_BUSINESS_DAY = pd.date_range(start = START_DATE, end = endtime, freq = 'BM')


#----------------------------------------------------------------------
def get_pricing_model_by_gid(gid):
    idx = OPTIONS_MODELS_MAP.get(gid, PriceModelIndex.AMERICAN_FUTURES)
    model = PRICING_MODELS_MAP.get(idx)
    return model


#----------------------------------------------------------------------
@lru_cache
def calc_remained_days(name: str, full_date: str, current: str):
    """calculate the days remaining of options"""
    expiry_date = get_expiry_date(name, full_date)
    intervals = datetime.strptime(expiry_date, DATE_FORMAT) - datetime.strptime(current, DATE_FORMAT)
    days = intervals.days + 1
    if days <= 0:
        raise ValueError(f'remained days {expiry_date} - {current},  {days} out of range. ')
    return days


#----------------------------------------------------------------------
def calc_iv(gid: str, option_price: float, underlying_price: float,
            strike_price: float, days: int, otype_str: str,
            interest_rate: float = INTEREST_RATE):
    model = get_pricing_model_by_gid(gid)
    otype = OPTIONS_TYPE_MAP.get(otype_str)
    return model.calculate_impv(option_price, underlying_price, strike_price,
                                interest_rate, days / ANNUAL_DAYS, otype)


#----------------------------------------------------------------------
def fill_the_date(year_str: str, month_str: str, delta: int = 732):
    """fill the year and date 01"""
    n2y = datetime.now() + timedelta(days = delta)
    dt = 10
    if len(year_str) == 2:
        dt = 100
    year = n2y.year // dt * dt + int(year_str)
    if year >= n2y.year:
        year = year - dt
    return str(year) + '-' + month_str + '-01'


#----------------------------------------------------------------------
def get_expiry_date(name: str, date_str: str):
    """get the expiry date by the options' name and date"""
    if 'IO' == name:
        # 沪深300, 到期月份的第三个星期五，遇国家法定假日顺延
        dates = THIRD_FRIDAYS[THIRD_FRIDAYS > date_str]
        day_str = get_next_trading_day_str(dates[0])
    elif name in ['cu', 'al', 'zn', 'au', 'ru']:
        # 上期所，标的期货合约到期日前一个月的倒数第 5 个交易日
        dates = LAST_BUSINESS_DAY[LAST_BUSINESS_DAY < date_str]
        day_str = get_next_trading_day_str(dates[-1], -5)
    elif name in ['m', 'c', 'i', 'pg', 'l', 'v', 'pp']:
        # 大商所，标的期货合约到期日前一个月的第 5 个交易日
        dates = FIRST_BUSINESS_DAY[FIRST_BUSINESS_DAY < date_str]
        day_str = get_next_trading_day_str(dates[-1], 5)
    elif 'SR' == name and date_str < '2019-09-01':
        # 郑商所，2019-09-01 之前为标的期货合约到期日前两个月的倒数第 5 个交易日
        dates = LAST_BUSINESS_DAY[LAST_BUSINESS_DAY < date_str]
        day_str = get_next_trading_day_str(dates[-2], -5)
    elif name in ['CF', 'SR', 'RM', 'MA', 'TA', 'ZC']:
        # 郑商所，标的期货合约到期日前一个月的第 3 个交易日
        dates = FIRST_BUSINESS_DAY[FIRST_BUSINESS_DAY < date_str]
        day_str = get_next_trading_day_str(dates[-1], 3)
    else:
        raise ValueError(f"options contract not supported: {name}")
    return day_str


#----------------------------------------------------------------------
class OptionsContractsManager(metaclass = Singleton):

    #----------------------------------------------------------------------
    def get_expiry_date_test(self, contract: str):
        """get the expiry date by the contract's name"""
        name, full_date, _o_type, _s_price = self.parse_the_contract(contract)
        return get_expiry_date(name, full_date)

    #----------------------------------------------------------------------
    def parse_the_contract(self, contract: str):
        """parse the contract name"""
        result = COMPILED_OPTIONS_NAME_PATTERN.match(contract)
        name = result.group(1)
        full_date = fill_the_date(result.group(2), result.group(3))
        o_type = result.group(4).strip('-')
        s_price = float(result.group(5))
        return name, full_date, o_type, s_price

    #----------------------------------------------------------------------
    def calc_iv_and_rdays(self, contract: str, o_price: float, u_price: float,
                          c_date: str, interest_rate: float = INTEREST_RATE):
        """calculate the iv"""
        name, full_date, o_type, s_price = self.parse_the_contract(contract)
        days = calc_remained_days(name, full_date, c_date)
        # ignore the options' iv of the last day
        iv = calc_iv(name, o_price, u_price, s_price, days, o_type, interest_rate)
        return iv, days


oc_mgr = OptionsContractsManager()
