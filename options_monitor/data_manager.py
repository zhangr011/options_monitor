# encoding: UTF-8

from .data_ref import \
    DATE_FORMAT, sse_calendar, \
    PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, CLOSE_PRICE_NAME, TOTAL_ROW_KEY, \
    HV_20_NAME, HV_250_NAME, HV_20_250_NAME, HV_PER
from .utilities_hv import \
    HV_DISTRIBUTION_PERIODS, historical_volatility, calc_percentage
from .data_ref import SYNC_DATA_MODE
from .logger import logger
from functools import cached_property

import trading_calendars as tcs
import os, datetime
import pandas as pd


#----------------------------------------------------------------------
class DataManager():

    data_mode = None
    local = ''

    #----------------------------------------------------------------------
    def __init__(self, trade_dates: pd.Index = None, df_extra: pd.DataFrame = None):
        """Constructor"""
        self._trade_dates = trade_dates
        self._remote_data = None
        self._df_extra = df_extra
        self.post_initialized()

    #----------------------------------------------------------------------
    def post_initialized(self):
        """do some hook when initialized"""
        pass

    #----------------------------------------------------------------------
    def download_raw_data(self, downloaded = False):
        """download the data"""
        if downloaded is True:
            return
        logger.info(f'start downloading data from {self.data_mode}')
        from .remote_data import remote_data_fac
        self._remote_data = remote_data_fac.create(
            self.local, self.data_mode, self._trade_dates, self._df_extra)
        self._remote_data.sync_data()
        logger.info('all data downloaded. ')


    #----------------------------------------------------------------------
    def analyze_one_hv(self, df: pd.DataFrame):
        """analyze the hv of on product based on index"""
        df[HV_20_NAME] = historical_volatility(df[CLOSE_PRICE_NAME], 20)
        df[HV_250_NAME] = historical_volatility(df[CLOSE_PRICE_NAME], 250)
        df[HV_20_250_NAME] = df[HV_20_NAME] / df[HV_250_NAME]
        df[HV_PER] = calc_percentage(df[HV_20_NAME])
        return df

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        products = self.get_products_dataframe()
        dfs = map(lambda df: self.analyze_one_hv(df), products)
        return list(dfs)

    #----------------------------------------------------------------------
    def get_products_dataframe(self):
        """get the products' dataframe"""
        lindex, df = self._remote_data.get_last_index()
        df = df[df[PRODUCT_ID_NAME] == TOTAL_ROW_KEY]
        group = df.groupby(PRODUCT_GROUP_NAME)
        dfs = []
        for pid in group.groups.keys():
            dfs.append(group.get_group(pid))
        return dfs


#----------------------------------------------------------------------
class CSIndex000300DataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300
    local = 'csindex_000300'

    #----------------------------------------------------------------------
    def get_products_dataframe(self):
        """"""
        lindex, df = self._remote_data.get_last_index()
        return [df]


#----------------------------------------------------------------------
class CFFECalendarDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE_CALENDAR
    local = 'cffe_calendar'

    #----------------------------------------------------------------------
    def post_initialized(self):
        """"""
        super(CFFECalendarDataManager, self).post_initialized()
        self._trading_calendars = tcs.get_calendar('XSHG')
        self._trading_sessions = self._trading_calendars.all_sessions

    #----------------------------------------------------------------------
    def analyze(self):
        pass

    #----------------------------------------------------------------------
    @cached_property
    def get_trading_calendar(self):
        # there is something wrong in sse calendar, so we need to get the cffe's
        end_time = datetime.datetime.now(sse_calendar.tz)
        # about 3 years
        start_time = end_time + datetime.timedelta(days = -1100)
        dates = pd.date_range(start = start_time, end = end_time, freq = 'MS')
        self._trade_dates = dates.strftime(DATE_FORMAT)
        self.download_raw_data()
        li, df = self._remote_data.get_last_index()
        if df is None:
            raise ValueError('calendar data download failed. ')
        return df

    #----------------------------------------------------------------------
    def check_open2(self, date_str: str):
        """check if the market is opened. """
        calendar = self.get_trading_calendar
        try:
            date_row = calendar.loc[date_str]
            return not date_row['closed']
        except KeyError:
            # not in the calendar, it's ok
            return True

    #----------------------------------------------------------------------
    def check_open(self, date_str: str):
        """check if the market is opened. """
        checked = date_str in self._trading_sessions
        if checked is True:
            return self.check_open2(date_str)
        return checked


#----------------------------------------------------------------------
calendar_manager = CFFECalendarDataManager()


#----------------------------------------------------------------------
class CFFEDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE
    local = 'cffe'


#----------------------------------------------------------------------
class CFFEOptionsDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE_OPTIONS
    local = 'cffe_options'


#----------------------------------------------------------------------
class SHFEDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_SHFE
    local = 'shfe'


#----------------------------------------------------------------------
class DCEDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_DCE
    local = 'dce'


#----------------------------------------------------------------------
class CZCEDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CZCE
    local = 'czce'


#----------------------------------------------------------------------
class CZCEOptionsDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CZCE_OPTIONS
    local = 'czce_options'
