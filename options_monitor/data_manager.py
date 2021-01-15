# encoding: UTF-8

from .utilities import \
    make_sure_dirs_exist, DATA_ROOT, DATE_FORMAT, sse_calendar, \
    PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, CLOSE_PRICE_NAME, TOTAL_ROW_KEY, \
    HV_20_NAME, HV_250_NAME, HV_20_250_NAME, HV_PER
from .utilities_hv import \
    HV_DISTRIBUTION_PERIODS, historical_volatility, calc_percentage
from .remote_data import RemoteDataFactory, SYNC_DATA_MODE
from .logger import logger
from functools import cached_property

import os, datetime
import pandas as pd


#----------------------------------------------------------------------
class DataManager():

    data_mode = None
    data_path = DATA_ROOT
    local = ''

    def __init__(self, trade_dates: pd.Index = None):
        """Constructor"""
        self._trade_dates = trade_dates
        self._remote_data = None

    #----------------------------------------------------------------------
    def download_raw_data(self, downloaded = False):
        """download the data"""
        if downloaded is True:
            return
        make_sure_dirs_exist(self.data_path)
        logger.info(f'start downloading data from {self.data_mode}')
        data_fac = RemoteDataFactory(self.data_path)
        self._remote_data = data_fac.create(self.local, self.data_mode, self._trade_dates)
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
        return df

    #----------------------------------------------------------------------
    def check_closed(self, date_str: str):
        """check if the market closed. """
        calendar = self.get_trading_calendar
        try:
            date_row = calendar.loc[date_str]
            return date_row['closed']
        except KeyError:
            # not in the calendar, it's ok
            return False


#----------------------------------------------------------------------
calendar_manager = CFFECalendarDataManager()


#----------------------------------------------------------------------
class CFFEDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE
    local = 'cffe'


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
