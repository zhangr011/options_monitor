# encoding: UTF-8

from .data_ref import \
    DATE_FORMAT, sse_calendar, \
    INDEX_KEY, PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, \
    CLOSE_PRICE_NAME, TOTAL_ROW_KEY, \
    HV_20_NAME, HV_250_NAME, HV_20_250_NAME, HV_PER, IV_NAME, IV_PER
from .utilities_hv import \
    HV_DISTRIBUTION_PERIODS, historical_volatility, calc_percentage
from .data_ref import SYNC_DATA_MODE
from .logger import logger
from functools import cached_property

import os, datetime
import trading_calendars as tcs
import numpy as np
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
    def get_products_dataframe(self):
        """get the products' dataframe"""
        lindex, df = self._remote_data.get_last_index()
        df = df[df[PRODUCT_ID_NAME] == TOTAL_ROW_KEY]
        df.reset_index(inplace = True)
        df.drop_duplicates(subset = [INDEX_KEY, PRODUCT_ID_NAME, PRODUCT_GROUP_NAME],
                           keep = 'last', inplace = True)
        df.set_index(INDEX_KEY, inplace = True)
        group = df.groupby(PRODUCT_GROUP_NAME)
        dfs = []
        for pid in group.groups.keys():
            dfs.append(group.get_group(pid))
        return dfs, df

    #----------------------------------------------------------------------
    def analyze(self):
        """"""
        pass


#----------------------------------------------------------------------
class FuturesDataManager(DataManager):

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
        products, df_all = self.get_products_dataframe()
        dfs = map(lambda df: self.analyze_one_hv(df), products)
        return list(dfs), df_all


#----------------------------------------------------------------------
class OptionsDataManager(DataManager):

    futures_options_map = {}

    #----------------------------------------------------------------------
    def get_options_id(self, fid: str):
        """get the options' product id"""
        return self.futures_options_map.get(fid, None)

    #----------------------------------------------------------------------
    def get_options_df(self, fid: str, options_dfs: list):
        """"""
        oid = self.get_options_id(fid)
        if oid is None:
            oid = fid
        for o_df in options_dfs:
            if o_df[PRODUCT_GROUP_NAME][-1] == oid:
                return o_df[[IV_NAME]]
        return None

    #----------------------------------------------------------------------
    def join_options(self, futures_df: list, options_df: list):
        """"""
        joined = []
        for df_f in futures_df:
            futures_id = df_f[PRODUCT_GROUP_NAME][-1]
            o_df = self.get_options_df(futures_id, options_df)
            if o_df is None:
                df_f[IV_NAME] = np.nan
            else:
                df_f = df_f.join(o_df, how = 'left')
            df_f[IV_PER] = calc_percentage(df_f[IV_NAME])
            joined.append(df_f)
        return joined

    #----------------------------------------------------------------------
    def analyze(self, futures_dfs: list):
        """analyze the data"""
        options, df_all = self.get_products_dataframe()
        dfs = self.join_options(futures_dfs, options)
        # merge the options' data into futures
        return dfs, df_all


#----------------------------------------------------------------------
class CSIndex000300DataManager(FuturesDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300
    local = 'csindex_000300'

    #----------------------------------------------------------------------
    def get_products_dataframe(self):
        """"""
        lindex, df = self._remote_data.get_last_index()
        return [df], df


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
    def check_open(self, date_str: str, check_detail: bool = True):
        """check if the market is opened. """
        checked = date_str in self._trading_sessions
        if checked is True and check_detail is True:
            return self.check_open2(date_str)
        return checked


#----------------------------------------------------------------------
calendar_manager = CFFECalendarDataManager()


#----------------------------------------------------------------------
class CFFEDataManager(FuturesDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE
    local = 'cffe'


#----------------------------------------------------------------------
class CFFEOptionsDataManager(OptionsDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE_OPTIONS
    local = 'cffe_options'
    futures_options_map = {'csidx300' : 'IO'}


#----------------------------------------------------------------------
class SHFEDataManager(FuturesDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_SHFE
    local = 'shfe'


#----------------------------------------------------------------------
class SHFEOptionsDataManager(OptionsDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_SHFE_OPTIONS
    local = 'shfe_options'

    #----------------------------------------------------------------------
    def get_options_id(self, fid: str):
        """get the options' product id"""
        return fid.replace('_f', '')


#----------------------------------------------------------------------
class DCEDataManager(FuturesDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_DCE
    local = 'dce'


#----------------------------------------------------------------------
class DCEOptionsDataManager(OptionsDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_DCE_OPTIONS
    local = 'dce_options'


#----------------------------------------------------------------------
class CZCEDataManager(FuturesDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CZCE
    local = 'czce'


#----------------------------------------------------------------------
class CZCEOptionsDataManager(OptionsDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CZCE_OPTIONS
    local = 'czce_options'
