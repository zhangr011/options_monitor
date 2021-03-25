# encoding: UTF-8

from .data_ref import \
    DATE_FORMAT, sse_calendar, \
    INDEX_KEY, PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, \
    CLOSE_PRICE_NAME, TOTAL_ROW_KEY, \
    HV_20_NAME, HV_250_NAME, HV_20_250_NAME, HV_PER, IV_NAME, IV_PER, \
    HV_MAX, HV_MIN, IV_MAX, IV_MIN
from .utilities_hv import \
    HV_DISTRIBUTION_PERIODS, historical_volatility, \
    calc_percentage, historical_max_min
from .data_ref import SYNC_DATA_MODE
from .logger import logger
from functools import cached_property
from .singleton import Singleton

import os, datetime, time
import threadpool
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
    def init_remote_data(self, force_reset: bool = False):
        """init the remote data if needed."""
        if self._remote_data is None or force_reset is True:
            from .remote_data import remote_data_fac
            self._remote_data = remote_data_fac.create(
                self.local, self.data_mode, self._trade_dates, self._df_extra)

    #----------------------------------------------------------------------
    def download_raw_data(self):
        """download the data"""
        # force to reset the self._remote_data before download
        self.init_remote_data(True)
        logger.info(f'start downloading data from {self.data_mode}')
        self._remote_data.sync_data()
        logger.info(f'{self.data_mode} all data downloaded. ')

    #----------------------------------------------------------------------
    def get_products_dataframe(self, date_str: str = None):
        """get the products' dataframe"""
        self.init_remote_data()
        lindex, df_raw = self._remote_data.get_last_index()
        if date_str and date_str > lindex:
            return False, None, None
        df = df_raw[df_raw[PRODUCT_ID_NAME] == TOTAL_ROW_KEY]
        df.reset_index(inplace = True)
        df.drop_duplicates(subset = [INDEX_KEY, PRODUCT_ID_NAME, PRODUCT_GROUP_NAME],
                           keep = 'last', inplace = True)
        df.set_index(INDEX_KEY, inplace = True)
        group = df.groupby(PRODUCT_GROUP_NAME)
        dfs = []
        for pid in group.groups.keys():
            dfs.append(group.get_group(pid))
        return True, dfs, df_raw

    #----------------------------------------------------------------------
    def analyze(self, date_str: str = None):
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
        historical_max_min(df, HV_20_NAME, HV_MAX, HV_MIN)
        return df

    #----------------------------------------------------------------------
    def analyze(self, date_str: str = None):
        """analyze the data"""
        result, products, df_all = self.get_products_dataframe(date_str)
        if result is True:
            dfs = map(lambda df: self.analyze_one_hv(df), products)
            return True, list(dfs), df_all
        return False, None, None


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
            historical_max_min(df_f, IV_NAME, IV_MAX, IV_MIN)
            joined.append(df_f)
        return joined

    #----------------------------------------------------------------------
    def analyze(self, futures_dfs: list, date_str: str = None):
        """analyze the data"""
        result, options, df_all = self.get_products_dataframe(date_str)
        if result is True:
            dfs = self.join_options(futures_dfs, options)
            # merge the options' data into futures
            return True, dfs, df_all
        return False, None, None


#----------------------------------------------------------------------
class CSIndex000300DataManager(FuturesDataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300
    data_mode2 = SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300_DAILY
    local = 'csindex_000300'

    #----------------------------------------------------------------------
    def init_remote_data(self, force_reset: bool = False):
        """init the remote data if needed."""
        if self._remote_data is None or self._remote_data2 is None or \
           force_reset is True:
            from .remote_data import remote_data_fac
            self._remote_data = remote_data_fac.create(
                self.local, self.data_mode, self._trade_dates, self._df_extra)
            self._remote_data2 = remote_data_fac.create(
                self.local, self.data_mode2, self._trade_dates, self._df_extra)

    #----------------------------------------------------------------------
    def download_raw_data(self):
        """download the data"""
        # force to reset the self._remote_data before download
        super(CSIndex000300DataManager, self).download_raw_data()
        time.sleep(0.5)
        logger.info(f'start downloading data from {self.data_mode2}')
        self._remote_data2.sync_data()
        logger.info(f'{self.data_mode2} all data downloaded. ')

    #----------------------------------------------------------------------
    def get_products_dataframe(self, date_str: str = None):
        """"""
        self.init_remote_data()
        lindex, df = self._remote_data.get_last_index()
        if date_str and date_str > lindex:
            return False, None, None
        return True, [df], df


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


#----------------------------------------------------------------------
class SIVManager(metaclass = Singleton):

    pool_size = 10

    def prepare(self, dates: pd.DataFrame, now_date_str: str,
                download: bool = False, recalc_siv: bool = False):
        """prepare the data"""
        if dates is None:
            # reset the download flag
            download = False
        # do download data and analyze
        csindex000300_mgr = CSIndex000300DataManager(dates)
        cffe_mgr = CFFEDataManager(dates)
        shfe_mgr = SHFEDataManager(dates)
        dce_mgr = DCEDataManager(dates)
        czce_mgr = CZCEDataManager(dates)
        if download is True:
            requests = threadpool.makeRequests(
                lambda x: x.download_raw_data(),
                [csindex000300_mgr, cffe_mgr, shfe_mgr, dce_mgr, czce_mgr])
            pool = threadpool.ThreadPool(self.pool_size)
            [pool.putRequest(req) for req in requests]
            pool.wait()
            logger.info('all futures data downloaded. ')
        res_idx300, csindex300_dfs, csindex300_df_all = csindex000300_mgr.analyze(now_date_str)
        res_cffe, cffe_dfs, cffe_df_all = cffe_mgr.analyze(now_date_str)
        res_shfe, shfe_dfs, shfe_df_all = shfe_mgr.analyze(now_date_str)
        res_dce, dce_dfs, dce_df_all = dce_mgr.analyze(now_date_str)
        res_czce, czce_dfs, czce_df_all = czce_mgr.analyze(now_date_str)
        if not all([res_idx300, res_cffe, res_shfe, res_dce, res_czce]):
            # failed
            logger.info('futures info fetch failed. ')
            return False
        # for options
        cffe_options_mgr = CFFEOptionsDataManager(dates, csindex300_df_all)
        shfe_options_mgr = SHFEOptionsDataManager(dates, shfe_df_all)
        dce_options_mgr = DCEOptionsDataManager(dates, dce_df_all)
        czce_options_mgr = CZCEOptionsDataManager(dates, czce_df_all)
        if download is True:
            requests2 = threadpool.makeRequests(
                lambda x: x.download_raw_data(),
                [cffe_options_mgr, shfe_options_mgr, dce_options_mgr, czce_options_mgr])
            pool2 = threadpool.ThreadPool(self.pool_size)
            [pool2.putRequest(req) for req in requests2]
            pool2.wait()
            logger.info('all options data downloaded. ')
        all_dfs = self.analyze([(cffe_options_mgr, csindex300_dfs),
                                (shfe_options_mgr, shfe_dfs),
                                (dce_options_mgr,  dce_dfs),
                                (czce_options_mgr, czce_dfs)], now_date_str, recalc_siv)
        return all_dfs

    #----------------------------------------------------------------------
    def analyze(self, mgrs: list, date_str: str, recalc_siv: bool):
        """analyze the options data"""
        if recalc_siv is True:
            logger.info('recalculate siv for all. ')
            # only need recalculate siv once
            [mgr._remote_data.recalculate_siv_test() for mgr, _ in mgrs]
        # do analyze
        results = map(lambda mgr: mgr[0].analyze(mgr[1], date_str), mgrs)
        all_dfs = []
        for result, analyze_dfs, _data_all in results:
            if result is False:
                return False
            all_dfs += analyze_dfs
        return all_dfs
