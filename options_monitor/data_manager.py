# encoding: UTF-8

from .utilities import \
    make_sure_dirs_exist, DATA_ROOT, get_file_path, combine_all, \
    analyze_diff_percent, load_futures_by_csv, close_ma5_ma10_ma20
from .remote_data import RemoteDataFactory, SYNC_DATA_MODE
from .logger import logger

import os, logging
import pandas as pd


#----------------------------------------------------------------------
class DataManager():

    data_mode = None
    data_path = DATA_ROOT
    local = ''

    def __init__(self, trade_dates: pd.DataFrame):
        """Constructor"""
        self._trade_dates = trade_dates

    #----------------------------------------------------------------------
    def download_raw_data(self, downloaded = False):
        """download the data"""
        if downloaded is True:
            return
        make_sure_dirs_exist(self.data_path)
        logger.info(f'start downloading data from {self.data_mode}')
        data_fac = RemoteDataFactory(self.data_path)
        rdata = data_fac.create(self.local, self.data_mode, self._trade_dates)
        rdata.sync_data()
        logger.info('all data downloaded. ')

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        raise NotImplementedError


#----------------------------------------------------------------------
class CSIndex000300DataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300
    local = 'csindex_000300'

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        pass


#----------------------------------------------------------------------
class CFFEDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE
    local = 'cffe'

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        pass
