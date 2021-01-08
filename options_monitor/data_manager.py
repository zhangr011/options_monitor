# encoding: UTF-8

from .utilities import \
    CHECK_SECTION, make_sure_dirs_exist, \
    get_file_path, generate_csv_checksums, combine_all, \
    analyze_diff_percent, load_futures_by_csv, load_vix_by_csv, \
    close_ma5_ma10_ma20, generate_futures_chain
from .remote_data import RemoteDataFactory, SYNC_DATA_MODE
from .logger import logger

import os, logging
import pandas as pd


#----------------------------------------------------------------------
class DataManager():

    data_mode = None
    data_path = ''

    def __init__(self, trade_dates: list = []):
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
        rdata = data_fac.create(self.data_mode, self._trade_dates)
        rdata.sync_data()
        logger.info('all data downloaded. ')

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        raise NotImplementedError


#----------------------------------------------------------------------
class CSIndex000300DataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300
    data_path = get_file_path('csindex_000300')

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        pass


#----------------------------------------------------------------------
class CFFEDataManager(DataManager):

    data_mode = SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE
    data_path = get_file_path('cffe')

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        pass
