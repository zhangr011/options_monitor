# encoding: UTF-8

import unittest as ut
from options_monitor.utilities import get_last_trade_dates
from options_monitor.data_manager import CZCEDataManager


#----------------------------------------------------------------------
class TestCZCEDataManager(ut.TestCase):

    def testCZCEDataManager(self):
        """"""
        dates = get_last_trade_dates()
        mgr = CZCEDataManager(dates)
        mgr.download_raw_data()


if __name__ == '__main__':
    ut.main()
