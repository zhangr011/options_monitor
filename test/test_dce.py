# encoding: UTF-8

import unittest as ut
from options_monitor.data_manager import DCEDataManager
from options_monitor.utilities import get_last_trade_dates


#----------------------------------------------------------------------
class TestDCEDataManager(ut.TestCase):

    def testDCEDataManager(self):
        """"""
        dates = get_last_trade_dates()
        mgr = DCEDataManager(dates)
        mgr.download_raw_data()


if __name__ == '__main__':
    ut.main()
