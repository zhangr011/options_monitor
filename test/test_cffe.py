# encoding: UTF-8

import unittest as ut

from options_monitor.utilities_calendar import get_last_trade_dates
from options_monitor.data_manager import CFFEDataManager


#----------------------------------------------------------------------
class TestCFFEData(ut.TestCase):

    def testData(self):
        dates = get_last_trade_dates()
        mgr = CFFEDataManager(dates)
        mgr.download_raw_data()


if __name__ == '__main__':
    ut.main()
