# encoding: UTF-8

import unittest as ut
from options_monitor.utilities import get_last_trade_dates, CLOSE_PRICE_NAME
from options_monitor.data_manager import CZCEDataManager


#----------------------------------------------------------------------
class TestCZCEDataManager(ut.TestCase):

    def testCZCEDataManager(self):
        """"""
        dates = get_last_trade_dates()
        mgr = CZCEDataManager(dates)
        mgr.download_raw_data()
        mgr._remote_data.fix_close_data()
        _li, df = mgr._remote_data.get_last_index()
        self.assertEqual(False, (df[CLOSE_PRICE_NAME] == 0).any())


if __name__ == '__main__':
    ut.main()
