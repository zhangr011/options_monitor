# encoding: UTF-8

import unittest as ut
from options_monitor.data_manager import SHFEDataManager
from options_monitor.utilities import get_last_trade_dates


#----------------------------------------------------------------------
class TestSHFEDataManager(ut.TestCase):

    def testSHFEDataManager(self):
        """"""
        dates = get_last_trade_dates()
        mgr = SHFEDataManager(dates)
        mgr.download_raw_data()
        dfs = mgr.analyze()
        # mgr._remote_data.drop_local_last_date_data()
        pass


if __name__ == '__main__':
    ut.main()
