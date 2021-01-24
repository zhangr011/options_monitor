# encoding: UTF-8

import unittest as ut
from options_monitor.data_manager import DCEDataManager, DCEOptionsDataManager
from options_monitor.utilities_calendar import get_last_trade_dates


#----------------------------------------------------------------------
class TestDCEDataManager(ut.TestCase):

    def testDCEDataManager(self):
        """"""
        dates = get_last_trade_dates()
        mgr = DCEDataManager(dates)
        mgr.download_raw_data()
        _li, df_extra = mgr._remote_data.get_last_index()
        mgr2 = DCEOptionsDataManager(dates, df_extra)
        mgr2.download_raw_data()
        # mgr2._remote_data.recalculate_siv_test()


if __name__ == '__main__':
    ut.main()
