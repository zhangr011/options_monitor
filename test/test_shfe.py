# encoding: UTF-8

import unittest as ut
from options_monitor.data_manager import SHFEDataManager, SHFEOptionsDataManager
from options_monitor.remote_data import calculate_siv_by_turnovers
from options_monitor.utilities_calendar import get_last_trade_dates


#----------------------------------------------------------------------
class TestSHFEDataManager(ut.TestCase):

    def testSHFEDataManager(self):
        """"""
        dates = get_last_trade_dates()
        mgr = SHFEDataManager(dates)
        mgr.download_raw_data()
        # dfs = mgr.analyze()
        # mgr._remote_data.drop_local_last_date_data()
        _li, df_extra = mgr._remote_data.get_last_index()
        mgr2 = SHFEOptionsDataManager(dates, df_extra)
        mgr2.download_raw_data()
        # mgr2._remote_data.recalculate_siv_test()



if __name__ == '__main__':
    ut.main()
