# encoding: UTF-8

import unittest as ut

from options_monitor.utilities import get_last_trade_dates, check_date_in


#----------------------------------------------------------------------
class TestUtilities(ut.TestCase):

    def testGetTradeDates(self):
        """"""
        dates_df = get_last_trade_dates(30)
        self.assertEqual(False, dates_df.empty)

    def testCheckDateIn(self):
        """"""
        dates_df = get_last_trade_dates(30)
        self.assertEqual(True, check_date_in('2021-01-08', dates_df))
        self.assertEqual(False, check_date_in('2021-01-10', dates_df))


if __name__ == '__main__':
    ut.main()
