# encoding: UTF-8

import unittest as ut

from datetime import datetime
from options_monitor.utilities import \
    get_last_trade_dates, check_date_in, sse_calendar, DATE_FORMAT


#----------------------------------------------------------------------
class TestUtilities(ut.TestCase):

    def testGetTradeDates(self):
        """"""
        dates = get_last_trade_dates(30)
        self.assertEqual(False, dates.empty)
        now = datetime.now(tz = sse_calendar.tz)
        now_date_str = now.strftime(DATE_FORMAT)
        if now.hour >= 19:
            self.assertEqual(now_date_str, dates[-1])
        else:
            self.assertNotEqual(now_date_str, dates[-1])


    def testCheckDateIn(self):
        """"""
        dates = get_last_trade_dates(30)
        self.assertEqual(True, check_date_in('2021-01-08', dates))
        self.assertEqual(False, check_date_in('2021-01-10', dates))


if __name__ == '__main__':
    ut.main()
