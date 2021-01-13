# encoding: UTF-8

from options_monitor.utilities import get_last_trade_dates, SCHEDULE_HOUR
from options_monitor.schedule_manager import ScheduleManager
from options_monitor.util_dingding import send_md_msg
from options_monitor.data_manager import CSIndex000300DataManager
from options_monitor.logger import logger
from datetime import datetime, timezone
from time import sleep


#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _crontab = f'05 {SCHEDULE_HOUR} * * *'
    _day_index = None

    def do_timeout(self):
        """"""
        dates = get_last_year_trade_dates()
        csindex000300_mgr = CSIndex000300DataManager(dates)
        logger.info('start schedule task. ')
        logger.info('schedule task done. ')
        return self.clear_and_return_true()

    def clear_and_return_true(self):
        """clear the _day_index and return True"""
        self._day_index = None
        return True


if __name__ == '__main__':
    mgr = MonitorScheduleManager(False)
    logger.info('cboe monitor started. ')
    while True:
        sleep(1)
