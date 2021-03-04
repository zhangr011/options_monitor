# encoding: UTF-8

from options_monitor.utilities_calendar import get_last_trade_dates
from options_monitor.data_ref import SCHEDULE_HOUR, DATE_FORMAT
from options_monitor.utilities import mk_notification
from options_monitor.utilities_hv import sort_hv20250
from options_monitor.schedule_manager import ScheduleManager
from options_monitor.util_dingding import send_html_msg
from options_monitor.data_manager import calendar_manager, SIVManager
from options_monitor.logger import logger
from datetime import datetime
from time import sleep
import pandas as pd
import argparse

pd.set_option('mode.chained_assignment', None)

#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _crontab = f'05 {SCHEDULE_HOUR} * * *'
    day_pushed = None

    def __init__(self, immediately: bool = False, push_msg: bool = False, recalculate_siv: bool = False):
        """"""
        self._immediately = immediately
        self._push_msg = push_msg
        self._recalculate_siv = recalculate_siv
        super(MonitorScheduleManager, self).__init__(immediately)

    def do_timeout(self):
        """"""
        logger.info('start schedule task. ')
        dates = get_last_trade_dates()
        now_date_str = dates[-1]
        if not calendar_manager.check_open(now_date_str) or now_date_str == self.day_pushed:
            if now_date_str == self.day_pushed:
                logger.info(f'{now_date_str} info has been pushed. ')
            else:
                logger.info(f'market is closed: {now_date_str}')
            if self._immediately is not True:
                return self.clear_and_return_true()
        siv_manager = SIVManager()
        all_dfs = siv_manager.prepare(dates, now_date_str, True, self._recalculate_siv)
        if all_dfs is False:
            logger.info('options info fetch failed. ')
            return False
        this_date, final_df = sort_hv20250(all_dfs)
        stat_df = mk_notification(final_df)
        send_html_msg(this_date, stat_df, self._push_msg)
        self._push_msg = True
        self._immediately = False
        self._recalculate_siv = False
        self.day_pushed = now_date_str
        logger.info(f'{now_date_str} schedule task done. ')
        return self.clear_and_return_true()

    def clear_and_return_true(self):
        """clear the _day_index and return True"""
        return True


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--imm', type = bool, dest = 'immediately', default = False,
                            help = 'immediately analyze when started. ')
    arg_parser.add_argument('--push', type = bool, dest = 'push_msg', default = False,
                            help = 'push the message. ')
    arg_parser.add_argument('--recalculate_siv', type = bool, dest = 'recalculate_siv',
                            default = False,
                            help = 'recalculate the siv before analyze, this may be used when siv calculation method changed. ')
    args = arg_parser.parse_args()
    # logger.info('', args.immediately, type(args.immediately), args.push_msg, type(args.push_msg), args.recalculate_siv, type(args.recalculate_siv))
    mgr = MonitorScheduleManager(args.immediately, args.push_msg, args.recalculate_siv)
    logger.info('options monitor started. ')
    while True:
        sleep(1)
