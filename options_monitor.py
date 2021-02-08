# encoding: UTF-8

from options_monitor.utilities_calendar import get_last_trade_dates
from options_monitor.data_ref import SCHEDULE_HOUR, DATE_FORMAT
from options_monitor.utilities import mk_notification
from options_monitor.utilities_hv import sort_hv20250
from options_monitor.schedule_manager import ScheduleManager
from options_monitor.util_dingding import send_html_msg
from options_monitor.data_manager import \
    CSIndex000300DataManager, CFFEDataManager, SHFEDataManager, \
    DCEDataManager, CZCEDataManager, \
    CFFEOptionsDataManager, SHFEOptionsDataManager, DCEOptionsDataManager, \
    CZCEOptionsDataManager, calendar_manager
from options_monitor.logger import logger
from datetime import datetime
from time import sleep
import pandas as pd
import threadpool
import argparse

pd.set_option('mode.chained_assignment', None)

#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _crontab = f'05 {SCHEDULE_HOUR} * * *'
    pool_size = 10
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
        # do download data and analyze
        csindex000300_mgr = CSIndex000300DataManager(dates)
        cffe_mgr = CFFEDataManager(dates)
        shfe_mgr = SHFEDataManager(dates)
        dce_mgr = DCEDataManager(dates)
        czce_mgr = CZCEDataManager(dates)
        requests = threadpool.makeRequests(
            lambda x: x.download_raw_data(),
            [csindex000300_mgr, cffe_mgr, shfe_mgr, dce_mgr, czce_mgr])
        pool = threadpool.ThreadPool(self.pool_size)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        logger.info('all futures data downloaded. ')
        res_idx300, csindex300_dfs, csindex300_df_all = csindex000300_mgr.analyze(now_date_str)
        res_cffe, cffe_dfs, cffe_df_all = cffe_mgr.analyze(now_date_str)
        res_shfe, shfe_dfs, shfe_df_all = shfe_mgr.analyze(now_date_str)
        res_dce, dce_dfs, dce_df_all = dce_mgr.analyze(now_date_str)
        res_czce, czce_dfs, czce_df_all = czce_mgr.analyze(now_date_str)
        if not all([res_idx300, res_cffe, res_shfe, res_dce, res_czce]):
            # failed
            logger.info('futures info fetch failed. ')
            return False
        # for options
        cffe_options_mgr = CFFEOptionsDataManager(dates, csindex300_df_all)
        shfe_options_mgr = SHFEOptionsDataManager(dates, shfe_df_all)
        dce_options_mgr = DCEOptionsDataManager(dates, dce_df_all)
        czce_options_mgr = CZCEOptionsDataManager(dates, czce_df_all)
        requests2 = threadpool.makeRequests(
            lambda x: x.download_raw_data(),
            [cffe_options_mgr, shfe_options_mgr, dce_options_mgr, czce_options_mgr])
        pool2 = threadpool.ThreadPool(self.pool_size)
        [pool2.putRequest(req) for req in requests2]
        pool2.wait()
        logger.info('all options data downloaded. ')
        all_dfs = self.analyze([(cffe_options_mgr, csindex300_dfs),
                                (shfe_options_mgr, shfe_dfs),
                                (dce_options_mgr,  dce_dfs),
                                (czce_options_mgr, czce_dfs)], now_date_str)
        if all_dfs is False:
            logger.info('options info fetch failed. ')
            return False
        this_date, final_df = sort_hv20250(all_dfs)
        stat_df = mk_notification(final_df)
        send_html_msg(this_date, stat_df, self._push_msg)
        self._push_msg = True
        self._immediately = False
        self.day_pushed = now_date_str
        logger.info(f'{now_date_str} schedule task done. ')
        return self.clear_and_return_true()

    def analyze(self, mgrs: list, date_str: str):
        if self._recalculate_siv is True:
            logger.info('recalculate siv for all. ')
            # only need recalculate siv once
            self._recalculate_siv = False
            [mgr._remote_data.recalculate_siv_test() for mgr, _ in mgrs]
        results = map(lambda mgr: mgr[0].analyze(mgr[1], date_str), mgrs)
        all_dfs = []
        for result, analyze_dfs, _data_all in results:
            if result is False:
                return False
            all_dfs += analyze_dfs
        return all_dfs

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
