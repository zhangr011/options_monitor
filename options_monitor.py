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

pd.set_option('mode.chained_assignment', None)

ANALYZE_WHEN_START = False
SEND_MSG           = True

#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _crontab = f'05 {SCHEDULE_HOUR} * * *'
    pool_size = 10

    def do_timeout(self):
        """"""
        logger.info('start schedule task. ')
        dates = get_last_trade_dates()
        now_date_str = datetime.now().strftime(DATE_FORMAT)
        if not calendar_manager.check_open(now_date_str):
            logger.info(f'market is closed: {now_date_str}')
            if ANALYZE_WHEN_START is False:
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
        csindex300_dfs, csindex300_df_all = csindex000300_mgr.analyze()
        cffe_dfs, cffe_df_all = cffe_mgr.analyze()
        shfe_dfs, shfe_df_all = shfe_mgr.analyze()
        dce_dfs, dce_df_all = dce_mgr.analyze()
        czce_dfs, czce_df_all = czce_mgr.analyze()
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
                                (czce_options_mgr, czce_dfs)])
        this_date, final_df = sort_hv20250(all_dfs)
        stat_df = mk_notification(final_df)
        send_html_msg(this_date, stat_df, SEND_MSG)
        logger.info('schedule task done. ')
        return self.clear_and_return_true()

    def analyze(self, mgrs: list):
        if ANALYZE_WHEN_START is True:
            map(lambda mgr: mgr[0]._remote_data.recalculate_siv_test(), mgrs)
        results = map(lambda mgr: mgr[0].analyze(mgr[1]), mgrs)
        all_dfs = []
        for analyze_dfs, _data_all in results:
            all_dfs += analyze_dfs
        return all_dfs

    def clear_and_return_true(self):
        """clear the _day_index and return True"""
        return True


if __name__ == '__main__':
    mgr = MonitorScheduleManager(ANALYZE_WHEN_START)
    logger.info('options monitor started. ')
    while True:
        sleep(1)
