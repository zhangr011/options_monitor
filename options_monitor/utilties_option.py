# encoding: UTF-8

from .logger import logger

try:
    # 欧式期权
    import black_76_cython as black_76
    # 美式期货期权
    import binomial_tree_cython as binomial_tree
    # 欧式股票期权
    import black_scholes_cython as black_scholes
except ImportError:
    from .pricing import (
        black_76, binomial_tree, black_scholes
    )
    logger.info("Faile to import cython option pricing model, please rebuild with cython in cmd.")
    logger.info('pip3 install ./pricing/cython_model/binomial_tree_cython')
    logger.info('pip3 install ./pricing/cython_model/black_76_cython')
    logger.info('pip3 install ./pricing/cython_model/black_scholes_cython')


from enum import Enum


class PriceOptionType(Enum):
    CALL = 1
    PUT  = -1


class PriceModelIndex(Enum):
    EUROPEAN_FUTURES = 1
    EUROPEAN_STOCKS  = 2
    AMERICAN_FUTURES = 3


PRICING_MODELS = {
    PriceModelIndex.EUROPEAN_FUTURES : black_76,
    PriceModelIndex.EUROPEAN_STOCKS  : black_scholes,
    PriceModelIndex.AMERICAN_FUTURES : binomial_tree
}


ANNUAL_DAYS   = 365
INTEREST_RATE = 0.0435 # from czce


OPTION_TYPE_MAP = {
    'csidx300' : PriceModelIndex.EUROPEAN_FUTURES,
    'au_f'     : PriceModelIndex.EUROPEAN_FUTURES
}


#----------------------------------------------------------------------
def get_pricing_model_by_gid(gid):
    idx = OPTION_TYPE_MAP.get(gid, PriceModelIndex.AMERICAN_FUTURES)
    model = PRICING_MODELS.get(idx)
    return model


#----------------------------------------------------------------------
def calc_iv(gid: str, option_price: float, underlying_price: float,
            strike_price: float, days: int, otype: PriceOptionType,
            interest_rate: float = INTEREST_RATE):
    model = get_pricing_model_by_gid(gid)
    return model.calculate_impv(option_price, underlying_price, strike_price,
                                interest_rate, days / ANNUAL_DAYS, otype.value)
