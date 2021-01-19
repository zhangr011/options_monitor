# encoding: UTF-8

from .logger import logger

try:
    import black_76_cython as black_76
    import binomial_tree_cython as binomial_tree
    import black_scholes_cython as black_scholes
except ImportError:
    from .pricing import (
        black_76, binomial_tree, black_scholes
    )
    logger.info("Faile to import cython option pricing model, please rebuild with cython in cmd.")
    logger.info('pip3 install ./pricing/cython_model/binomial_tree_cython')
    logger.info('pip3 install ./pricing/cython_model/black_76_cython')
    logger.info('pip3 install ./pricing/cython_model/black_scholes_cython')
