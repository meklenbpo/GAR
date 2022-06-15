"""
GAR
---

GAR is a module that converts source GAR data into the customer-
specified format.
"""

import sys

from loguru import logger


# format loguru logger format
logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<level>{message}</level>"
)
logger.remove()
logger.add(sys.stderr, format=logger_format, level="TRACE")
