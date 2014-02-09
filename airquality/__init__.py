# -*- coding: utf-8 -*-
"""Stores readings in mongodb"""

import logging
from airquality import metadata
from .settings import LOG_FILE


__version__ = metadata.version
__author__ = metadata.authors[0]
__license__ = metadata.license
__copyright__ = metadata.copyright


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=LOG_FILE,
                    filemode='w')
