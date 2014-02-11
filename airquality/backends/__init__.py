# -*- coding: utf-8 -*-
"""
Persistence mechanisms.

.. To interact with the repository::

    from airquality import backends, settings
    from contextlib import closing

    with closing(backends.Repository(settings)) as repo:
        repo.save( [{'tweet_id': 12532657652, 'text': 'Hello World'}] )
"""
import traceback
import logging
import sys
from enum import Enum
from collections import Sequence
from functools import wraps

__author__ = 'Gavin Bong'


class Reading(Enum):
    hourly = u'hourly'
    instant = u'instant'
    avg24h = u'24havg'
    avg12h = u'12havg'
    everything = u'everything'


class Backend(Enum):
    mongo = u'mongo'
    postgres = 'postgresql'


class MongoProxy(object):
    """Proxy to MongoRepository. Don't use!
    Use `Repository` class instead.
    """
    def __init__(self, repo_class, settings):
        #logging.debug(repo_class)
        self.collection_name = getattr(settings, 'MONGO_COLLECTION')
        self.persister = repo_class(
            getattr(settings, 'MONGO_URL'),
            getattr(settings, 'MONGO_DATABASE'))

    def is_alive(self):
        return self.persister is not None

    def kill(self):
        if self.persister:
            self.persister.close()
        self.persister = None

    def save(self, readings):
        collection = self.persister.get_collection(self.collection_name)
        result = []
        for reading in readings:
            result.append(self.persister.save(collection, reading))
        return result

    def debug(self):
        collection = self.persister.get_collection(self.collection_name)
        acursor = self.persister.find(collection,
                                      {'type.nature': '24havg'}, limit=30)
        result = [reading for reading in acursor]
        return result


class Repository(object):
    """Entrypoint to persists pollutants' readings."""

    proxies = {
        Backend.mongo.value: MongoProxy
    }

    def __init__(self, settings, **kwargs):
        try:
            engine_tuple = getattr(settings, 'STORAGE_ENGINE')
            self.engine = engine_tuple[0]
            Backend(self.engine)
            self.storage_klasses = self._get_klazz(engine_tuple)
            proxy_class = self.proxies.get(self.engine)
            self.proxy = proxy_class(self.storage_klasses[self.engine],
                                     settings)
        except:
            exc_type, exc_value, exc_tb = sys.exc_info()
            logging.exception(exc_tb)
            traceback.print_exc()
            raise RuntimeError('construction failed')

    def _checker(f):
        @wraps(f)
        def wrapper(*args, **kwds):
            if len(args) > 0:
                if hasattr(args[0], '_check_alive'):
                    getattr(args[0], '_check_alive')()
            return f(*args, **kwds)
        return wrapper

    @_checker
    def save(self, readings):
        if not readings:
            return 0, None

        if not isinstance(readings, Sequence):
            raise ValueError('readings must be a Sequence')

        return self.proxy.save(readings)

    @_checker
    def debug(self):
        """TODO: redo"""
        return self.proxy.debug()

    def close(self):
        self.proxy.kill()

    def _check_alive(self):
        if not self.proxy.is_alive():
            raise RuntimeError('disconnected')

    @classmethod
    def _get_klazz(cls, engine):
        segments = engine[1].split('.')
        modulepath = '.'.join(segments[:-1])
        m = __import__(modulepath)
        for segment in segments[1:]:
            m = getattr(m, segment)
        return {engine[0]: m}

__all__ = ['Repository', 'Reading', 'Backend']
