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
from collections import Sequence
from functools import wraps

__author__ = 'Gavin Bong'


class MongoProxy(object):
    """Proxy to MongoRepository"""
    def __init__(self, repo_class, settings):
        #logging.debug(repo_class)
        self.collection_name = getattr(settings, 'MONGO_COLLECTION')
        self.persister = repo_class(
            getattr(settings, 'MONGO_URL'),
            getattr(settings, 'MONGO_DATABASE'))

    def is_alive(self):
        return self.persister is not None

    def kill(self):
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
        for j in self.persister.find_one(collection):
            logging.debug(j)


class Repository(object):

    MONGO = 'mongo'
    POSTGRES = 'postgresql'

    proxies = {
        MONGO: MongoProxy
    }

    def __init__(self, settings, **kwargs):
        try:
            engine_tuple = getattr(settings, 'STORAGE_ENGINE')
            self.engine = engine_tuple[0]
            self.storage_klasses = self._get_klazz(engine_tuple)
            proxy_class = self.proxies.get(self.engine)
            self.proxy = proxy_class(self.storage_klasses[self.engine],
                                     settings)
        except:
            traceback.print_exc()
            raise ValueError('construction failed')

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
        self.proxy.debug()

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

__all__ = ['Repository']
