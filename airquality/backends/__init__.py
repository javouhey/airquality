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
from types import ListType
from functools import wraps

__author__ = 'Gavin Bong'


class MongoProxy(object):
    """Proxy to MongoRepository"""
    def __init__(self, repo_class, settings):
        logging.info(repo_class)
        self.collection_name = getattr(settings, 'MONGO_COLLECTION')
        self.persister = repo_class()  # TODO configure with MONGO_URL etc..

    def is_alive(self):
        return self.persister is not None

    def kill(self):
        self.persister.close()
        self.persister = None

    def save(self, tweets):
        collection = self.persister.get_collection(self.collection_name)
        for tweet in tweets:
            self.persister.save(collection, tweet)

    def burp(self):
        collection = self.persister.get_collection(self.collection_name)
        for j in self.persister.find_one(collection):
            print j


class Repository(object):

    MONGO = 'mongo'
    POSTGRES = 'postgresql'

    proxies = {
        MONGO: MongoProxy
    }

    def __init__(self, settings, **kwargs):
        self.a = 1
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
    def save(self, tweets):
        if not tweets:
            return 0

        if type(tweets) != ListType:
            raise ValueError('tweets must be a list')

        print 'saving tweets'
        self.proxy.save(tweets)
        return 0

    @_checker
    def burp(self):
        print 'burping', self.a
        self.proxy.burp()

    def close(self):
        self.proxy.kill()
        print 'closing ', self.a

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
