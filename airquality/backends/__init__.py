# -*- coding: utf-8 -*-
"""
Persistence mechanisms
"""
import traceback
from types import ListType
from functools import wraps

__author__ = 'Gavin Bong'


class MongoProxy(object):
    """Proxy to MongoRepository"""
    def __init__(self, **kwargs):
        pass

    def is_alive(self):
        return False

    def kill(self):
        pass


class Repository(object):

    MONGO = 'mongo'
    POSTGRES = 'postgresql'

    proxies = {
        MONGO: MongoProxy
    }

    def __init__(self, engine_tuple, **kwargs):
        self.a = 1
        self.engine_meta = {}  # put backend specific configs here
        try:
            # TODO pull collection name from kwargs
            self.engine = engine_tuple[0]
            self.storage_klasses = self._get_klazz(engine_tuple)
            self.persister = self.storage_klasses[self.engine]()
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
        if self.engine == self.MONGO:
            c = self.persister.get_collection('gavin')
            for tweet in tweets:
                self.persister.save(c, tweet)
        return 0

    @_checker
    def burp(self):
        print 'burping', self.a
        c = self.persister.get_collection('gavin')
        for j in self.persister.find_one(c):
            print j

    def close(self):
        self.persister.close()
        self._cleanup()
        print 'closing ', self.a

    def _cleanup(self):
        """Prevent it being re-awaken"""
        self.persister = None

    def _check_alive(self):
        if not self.persister:
            raise RuntimeError('disconnected')

    @classmethod
    def _get_klazz(cls, engine):
        segments = engine[1].split('.')
        modulepath = '.'.join(segments[:-1])
        m = __import__(modulepath)
        for segment in segments[1:]:
            m = getattr(m, segment)
        return {engine[0]: m}
