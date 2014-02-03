# -*- coding: utf-8 -*-
from pymongo import (MongoClient, DESCENDING,)


class MongoQueryMixin(object):

    @classmethod
    def find_one(cls, collection):
        return collection.find().sort('_id', DESCENDING)

    @classmethod
    def save(cls, collection, tweet):
        """Inserts into mongo with pollution time-series data.
           If documents with the tweet ids exist, then nothing is written.

        :param collection: The mongodb collection
        """
        return collection.insert(tweet)


class MongoRepository(MongoQueryMixin):
    kwargs = {'fsync': True}

    def __init__(self, url='mongodb://localhost:27017/', database='test'):
        self.client = MongoClient(host=url, max_pool_size=12, **self.kwargs)
        self.db = getattr(self.client, database)
        #print self.client.max_pool_size, self.client.write_concern

    def close(self):
        self.client.close()
        self.db = None

    def get_collection(self, collection_name):
        if collection_name:
            return getattr(self.db, collection_name)
        else:
            raise ValueError('collection_name missing')
