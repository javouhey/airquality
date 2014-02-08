# -*- coding: utf-8 -*-
from pymongo import (MongoClient, DESCENDING,)
from collections import Mapping
from enum import Enum


class Reading(Enum):
    hourly = u'hourly'
    instant = u'instant'
    avg24h = u'24havg'
    avg12h = u'12havg'


class MongoQueryMixin(object):

    @classmethod
    def find_one(cls, collection, limit=2, reading=Reading.hourly):
        print reading
        return collection.find().sort('_id', DESCENDING).limit(limit)

    @classmethod
    def save(cls, collection, reading):
        """Inserts into mongo with pollution time-series data.
           If documents with the tweet ids exist, then nothing is written.

        :param collection: The mongodb collection
        :param reading: must have a `reading_id` key
        :type reading: a dict
        :raises: `ValueError` if `reading` is missing or invalid type
        """
        if not isinstance(reading, Mapping):
            raise ValueError('expecting a dictionary')

        amatch = collection.find_one({'reading_id': reading['reading_id']})
        if amatch:
            return 0, None
        else:
            return 1, collection.insert(reading)


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
