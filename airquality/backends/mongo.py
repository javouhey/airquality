# -*- coding: utf-8 -*-
from pymongo import (MongoClient, DESCENDING,)
from pymongo.errors import DuplicateKeyError
from collections import Mapping
from . import Reading
import logging


class MongoQueryMixin(object):

    @classmethod
    def find(cls, collection, criteria={}, limit=2, reading=Reading.hourly,
             sort_order=DESCENDING):
        """
        :param reading: default to hourly
        :param sort_order: ordering by the default `ObjectId` in mongo.
        """
        if not isinstance(criteria, Mapping):
            raise ValueError('expecting a dictionary')
        result = collection.find(criteria)\
                           .sort('_id', sort_order)\
                           .limit(limit)
        return result

    @classmethod
    def save(cls, collection, reading):
        """Inserts into mongo with pollution time-series data.
           If documents with reading_ids exist, then nothing is written.

        :param collection: The mongodb collection
        :param reading: must have a `reading_id` key
        :type reading: a dict
        :raises: `ValueError` if `reading` is missing or invalid type
        """
        if not isinstance(reading, Mapping):
            raise ValueError('expecting a dictionary')
        if 'reading_id' not in reading:
            raise ValueError('expecting a key called reading_id')

        try:
            return 1, collection.insert(reading)
        except DuplicateKeyError:
            logging.exception('WARNING: duplicate found during save')
            m = collection.find_one({'reading_id': reading['reading_id'],
                                     'source.type': reading['source']['type']})
            if m is None:
                raise RuntimeError('Was it deleted midway?')

            return 0, m[u'_id']


class MongoRepository(MongoQueryMixin):
    kwargs = {'fsync': True}

    def __init__(self, url='mongodb://localhost:27017/', database='test'):
        self.client = MongoClient(host=url, max_pool_size=12, **self.kwargs)
        self.db = getattr(self.client, database)
        logging.debug('open MongoRepository')
        #print self.client.max_pool_size, self.client.write_concern

    def close(self):
        self.client.close()
        self.db = None
        logging.debug('closed MongoRepository')

    def get_collection(self, collection_name):
        if collection_name:
            return getattr(self.db, collection_name)
        else:
            raise ValueError('collection_name missing')
