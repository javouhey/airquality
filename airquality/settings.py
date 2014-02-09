# -*- coding: utf-8 -*-

# or 'postgresql'
STORAGE_ENGINE = ('mongo', 'airquality.backends.mongo.MongoRepository')
MONGO_COLLECTION = 'readings'
MONGO_DATABASE = 'test'
MONGO_URL = 'mongodb://localhost:44444/'
LOG_FILE = '/tmp/aqi/airquality.log'
