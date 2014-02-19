import os
import time
import atexit

from collections import namedtuple
from apscheduler.scheduler import Scheduler

LINKED_KEY = 'mongolink'  # docker's link name
LINKED_VOLUME = '/mongowork'


def get_metadata():
    """Read from environment variables

    ... example:
          FOO_PORT=tcp://172.17.0.2:44444
    """
    key = '{0}_PORT'.format(LINKED_KEY.upper())
    if key not in os.environ:
        raise RuntimeError('Expect environ variable {0}'.format(key))

    temp = os.environ[key].split('tcp://')
    return (temp[1], LINKED_VOLUME)


def get_settings(func, db='airquality'):
    Settings = namedtuple('Settings', ['STORAGE_ENGINE', 'MONGO_URL',
                          'MONGO_COLLECTION', 'MONGO_DATABASE', 'LOG_FILE'])

    metadata = func()
    MONGO_URL = 'mongodb://{0}/'.format(metadata[0])
    LOG_FILE = '{0}/airquality.log'.format(metadata[1])

    return Settings(('mongo', 'airquality.backends.mongo.MongoRepository'),
                    MONGO_URL, 'readings', db, LOG_FILE)


sched = Scheduler(daemon=False)
sched.start()


@sched.interval_schedule(seconds=15)
def poll():
    from airquality.feeds.twitter import TwitterService
    from airquality import backends
    from contextlib import closing

    # --on K46
    #hardcoded = lambda: ('127.0.0.1:44444', '/tmp')
    #settings = get_settings(hardcoded, db='test')

    # --production
    settings = get_settings(get_metadata)
    print "\tPolling twitter .."

    service = TwitterService(twitter_user='dvillacouple')
    tweets = service.get_latest_tweets(limit=6)

    with closing(backends.Repository(settings)) as repo:
        repo.save(tweets)
    print "done\n"


def goodbye(mesg):
    print 'dying {0}'.format(mesg)

atexit.register(goodbye, mesg=' my friend')


try:
    while(True):
        print '.\n'
        time.sleep(30)
except KeyboardInterrupt:
    sched.shutdown(wait=False)
    print 'caught CTRL-C'
