# -*- coding: utf-8 -*-
import pytest
import mongomock
from airquality.feeds.twitter import TwitterParser
from airquality.feeds import consts as c
from airquality.backends.mongo import MongoRepository
from mock import Mock


@pytest.fixture(scope="module")
def nodata(request):
    """No Data from Shanghai"""
    from datetime import datetime
    expect = {u'dt': datetime(2014, 1, 23, 15, 0)}
    expect.update(
        {u'pol': u'PM2.5', u'nat': u'hourly', u'en': 'No Data'}
    )
    data = [(u' 01-23-2014 15:00;  PM2.5;  No Data ', expect),
            (u'01-23-2014 15:00; PM2.5; No Data', expect),
            (u'01-23-2014 15:00; PM2.5; No Data;;', expect)]
    return data


@pytest.fixture(scope="module")
def nodata_bad(request):
    """Bad No Data"""
    data = [(u' 01-23-2014 15h00;  PM2.5;  No Data ',),
            (u'01-23-2014 15:00; PM2.5; rubbish;;',)]
    return data


@pytest.fixture(scope="module")
def average24hour(request):
    from datetime import datetime
    expect = {c.K_HOURFR: datetime(2014, 1, 20, 0, 0)}
    expect.update({c.K_HOURTO: datetime(2014, 1, 20, 23, 59)})
    expect.update(
        {u'pol': u'PM2.5', u'nat': c.V_24HAVG, u'en': 'Very Unhealthy',
         u'aqi': 217, u'concentration': 166.7})

    data = [(u' 01-20-2014 00:00 to {0} ; {1}; {2}; {3}; {4}'.format(
            '01-20-2014 23:59', u'PM2.5 24hr avg', u'166.7',
            u'217', u'Very Unhealthy'), expect)]
    return data


@pytest.fixture(scope="module")
def onehour(request):
    from datetime import datetime
    expect = {c.K_HOURFR: datetime(2014, 1, 23, 16, 0)}
    expect.update(
        {u'pol': u'PM2.5', u'nat': c.V_HOURLY,
         u'en': u'Moderate (at 24-hour exposure at this level)',
         u'aqi': 99, u'concentration': 35.0})

    data = [(u' 01-23-2014 16:00 ; {0}; {1}; {2}; {3}'.format(
            u'PM2.5', u'35.0', u'99',
            u'Moderate (at 24-hour exposure at this level)'), expect)]
    return data


@pytest.fixture(scope="module")
def twitter(request):
    parser = TwitterParser()

    def fin():
        print("teardown twitter")
    request.addfinalizer(fin)
    return parser


@pytest.fixture(scope="module")
def apikeys(request):
    adict = {'CONSUMER_KEY': 'consumerkey',
             'CONSUMER_SECRET': 'consumersecret',
             'OAUTH_TOKEN': '12345token',
             'OAUTH_TOKEN_SECRET': '8798797secret'}
    return adict


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    """
    The following doesn't work because it uses __call__ internally
      monkeypatch.delattr("twitter.Twitter.lists")
    """
    #monkeypatch.delattr('urllib2.urlopen')
    pass


class MockResponse(object):
    def __init__(self, o):
        self.result = o
        self.headers = {'Content-Type': 'application/json',
                        'Content-Encoding': '',
                        'X-Rate-Limit-Limit': "20",
                        'X-Rate-Limit-Reset': "4"}

    def read(self):
        import json
        return json.dumps(self.result)

    def info(self):
        return self.headers


def urlopen(req, **kwargs):
    import raw_twitter
    return MockResponse([raw_twitter.f])


@pytest.fixture(scope="module")
def mockurlresponse(request):
    return urlopen


class FakeModuleBad(object):
    STORAGE_ENGINE = ('mong', 'airquality.backends.mongo.MongoRepository')


@pytest.fixture(scope="module")
def settings_bad_backend(request):
    return FakeModuleBad()


class FakeModuleOk(object):
    STORAGE_ENGINE = ('mongo', 'airquality.backends.mongo.MongoRepository')
    MONGO_COLLECTION = 'sochi'
    MONGO_DATABASE = 'poutine'
    MONGO_URL = 'foo_url'


@pytest.fixture(scope="module")
def settings_ok(request):
    return FakeModuleOk()


@pytest.fixture(scope="module")
def mocked_mongo_tuple(request):
    mock_mongo_repo = Mock(spec_set=MongoRepository)
    mock_mongo_repo.close.return_value = None

    mock_class = Mock()
    mock_class.return_value = mock_mongo_repo
    return mock_class, mock_mongo_repo


@pytest.fixture(scope="module")
def readings3(request):
    """:returns: a mongodb collection containing pollution documents"""
    collection = mongomock.Connection().db.collection
    objects = [dict(votes=1, reading_id=999L), dict(votes=2, reading_id=888L)]
    for obj in objects:
        obj['_id'] = collection.insert(obj)
    return collection, objects


@pytest.fixture(scope="module")
def readings(request):
    """:returns: a mongodb collection containing pollution documents"""
    import raw_mongo
    collection = mongomock.Connection().db.collection
    for obj in raw_mongo.m:
        obj['_id'] = collection.insert(obj)
    return collection, raw_mongo.m
