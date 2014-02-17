# -*- coding: utf-8 -*-
from airquality.backends import Reading
from airquality.backends import Repository
from airquality.backends.mongo import MongoQueryMixin
from pytest import raises


def test_repository1(settings_bad_backend):
    with raises(RuntimeError):
        Repository(settings_bad_backend)


def test_repository2(settings_ok, mocked_mongo_tuple, monkeypatch):
    mock_class, mock_mongo_repo = mocked_mongo_tuple

    returnval = lambda a, b: {settings_ok.STORAGE_ENGINE[0]: mock_class}
    monkeypatch.setattr(Repository, '_get_klazz', returnval)

    repo = Repository(settings_ok)
    repo.close()

    mock_mongo_repo.close.assert_called_once_with()


def test_save1(readings3):
    the_collection, objects = readings3

    # invalid document
    new_document = dict(foo=2, bar='hello')

    repo = MongoQueryMixin()
    with raises(ValueError):
        repo.save(the_collection, new_document)

    # good document
    RID = 1234567890L
    new_document2 = dict(reading_id=RID, raw='PM2.5 90 AQI',
                         source=dict(type=u'twitter',
                                     screen_name=u'Guangzhou_Air'))
    status0, objid0 = repo.save(the_collection, new_document2)
    assert status0 == 1
    assert objid0 is not None

    zero = 0

    # DESCENDING
    acursor = repo.find(the_collection, limit=4)
    o = [item for item in acursor]
    assert len(o) == 3
    assert o[zero]['reading_id'] == RID

    # ASCENDING
    from pymongo import ASCENDING
    acursor = repo.find(the_collection, limit=4, sort_order=ASCENDING)
    o = [item for item in acursor]
    assert len(o) == 3
    assert o[zero]['reading_id'] == 999L

    # unique keys test?
    status, objid = repo.save(the_collection, new_document2)
    assert status == 0
    assert objid == objid0


def test_query1(readings):
    MAX = 100
    zero = 0

    the_collection, objects = readings
    assert 16 == len(objects)

    repo = MongoQueryMixin()

    # all
    acursor = repo.find(the_collection, limit=MAX)
    o = [item for item in acursor]
    assert len(o) == 16

    # only those with nodata
    acursor = repo.find(the_collection, {'data.missing': True}, limit=MAX)
    o = [item for item in acursor]
    assert len(o) == 1
    match_obj = o[zero]
    assert match_obj['reading_id'] == 433031447578808320L
    assert match_obj[u'source'][u'screen_name'] == u'Guangzhou_Air'

    # only hourly readings
    criteria = {'data.missing': False, 'type.nature': Reading.hourly.value}
    acursor = repo.find(the_collection, criteria, limit=MAX)
    o = [item for item in acursor]
    assert len(o) == 14
    match_obj = o[zero]
    assert match_obj['reading_id'] == 432019781697273856L
    assert match_obj[u'source'][u'screen_name'] == u'Guangzhou_Air'

    # only hourly readings that HAS data for Shanghai
    criteria = {'data.missing': False,
                'type.nature': Reading.hourly.value,
                'source.screen_name': u'CGShanghaiAir'}
    acursor = repo.find(the_collection, criteria, limit=MAX)
    o = [item for item in acursor]
    assert len(o) == 3
    match_obj = o[zero]
    assert match_obj['reading_id'] == 432019782871699458L
    assert match_obj[u'source'][u'screen_name'] == u'CGShanghaiAir'

    # only 24h average readings that HAS data
    criteria = {'data.missing': False,
                'type.nature': Reading.avg24h.value}
    acursor = repo.find(the_collection, criteria, limit=MAX)
    o = [item for item in acursor]
    assert len(o) == 1
    match_obj = o[zero]
    assert match_obj['reading_id'] == 432004689362558976L
    assert match_obj[u'source'][u'screen_name'] == u'Shenyang_Air'


def test_query2(readings):
    """Testing temporal queries"""
    MAX = 100
    zero = 0

    the_collection, objects = readings
    assert 16 == len(objects)

    repo = MongoQueryMixin()
    from datetime import datetime
    start = datetime(2014, 2, 10)
    end = datetime(2014, 2, 11)
    criteria = {'data.missing': False,
                'type.hour_from': {"$gte": start, "$lt": end},
                'type.nature': Reading.hourly.value}
    acursor = repo.find(the_collection, criteria, limit=MAX)
    o = [item for item in acursor]
    assert len(o) == 6

    start = datetime(2014, 2, 11, 7, 0)
    criteria = {'type.hour_from': {"$in": [start]},
                'type.nature': Reading.hourly.value}
    acursor = repo.find(the_collection, criteria, limit=MAX)
    o = [item for item in acursor]
    assert len(o) == 1
    match_obj = o[zero]
    assert match_obj['reading_id'] == 433016360931573760L
    assert match_obj[u'source'][u'screen_name'] == u'Shenyang_Air'
