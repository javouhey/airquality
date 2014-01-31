# -*- coding: utf-8 -*-
import pytest
from airquality.feeds.twitter import TwitterParser
from airquality.feeds import consts as c


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
def hourly_shanghai(request):
    """a"""
    return None
