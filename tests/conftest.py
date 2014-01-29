import pytest
from airquality.feeds.twitter import TwitterParser


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
