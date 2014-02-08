# -*- coding: utf-8 -*-
import pytest
import os
from airquality.feeds.twitter import TwitterService
from airquality.feeds import consts as c
from pytest import raises
parametrize = pytest.mark.parametrize


class TestTwitterParser(object):

    ERR_VALUEERROR = u'ValueError: %s' % c.M_UNICODE

    def test_parsing_argerror(self, twitter):
        with raises(TypeError):
            twitter.parse(None)
        with raises(ValueError) as exc_info:
            twitter.parse('not unicode')
        assert exc_info.exconly(tryshort=True) == self.ERR_VALUEERROR

    def test_parsing_bad_nodata(self, nodata_bad, twitter):
        """See module conftest"""
        assert twitter is not None
        for item in nodata_bad:
            with raises(ValueError):
                twitter.parse(item[0])

    def _assert_nodata(self, result, expects):
        """{u'pol': u'PM2.5', u'nat': u'hourly', u'en': 'No Data'}"""
        assert result[c.K_TYPE][c.K_HOURFR] == expects['dt']
        assert result[c.K_TYPE][c.K_NATURE] == expects['nat']
        assert result[c.K_DATA][u'pollutant'] == expects['pol']
        assert result[c.K_DATA][u'display'][u'en'] == expects['en']
        assert result[c.K_DATA][u'missing']
        assert result[c.K_DATA][u'concentration'] == c.V_BOGUS_CONCENTRATION
        assert result[c.K_DATA][u'index'] == tuple()

    def test_parsing_good_nodata(self, nodata, twitter):
        """See module conftest"""
        assert twitter is not None
        for item in nodata:
            res = twitter.parse(item[0])
            assert c.K_DATA in res
            assert c.K_TYPE in res
            self._assert_nodata(res, item[1])

    def _assert_24haverage(self, result, expects):
        """{u'pol': u'PM2.5', u'nat': c.V_24HAVG, u'en': 'Very Unhealthy',
         u'aqi': 217, u'concentration': 166.7})
        """
        assert result[c.K_TYPE][c.K_HOURFR] == expects[c.K_HOURFR]
        assert result[c.K_TYPE][c.K_HOURTO] == expects[c.K_HOURTO]
        assert result[c.K_TYPE][c.K_NATURE] == expects['nat']
        assert result[c.K_DATA][u'pollutant'] == expects['pol']
        assert result[c.K_DATA][u'concentration'] == \
            expects['concentration']
        assert result[c.K_DATA][u'index'][0] == \
            expects['aqi']
        assert result[c.K_DATA][u'display'][u'en'] == expects['en']
        assert not result[c.K_DATA][u'missing']

    def test_parsing_24h_average(self, average24hour, twitter):
        for item in average24hour:
            res = twitter.parse(item[0])
            assert c.K_DATA in res
            assert c.K_TYPE in res
            self._assert_24haverage(res, item[1])

    def _assert_onehour(self, result, expects):
        """{u'pol': u'PM2.5', u'nat': c.V_HOURLY, u'en': 'xxx...',
         u'aqi': 99, u'concentration': 35.0})
        """
        assert result[c.K_TYPE][c.K_HOURFR] == expects[c.K_HOURFR]
        assert result[c.K_TYPE][c.K_NATURE] == expects['nat']
        assert result[c.K_DATA][u'pollutant'] == expects['pol']
        assert result[c.K_DATA][u'concentration'] == \
            expects['concentration']
        assert result[c.K_DATA][u'index'][0] == \
            expects['aqi']
        assert result[c.K_DATA][u'display'][u'en'] == expects['en']
        assert not result[c.K_DATA][u'missing']

    def test_parsing_hour(self, onehour, twitter):
        for item in onehour:
            res = twitter.parse(item[0])
            assert c.K_DATA in res
            assert c.K_TYPE in res
            self._assert_onehour(res, item[1])

    @parametrize("input, expected", [
        ("3+5", 8),
        ("2+4", 6),
        pytest.mark.xfail(("6*9", 42)),
    ])
    def test_eval(self, input, expected):
        """copied from pytest samples"""
        assert eval(input) == expected


class TestTwitterService(object):

    def _set_environ(self, apikeys, monkeypatch):
        for k, v in apikeys.items():
            monkeypatch.setitem(os.environ, k, v)

    def test_environment_vars(self, apikeys, monkeypatch):
        self._set_environ(apikeys, monkeypatch)

        service = TwitterService()
        for k in apikeys.keys():
            lower_k = k.lower()
            assert apikeys[k] == getattr(service, lower_k)

    def test_defaultprops(self, apikeys, monkeypatch):
        self._set_environ(apikeys, monkeypatch)

        service1 = TwitterService()
        assert service1.slug == 'pollution'
        assert service1.screenname == 'GavinAtTiger'

        service2 = TwitterService(twitter_user='saucony', twitter_list='alist')
        assert service2.slug == 'alist'
        assert service2.screenname == 'saucony'

    def test_gettweets(self, apikeys, monkeypatch, mockurlresponse):
        self._set_environ(apikeys, monkeypatch)

        import urllib2
        monkeypatch.setattr(urllib2, 'urlopen', mockurlresponse)

        number_of_tweets = 1
        service = TwitterService()

        # parsed tweets
        result = service.get_latest_tweets(limit=number_of_tweets)
        assert type(result) == list
        assert len(result) == number_of_tweets
        assert type(result[0]) == dict
        assert result[0]['data']['index'] == (169, 'AQI')
        assert result[0]['data']['concentration'] == 90.0
        assert result[0]['reading_id'] == 431929188598042624L

        import datetime
        expected = datetime.datetime(2014, 2, 8, 7, 0)
        assert result[0]['type']['hour_from'] == expected

        # raw tweets
        result = service.get_latest_tweets(limit=number_of_tweets, raw=True)
        from twitter import TwitterResponse
        assert isinstance(result, TwitterResponse)
        assert isinstance(result, list)
        assert len(result) == number_of_tweets
        assert result.rate_limit_limit == 20
        assert result.rate_limit_reset == 4

        assert type(result[0]) == dict
        assert 'data' not in result[0]
        assert 'created_at' in result[0]
        assert result[0]['id'] == 431929188598042624L
