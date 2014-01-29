import pytest
from pytest import raises
parametrize = pytest.mark.parametrize


class TestTwitterParser(object):

    @parametrize("input, expected", [
        ("3+5", 8),
        ("2+4", 6),
        pytest.mark.xfail(("6*9", 42)),
    ])
    def test_eval(self, input, expected):
        """copied from pytest samples"""
        assert eval(input) == expected

    ERR_VALUEERROR = 'ValueError: raw must be in unicode'

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
        assert result[u'type'][u'hour_from'] == expects['dt']
        assert result[u'type'][u'nature'] == expects['nat']
        assert result[u'data'][u'pollutant'] == expects['pol']
        assert result[u'data'][u'display'][u'en'] == expects['en']

    def test_parsing_good_nodata(self, nodata, twitter):
        """See module conftest"""
        assert twitter is not None
        for item in nodata:
            res = twitter.parse(item[0])
            assert 'data' in res
            assert 'type' in res
            self._assert_nodata(res, item[1])
