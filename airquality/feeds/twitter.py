# -*- coding: utf-8 -*-
from __future__ import absolute_import
from . import ReadingParser
from . import consts as c
from datetime import datetime
from copy import deepcopy

import logging
import twitter
import re
import os

"""Functionality to parse feeds from Twitter"""


class TwitterParser(ReadingParser):
    """Parses the tweet's `text` field.
    """

    date_fmt = u'%m-%d-%Y %H:%M'

    regex_nodata = re.compile(r"^no data$", re.IGNORECASE)
    regex_24h = re.compile(r"^(?P<pollutant>.*?)\s*24hr\s*avg$", re.IGNORECASE)
    regex_dtfrom = re.compile(r"^(?P<from>\d{2}-\d{2}-\d{4} \d{2}:\d{2})$")
    regex_dtfromto2 = re.compile(r"""^(?P<from>\d{2}-\d{2}-\d{4} \d{2}:\d{2})
                                     \s*to\s*
                                     (?P<to>\d{2}-\d{2}-\d{4} \d{2}:\d{2})$""",
                                 re.X)
    e = r"\s*to\s*(?P<to>\d{2}-\d{2}-\d{4} \d{2}:\d{2})$"
    regex_dtfromto = re.compile(r"^(?P<from>\d{2}-\d{2}-\d{4} \d{2}:\d{2})"+e)

    def parse(self, raw, **kwargs):
        """Parse a tweet's content.

        :param raw: The text of the tweet
        :type raw: :class:`unicode`
        :raises: TypeError, ValueError
        :returns: a dictionary
        """
        if raw is None:
            raise TypeError
        if type(raw) != unicode:
            raise ValueError(c.M_UNICODE)

        segments = raw.strip() \
                      .split(c.SEMICOLON)
        segments = [s.strip() for s in segments if s]
#        import pytest
#        pytest.set_trace()
        retval = {}
        processors = [self._parse_nodata, self._parse_24havg,
                      self._parse_onehour]
        for processor in processors:
            # TODO: catch ValueError and log it
            ok, adict = processor(segments)
            if ok:
                self._attach(retval, adict, [u'type', u'data'])
                break

        if not retval:
            raise ValueError(u'unrecognized')

        return retval

    def _attach(self, retval, adict, keylist):
        for key in keylist:
            if key in adict:
                retval[key] = deepcopy(adict[key])

    def _parse_24havg(self, segments, timezone=None):
        if len(segments) != 5 \
           or self.regex_24h.match(segments[1]) is None \
           or self.regex_dtfromto.match(segments[0]) is None:
            return (False, {})

        r = self.regex_dtfromto.search(segments[0])
        dfrom, dto = r.groups()

        r = self.regex_24h.match(segments[1])
        pollutant = r.groups()[0]

        keytype = {c.K_NATURE: c.V_24HAVG}
        keytype.update(
            {c.K_HOURFR: datetime.strptime(dfrom, self.date_fmt),
             c.K_HOURTO: datetime.strptime(dto, self.date_fmt)}
        )
        if timezone:
            keytype.update({c.K_TZ: timezone})
        keydata = {u'pollutant': pollutant, u'missing': False}
        keydata.update(
            {u'index': tuple([int(segments[3]), c.V_AQI]),
             u'concentration': float(segments[2]),
             u'display': {
                 u'en': segments[4],
                 u'fr': u''
             }}
        )
        return True, {c.K_TYPE: keytype, c.K_DATA: keydata}

    def _parse_onehour(self, segments, timezone=None):
        if len(segments) != 5 \
           or self.regex_dtfrom.match(segments[0]) is None:
            return (False, {})

        keytype = {c.K_NATURE: c.V_HOURLY, c.K_HOURTO: None}
        keytype.update(
            {c.K_HOURFR: datetime.strptime(segments[0], self.date_fmt)})
        if timezone:
            keytype.update({c.K_TZ: timezone})
        keydata = {u'pollutant': segments[1], u'missing': False}
        keydata.update(
            {u'index': tuple([int(segments[3]), c.V_AQI]),
             u'concentration': float(segments[2]),
             u'display': {
                 u'en': segments[4],
                 u'fr': u''
             }}
        )
        return True, {c.K_TYPE: keytype, c.K_DATA: keydata}

    def _parse_nodata(self, segments, timezone=None):
        """Expects len(segments) to be 3 and item 2 is 'No Data'

        :param segments: the bits that make up the content
        :type segments: `list`
        :returns: a tuple (Boolean, result)

        .. Examples of result::

           (True, {
             u'type': {
               u'nature': u'hourly',
               u'hour_from': datetime.datetime(2014, 1, 23, 16, 0),
               u'hour_to': None,
               u'timezone': u'+0800'
             },
             u'data': {
               u'pollutant': u'PM2.5',
               u'concentration': float('-0.1'),
               u'index': tuple(),
               u'missing': True,
               u'display': {
                 u'en': u'No Data',
                 u'fr': u'Pas de donn\u00E9es'
               }
             }
           })

           (False, {})
        """
        if len(segments) != 3 \
           or self.regex_nodata.match(segments[2]) is None \
           or self.regex_dtfrom.match(segments[0]) is None:
            return (False, {})

        keytype = {c.K_NATURE: c.V_HOURLY, c.K_HOURTO: None}
        keytype.update(
            {c.K_HOURFR: datetime.strptime(segments[0], self.date_fmt)}
        )
        if timezone:
            keytype.update({c.K_TZ: timezone})
        keydata = {u'pollutant': segments[1],
                   u'concentration': c.V_BOGUS_CONCENTRATION}
        keydata.update(
            {u'index': tuple(),
             u'missing': True,
             u'display': {
                 u'en': segments[2],
                 u'fr': u'Pas de donn\u00E9es'
             }}
        )

        return True, {c.K_TYPE: keytype, c.K_DATA: keydata}


class TwitterService(object):
    """Connects to twitter and pull down pollution tweets.

    .. Collaborators:
       * `TwitterParser
       * environment variables
    """
    def __init__(self, twitter_list='pollution', twitter_user='GavinAtTiger'):
        """
        :raises: `ValueError` if these enviroment variables are not set
            * `CONSUMER_KEY`
            * `CONSUMER_SECRET`
            * `OAUTH_TOKEN`
            * `OAUTH_TOKEN_SECRET`
        """
        self.consumer_key = os.environ['CONSUMER_KEY']
        self.consumer_secret = os.environ['CONSUMER_SECRET']
        self.oauth_token = os.environ['OAUTH_TOKEN']
        self.oauth_token_secret = os.environ['OAUTH_TOKEN_SECRET']
        self.slug = twitter_list
        self.screenname = twitter_user
        self.pollution_parser = TwitterParser()

    def _log_ratelimit(self, reset=0, limit=0, remaining=0):
        """Logs our counts for twitter API rate limit

        .. See https://dev.twitter.com/docs/rate-limiting/1.1
        """
        format_string = 'Twitter rate limit: remaining={0}, ' + \
                        'reset={1}, limit={2}'
        logging.info(format_string.format(remaining, reset, limit))

    def get_latest_tweets(self, raw=False, limit=5):
        """Returns an array of tweets.

        :param raw: if `True` it returns the whole enchilada.
        :type raw: `bool`
        :returns: an array of dicts or a `TwitterResponse` if raw
        """
        credential = twitter.oauth.OAuth(self.oauth_token,
                                         self.oauth_token_secret,
                                         self.consumer_key,
                                         self.consumer_secret)
        api = twitter.Twitter(auth=credential)
        tweets = api.lists.statuses(slug=self.slug,
                                    owner_screen_name=self.screenname,
                                    count=limit)
        self._log_ratelimit(reset=tweets.rate_limit_reset,
                            limit=tweets.rate_limit_limit,
                            remaining=tweets.rate_limit_remaining)

        if raw:
            return tweets
        else:
            retval = []
            for tweet in tweets:
                if tweet.get('text'):
                    new_tweet = self.pollution_parser.parse(tweet.get('text'))
                    new_tweet.update({u'raw': tweet.get('text')})
                    new_tweet.update({u'reading_id': tweet.get('id')})
                    new_tweet.update({u'source': self._get_source(tweet)})
                    retval.append(new_tweet)

            tweets = retval

        return tweets

    def _get_source(self, tweet):
        """Construct the identity of the feed responsible for this tweet

        :returns: a dictionary that looks like this

           {'type': 'twitter', 'screen_name' : 'shanghai',
            'display_name': 'Shanghai guy', 'userid': 12617626715L}
        """
        retval = {u'type': u'twitter',
                  u'user_id': tweet[u'user'][u'id'],
                  u'display_name': tweet[u'user'][u'name'],
                  u'screen_name': tweet[u'user'][u'screen_name']}
        return retval
