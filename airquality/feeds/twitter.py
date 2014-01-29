from . import ReadingParser
import re
from datetime import datetime
from copy import deepcopy


class TwitterParser(ReadingParser):

    SEMICOLON = u';'
    date_fmt = u'%m-%d-%Y %H:%M'

    regex_nodata = re.compile(r"^no data$", re.IGNORECASE)
    regex_dtfrom = re.compile(r"^(?P<from>\d{2}-\d{2}-\d{4} \d{2}:\d{2})$")
    regex_dtfromto = re.compile(r"""
    ^(?P<from>\d{2}-\d{2}-\d{4} \d{2}:\d{2})  # starting datetime
    \s* to \s*                                # lose the 'to' & whitespaces
    ^(?P<to>\d{2}-\d{2}-\d{4} \d{2}:\d{2})$   # end datetime
    """, re.VERBOSE)

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
            raise ValueError('raw must be in unicode')

        segments = raw.strip() \
                      .split(TwitterParser.SEMICOLON)
        segments = [s.strip() for s in segments if s]

        retval = {}
        processors = [self._parse_nodata]
        for processor in processors:
            ok, adict = processor(segments)
            print adict
            if ok:
                self._attach(retval, adict, [u'type', u'data'])

        if not retval:
            raise ValueError('unrecognized')

        return retval

    def _attach(self, retval, adict, keylist):
        for key in keylist:
            if key in adict:
                retval[key] = deepcopy(adict[key])

    def _parse_nodata(self, segments, timezone=None):
        """Expects len(segments) is 3 and item 2 is 'No Data'

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
               u'concentration': {}
               u'index': {}
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

        keytype = {u'nature': u'hourly', u'hour_to': None}
        keytype.update(
            {u'hour_from': datetime.strptime(segments[0], self.date_fmt)}
        )
        keydata = {u'pollutant': segments[1], u'concentration': {}}
        keydata.update(
            {u'index': {},
             u'display': {
                 u'en': segments[2],
                 u'fr': u'Pas de donn\u00E9es'
             }}
        )

        return (True, {u'type': keytype, u'data': keydata})
