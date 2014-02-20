=========================
 air quality
=========================

.. image:: https://travis-ci.org/javouhey/airquality.png
   :target: https://travis-ci.org/javouhey/airquality

This project scraps live pollution readings for cities and stores them in mongodb.

Data sources
============

Pollution readings are currently:

* `@BeijingAir <https://twitter.com/beijingair/>`_
* `British Columbia government air quality readings <http://www.bcairquality.ca/readings/>`_

Data Model
==========

* A reading scraped from Twitter is saved as follows (structure is not final)::

    {u'data': {u'concentration': 84.0,
               u'display': {u'en': u'Unhealthy (at 24-hour exposure at this level)', 
                            u'fr': u''},
               u'index': [166, u'AQI'],
               u'missing': False,
               u'pollutant': u'PM2.5'},
     u'raw': u'02-11-2014 08:00; PM2.5; 84.0; 166; Unhealthy (at 24-hour exposure at this level)',
     u'reading_id': 433031445540372480L,
     u'source': {u'display_name': u'BeijingAir',
                 u'screen_name': u'BeijingAir',
                 u'type': u'twitter',
                 u'user_id': 15527964},
     u'type': {u'hour_from': datetime.datetime(2014, 2, 11, 8, 0),
               u'hour_to': None,
               u'nature': u'hourly'}}

Project Setup
=============

* During deployment, you need to set several environment variables:

  * `CONSUMER_KEY`
  * `CONSUMER_SECRET`
  * `OAUTH_TOKEN`
  * `OAUTH_TOKEN_SECRET`

* This project is deployed using docker. See project `airquality-docker <http://https://github.com/javouhey/airquality-docker/>`_ for detals.

Project todos
=============

* Change code in `setup.py` back to original checking for git projects.

Authors
=======

* Gavin Bong
