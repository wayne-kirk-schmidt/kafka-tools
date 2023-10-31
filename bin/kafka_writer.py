#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=no-name-in-module

"""
Explanation:

    This script is a wrapper for a kafka producer.
    Other scripts will be built to consume messages, and manage messages.

Usage:
    $ python  kafka_writer [ options ]

Style:
    Google Python Style Guide:
    http://google.github.io/styleguide/pyguide.html

    @name           kafka_writer
    @version        1.0.0
    @author-name    Wayne Schmidt
    @author-email   wayne.kirk.schmidt@changeis.co.jp
    @license-name   Apache
    @license-url    https://www.apache.org/licenses/LICENSE-2.0
"""

__version__ = '1.0.0'
__author__ = "Wayne Schmidt (wayne.kirk.schmidt@changeis.co.jp)"

import sys
import datetime
import time
import json

from kafka import KafkaProducer

def get_timestamp():
    """
    This is a wrapper for datetime allowing to timestamp messages
    """
    right_now = datetime.datetime.now()
    return right_now.strftime('%Y-%m-%d %H:%M:%S')

producer = KafkaProducer(
                 bootstrap_servers=['localhost:9092'],
                 value_serializer=lambda x:
                 json.dumps(x).encode('utf-8')
             )

if len(sys.argv) > 1:
    TARGET_TOPIC = sys.argv[1]
else:
    TARGET_TOPIC = 'SampleTopic'

for mynumber in range(10):
    format_string = get_timestamp() + " -- " + str(mynumber)
    data = {'string' : format_string}
    print(format_string)
    producer.send(TARGET_TOPIC, value=data)
    time.sleep(.25)
