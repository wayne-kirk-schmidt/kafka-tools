#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Explanation:

    This script is a wrapper for a kafka consumer.
    Other scripts will be built to consume messages,
    and manage messages.

Usage:
    $ python  kafka-consumer-test [ options ]

Style:
    Google Python Style Guide:
    http://google.github.io/styleguide/pyguide.html

    @name           kafka-consumer-test
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
import kafka


def get_timestamp():
    """
    This is a wrapper for datetime allowing to timestamp messages
    """
    right_now = datetime.datetime.now()
    return right_now.strftime('%Y-%m-%d %H:%M:%S')

if len(sys.argv) > 1:
    TARGET_TOPIC = sys.argv[1]
else:
    TARGET_TOPIC = 'SampleTopic'

consumer = KafkaConsumer(
        TARGET_TOPIC,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
     )

for mymessage in consumer:
    message = message.value
    collection.insert_one(message)
    print('{} added to {}'.format(message, collection))
