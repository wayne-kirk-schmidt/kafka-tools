#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=no-name-in-module

"""
Explanation:

    This script is a wrapper for a kafka consumer.
    Other scripts will be built to consume messages, and manage messages.

Usage:
    $ python  kafka_reader [ options ]

Style:
    Google Python Style Guide:
    http://google.github.io/styleguide/pyguide.html

    @name           kafka_reader
    @version        1.0.0
    @author-name    Wayne Schmidt
    @author-email   wayne.kirk.schmidt@changeis.co.jp
    @license-name   Apache
    @license-url    https://www.apache.org/licenses/LICENSE-2.0
"""

__version__ = '1.0.0'
__author__ = "Wayne Schmidt (wayne.kirk.schmidt@changeis.co.jp)"

import sys
import json
import datetime
from kafka import KafkaConsumer

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
    enable_auto_commit=False,
    consumer_timeout_ms=1000,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print(f"{message.topic}:{message.partition}:{message.offset}: \
            key={message.key} value={message.value}")
