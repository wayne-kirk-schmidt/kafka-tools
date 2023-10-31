#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#pylint: disable=C0301

"""
Explanation:

    This checks to see how many messages and how many can be parsed

Usage:
    $ python3 syslog_parser [ file ]

Style:
    Google Python Style Guide:
    http://google.github.io/styleguide/pyguide.html

    @name           syslog_parser
    @version        1.0.0
    @author-name    Wayne Schmidt
    @author-email   wayne.kirk.schmidt@changeis.co.jp
    @license-name   Apache
    @license-url    https://www.apache.org/licenses/LICENSE-2.0
"""

__version__ = '1.0.0'
__author__ = "Wayne Schmidt (wayne.kirk.schmidt@changeis.co.jp)"

import sys
import pygrok

INPUT_FILE = sys.argv[1]

GROK_PATTERN = "Msg: Original Address=%{IP:original_address} %{SYSLOGTIMESTAMP:syslog_timestamp} %{GREEDYDATA:log_details}"

TOTAL_MESSAGES = 0
MESSAGES = {}

grok = pygrok.Grok(GROK_PATTERN)

with open(INPUT_FILE, "r", encoding="utf-8") as file_object:
    for file_line in file_object.readlines():
        sample_string = file_line.strip()
        message_size = len(sample_string)
        ### print("SYSLOG_LINE: {}".format(sample_string))
        parsed_data = grok.match(sample_string)
        if parsed_data:
            ### print(parsed_data)
            MESSAGES[message_size] = MESSAGES.get(message_size, 0) + 1

        else:
            MESSAGES['unparsed'] = MESSAGES.get('unparsed', 0) + 1
        ### print(" ------ ")
        TOTAL_MESSAGES = TOTAL_MESSAGES + 1

print(TOTAL_MESSAGES)
print(MESSAGES)
