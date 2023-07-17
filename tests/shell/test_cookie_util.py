#!/usr/bin/env impala-python
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function

import sys
import unittest

from datetime import datetime, timedelta
from http.client import HTTPMessage

from shell.cookie_util import (cookie_matches_path, get_cookie_expiry,
                               get_all_matching_cookies)


class TestCookieUtil(unittest.TestCase):
    def test_cookie_matches_path(self):
        assert cookie_matches_path({}, '/')
        assert cookie_matches_path({}, '')
        assert cookie_matches_path({}, 'cliservice')
        assert cookie_matches_path({}, '/cliservice')
        assert cookie_matches_path({}, '/cliservice/')

        assert cookie_matches_path({'path': ''}, '/')
        assert cookie_matches_path({'path': '/'}, '')
        assert cookie_matches_path({'path': '/'}, 'clisevice')
        assert cookie_matches_path({'path': '/'}, '/clisevice')

        assert not cookie_matches_path({'path': '/cliservice'}, '')
        assert not cookie_matches_path({'path': '/cliservice'}, '/')
        assert cookie_matches_path({'path': '/cliservice'}, 'cliservice')
        assert cookie_matches_path({'path': 'cliservice'}, '/cliservice')
        assert cookie_matches_path({'path': '/cliservice/'}, '/cliservice/')
        assert cookie_matches_path({'path': '/cliservice'}, 'cliservice/abc/def/ghi')
        assert cookie_matches_path({'path': '/cliservice/'}, 'cliservice/?q=abcd')

        assert not cookie_matches_path({'path': '/a/b/c//'}, '/a')
        assert not cookie_matches_path({'path': '/a/b/c//'}, '/a/b')
        assert cookie_matches_path({'path': '/a/b/c//'}, 'a/b/c')
        assert cookie_matches_path({'path': '/a/b/c//'}, '/a/b/c')
        assert cookie_matches_path({'path': '/a/b/c//'}, 'a/b/c/d')
        assert cookie_matches_path({'path': '/a/b/c//'}, '/a/b/c/d')
        assert cookie_matches_path({'path': '/a/b/c//'}, '/a/b/c/d/e/f/g/h')

    def test_get_cookie_expiry(self):
        assert get_cookie_expiry({}) is None
        assert get_cookie_expiry({'max-age': ''}) is None

        hour = timedelta(hours=1)
        sec = timedelta(seconds=1)
        now = datetime.now()
        assert now <= get_cookie_expiry({'max-age': '0'}) <= now + sec
        now = datetime.now()
        assert now - sec <= get_cookie_expiry({'max-age': '-1'}) <= now

        now = datetime.now()
        assert now - hour <= get_cookie_expiry({'max-age': '-3600'}) <= now - hour + sec
        now = datetime.now()
        assert now + hour <= get_cookie_expiry({'max-age': '3600'}) <= now + hour + sec

        now = datetime.now()
        days2k = timedelta(days=2000)
        assert now - days2k <= \
               get_cookie_expiry({'max-age': '-172800000'}) <= \
               now - days2k + sec
        now = datetime.now()
        assert now + days2k <= \
               get_cookie_expiry({'max-age': '172800000'}) <= \
               now + days2k + sec

    def test_get_all_matching_cookies(self):
        cookies = get_all_matching_cookies(['a', 'b'], '/path', {})
        assert not cookies

        headers = make_cookie_headers([
            ('c_cookie', 'c_value'),
            ('b_cookie', 'b_value'),
            ('a_cookie', 'a_value')
        ])
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path', headers)
        assert len(cookies) == 2
        assert cookies[0].key == 'a_cookie' and cookies[0].value == 'a_value'
        assert cookies[1].key == 'b_cookie' and cookies[1].value == 'b_value'
        cookies = get_all_matching_cookies(['b_cookie', 'a_cookie'], '/path', headers)
        assert len(cookies) == 2
        assert cookies[0].key == 'b_cookie' and cookies[0].value == 'b_value'
        assert cookies[1].key == 'a_cookie' and cookies[1].value == 'a_value'

        headers = make_cookie_headers([
            ('c_cookie', 'c_value;Path=/'),
            ('b_cookie', 'b_value;Path=/path'),
            ('a_cookie', 'a_value;Path=/')
        ])
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path', headers)
        assert len(cookies) == 2
        assert cookies[0].key == 'a_cookie' and cookies[0].value == 'a_value'
        assert cookies[1].key == 'b_cookie' and cookies[1].value == 'b_value'

        headers = make_cookie_headers([
            ('c_cookie', 'c_value;Path=/'),
            ('b_cookie', 'b_value;Path=/path'),
            ('a_cookie', 'a_value;Path=/path/path2')
        ])
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path', headers)
        assert len(cookies) == 1
        assert cookies[0].key == 'b_cookie' and cookies[0].value == 'b_value'
        cookies = get_all_matching_cookies(['b_cookie', 'a_cookie'], '/path', headers)
        assert len(cookies) == 1
        assert cookies[0].key == 'b_cookie' and cookies[0].value == 'b_value'

        headers = make_cookie_headers([
            ('c_cookie', 'c_value;Path=/'),
            ('b_cookie', 'b_value;Path=/path'),
            ('a_cookie', 'a_value;Path=/path')
        ])
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path/path1',
            headers)
        assert len(cookies) == 2
        assert cookies[0].key == 'a_cookie' and cookies[0].value == 'a_value'
        assert cookies[1].key == 'b_cookie' and cookies[1].value == 'b_value'

        headers = make_cookie_headers([
            ('c_cookie', 'c_value;Path=/'),
            ('b_cookie', 'b_value;Path=/path1'),
            ('a_cookie', 'a_value;Path=/path2')
        ])
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path', headers)
        assert not cookies

        headers = make_cookie_headers([
            ('c_cookie', 'c_value;Path=/'),
            ('b_cookie', 'b_value;Path=/path1'),
            ('a_cookie', 'a_value;Path=/path2')
        ])
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie', 'c_cookie'], '/path',
            headers)
        assert len(cookies) == 1
        assert cookies[0].key == 'c_cookie' and cookies[0].value == 'c_value'


def make_cookie_headers(cookie_vals):
    """Make an HTTPMessage containing Set-Cookie headers for Python 2 or Python 3"""
    if sys.version_info.major == 2:
        # In Python 2 the HTTPMessage is a mimetools.Message object, and the
        # Set-Cookie values all appear in a single header, separated by newlines.
        cookies = ""
        count = 0
        for pair in cookie_vals:
            name = pair[0]
            value = pair[1]
            cookies += name + '=' + value
            if count + 1 < len(cookie_vals):
                # Separate the cookies, unless it is the last cookie.
                cookies += '\n '
            count += 1
        return {'Set-Cookie': cookies}
    else:
        # In Python 3 the HTTPMessage is an email.message.Message, and the
        # Set-Cookie values appear as duplicate headers.
        headers = HTTPMessage()
        for pair in cookie_vals:
            name = pair[0]
            value = pair[1]
            headers.add_header('Set-Cookie', name + "=" + value)
        return headers
