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

import unittest

from datetime import datetime, timedelta
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

        headers = {'Set-Cookie': '''c_cookie=c_value
        b_cookie=b_value
        a_cookie=a_value'''}
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path', headers)
        assert len(cookies) == 2
        assert cookies[0].key == 'a_cookie' and cookies[0].value == 'a_value'
        assert cookies[1].key == 'b_cookie' and cookies[1].value == 'b_value'
        cookies = get_all_matching_cookies(['b_cookie', 'a_cookie'], '/path', headers)
        assert len(cookies) == 2
        assert cookies[0].key == 'b_cookie' and cookies[0].value == 'b_value'
        assert cookies[1].key == 'a_cookie' and cookies[1].value == 'a_value'

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path
        a_cookie=a_value;Path=/'''}
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path', headers)
        assert len(cookies) == 2
        assert cookies[0].key == 'a_cookie' and cookies[0].value == 'a_value'
        assert cookies[1].key == 'b_cookie' and cookies[1].value == 'b_value'

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path
        a_cookie=a_value;Path=/path/path2'''}
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path', headers)
        assert len(cookies) == 1
        assert cookies[0].key == 'b_cookie' and cookies[0].value == 'b_value'
        cookies = get_all_matching_cookies(['b_cookie', 'a_cookie'], '/path', headers)
        assert len(cookies) == 1
        assert cookies[0].key == 'b_cookie' and cookies[0].value == 'b_value'

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path
        a_cookie=a_value;Path=/path'''}
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path/path1',
            headers)
        assert len(cookies) == 2
        assert cookies[0].key == 'a_cookie' and cookies[0].value == 'a_value'
        assert cookies[1].key == 'b_cookie' and cookies[1].value == 'b_value'

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path1
        a_cookie=a_value;Path=/path2'''}
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie'], '/path', headers)
        assert not cookies

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path1
        a_cookie=a_value;Path=/path2'''}
        cookies = get_all_matching_cookies(['a_cookie', 'b_cookie', 'c_cookie'], '/path',
            headers)
        assert len(cookies) == 1
        assert cookies[0].key == 'c_cookie' and cookies[0].value == 'c_value'
