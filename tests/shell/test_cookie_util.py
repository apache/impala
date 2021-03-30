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
                               get_first_matching_cookie)


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

    def test_get_first_matching_cookie(self):
        assert get_first_matching_cookie(['a', 'b'], '/path', {}) is None

        headers = {'Set-Cookie': '''c_cookie=c_value
        b_cookie=b_value
        a_cookie=a_value'''}
        c = get_first_matching_cookie(['a_cookie', 'b_cookie'], '/path', headers)
        assert c.key == 'a_cookie' and c.value == 'a_value'
        c = get_first_matching_cookie(['b_cookie', 'a_cookie'], '/path', headers)
        assert c.key == 'b_cookie' and c.value == 'b_value'

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path
        a_cookie=a_value;Path=/'''}
        c = get_first_matching_cookie(['a_cookie', 'b_cookie'], '/path', headers)
        assert c.key == 'a_cookie' and c.value == 'a_value'
        c = get_first_matching_cookie(['b_cookie', 'a_cookie'], '/path', headers)
        assert c.key == 'b_cookie' and c.value == 'b_value'

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path
        a_cookie=a_value;Path=/path/path2'''}
        c = get_first_matching_cookie(['a_cookie', 'b_cookie'], '/path', headers)
        assert c.key == 'b_cookie' and c.value == 'b_value'
        c = get_first_matching_cookie(['b_cookie', 'a_cookie'], '/path', headers)
        assert c.key == 'b_cookie' and c.value == 'b_value'

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path
        a_cookie=a_value;Path=/path'''}
        c = get_first_matching_cookie(['a_cookie', 'b_cookie'], '/path/path1', headers)
        assert c.key == 'a_cookie' and c.value == 'a_value'
        c = get_first_matching_cookie(['b_cookie', 'a_cookie'], '/path/path1', headers)
        assert c.key == 'b_cookie' and c.value == 'b_value'

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path1
        a_cookie=a_value;Path=/path2'''}
        c = get_first_matching_cookie(['a_cookie', 'b_cookie'], '/path', headers)
        assert c is None

        headers = {'Set-Cookie': '''c_cookie=c_value;Path=/
        b_cookie=b_value;Path=/path1
        a_cookie=a_value;Path=/path2'''}
        c = get_first_matching_cookie(['a_cookie', 'b_cookie', 'c_cookie'], '/path',
                                      headers)
        assert c.key == 'c_cookie' and c.value == 'c_value'
