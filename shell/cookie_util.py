#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import six
import datetime
import os.path
from six.moves import http_cookies


def cookie_matches_path(c, path):
  if 'path' not in c or not c['path']:
    return True
  cookie_path = c['path'].strip()
  if not cookie_path.startswith('/'):
    cookie_path = '/' + cookie_path
  cookie_path = os.path.normpath(cookie_path)
  if cookie_path == '/':
    return True
  if not path.startswith('/'):
    path = '/' + path
  path = os.path.normpath(path)
  return path == cookie_path or path.startswith(cookie_path + '/')


def get_cookie_expiry(c):
  if 'max-age' in c and c['max-age']:
    try:
      max_age_sec = int(c['max-age'])
      return datetime.datetime.now() + datetime.timedelta(seconds=max_age_sec)
    except Exception:
      pass
  # TODO: implement support for 'expires' cookie attribute as well.
  return None


def get_all_matching_cookies(cookie_names, path, resp_headers):
  matching_cookies = None
  if 'Set-Cookie' not in resp_headers:
    return None

  cookies = http_cookies.SimpleCookie()
  try:
    cookies.load(resp_headers['Set-Cookie'])
  except Exception:
    return None

  matching_cookies = []
  for cn in cookie_names:
    if cn in cookies:
      c = cookies[cn]
      if c and cookie_matches_path(c, path):
        matching_cookies.append(c)
  return matching_cookies
