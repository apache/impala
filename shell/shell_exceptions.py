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


class RPCException(Exception):
  def __init__(self, value="", exception_type=""):
    self.value = value
    self.exception_type = exception_type

  def __str__(self):
    return self.value


class QueryStateException(Exception):
  def __init__(self, value=""):
    self.value = value

  def __str__(self):
    return self.value


class DisconnectedException(Exception):
  def __init__(self, value=""):
      self.value = value

  def __str__(self):
      return self.value


class QueryCancelledByShellException(Exception): pass


class MissingThriftMethodException(Exception):
  """Thrown if a Thrift method that the client tried to call is missing."""
  def __init__(self, value=""):
      self.value = value

  def __str__(self):
      return self.value


class HttpError(Exception):
  """An error containing an http response code and a possible message body"""
  def __init__(self, code, message, body, http_headers):
    self.code = code
    self.message = message
    self.body = body
    self.http_headers = http_headers

  def __str__(self):
    if not self.body:
      return "HTTP code {}: {}".format(self.code, self.message)
    else:
      return "HTTP code {}: {} [{}]".format(self.code, self.message, self.body)
