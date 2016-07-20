#!/usr/bin/python
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Implement the basic 'pip download' functionality in a way that gives us more control
# over which archive type is downloaded and what post-download steps are executed.
# This script requires Python 2.6+.

import json
import os.path
import sys
from hashlib import md5
from time import sleep
from urllib import urlopen, URLopener

NUM_TRIES = 3

def check_md5sum(filename, expected_md5):
  actual_md5 = md5(open(filename).read()).hexdigest()
  return actual_md5 == expected_md5

def retry(func):
  '''Retry decorator.'''

  def wrapper(*args, **kwargs):
    for _ in xrange(NUM_TRIES):
      try:
        result = func(*args, **kwargs)
        if result: return result
      except Exception as e:
        print e
      sleep(5)
    print "Download failed after several attempts."
    sys.exit(1)

  return wrapper

@retry
def download_package(pkg_name, pkg_version):
  '''Download the required package. Sometimes the download can be flaky, so we use the
  retry decorator.'''
  pkg_type = 'sdist' # Don't download wheel archives for now
  pkg_info = json.loads(urlopen('https://pypi.python.org/pypi/%s/json' % pkg_name).read())

  downloader = URLopener()
  for pkg in pkg_info['releases'][pkg_version]:
    if pkg['packagetype'] == pkg_type:
      filename = pkg['filename']
      expected_md5 = pkg['md5_digest']
      if os.path.isfile(filename) and check_md5sum(filename, expected_md5):
        print "File with matching md5sum already exists, skipping %s" % filename
        return True
      print "Downloading %s from %s " % (filename, pkg['url'])
      downloader.retrieve(pkg['url'], filename)
      actual_md5 = md5(open(filename).read()).hexdigest()
      if check_md5sum(filename, expected_md5):
        return True
      else:
        print "MD5 mismatch in file %s." % filename
        return False
  print "Could not find archive to download for %s %s %s" % (
      pkg_name, pkg_version, pkg_type)
  sys.exit(1)

def main():
  if len(sys.argv) > 1:
    _, pkg_name, pkg_version = sys.argv
    download_package(pkg_name, pkg_version)
  else:
    # If the package name and version are not specified in the command line arguments,
    # download the packages that in requirements.txt.
    f = open("requirements.txt", 'r')
    try:
      # requirements.txt follows the standard pip grammar.
      for line in f:
        # A hash symbol ("#") represents a comment that should be ignored.
        hash_index = line.find('#')
        if hash_index != -1:
          line = line[:hash_index]
        # A semi colon (";") specifies some additional condition for when the package
        # should be installed (for example a specific OS). We can ignore this and download
        # the package anyways because the installation script(bootstrap_virtualenv.py) can
        # take it into account.
        semi_colon_index = line.find(';')
        if semi_colon_index != -1:
          line = line[:semi_colon_index]
        l = line.strip()
        if len(l) > 0:
          pkg_name, pkg_version = l.split('==')
          download_package(pkg_name.strip(), pkg_version.strip())
    finally:
      f.close()

if __name__ == '__main__':
  main()
