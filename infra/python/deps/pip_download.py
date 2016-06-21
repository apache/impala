#!/usr/bin/python
# Implement the basic 'pip download' functionality in a way that gives us more control
# over which archive type is downloaded and what post-download steps are executed.
import hashlib
import json
import os.path
from urllib import urlopen, URLopener
import sys

pkg_name = sys.argv[1]
pkg_version = sys.argv[2]
pkg_type = 'sdist' # Don't download wheel archives for now
pkg_info = json.loads(urlopen('https://pypi.python.org/pypi/%s/json' % pkg_name).read())

def check_md5sum(filename, expected_md5):
  expected_md5 = pkg['md5_digest']
  actual_md5 = hashlib.md5(open(filename).read()).hexdigest()
  return actual_md5 == expected_md5

found = False
downloader = URLopener()
for pkg in pkg_info['releases'][pkg_version]:
  if pkg['packagetype'] == pkg_type:
    filename = pkg['filename']
    expected_md5 = pkg['md5_digest']
    print "Downloading %s from %s " % (filename, pkg['url'])
    if os.path.isfile(filename) and check_md5sum(filename, expected_md5):
      print "File with matching md5sum already exists, skipping download."
      found = True
      break
    downloader.retrieve(pkg['url'], filename)
    actual_md5 = hashlib.md5(open(filename).read()).hexdigest()
    if not check_md5sum(filename, expected_md5):
      print "MD5 mismatch in file %s." % filename
      sys.exit(1)
    found = True
    break

if not found:
  print "Could not find archive to download for %s %s %s" % (pkg_name, pkg_version,
      pkg_type)
  sys.exit(1)
