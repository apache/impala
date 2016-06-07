#!/usr/bin/python
# Implement the basic 'pip download' functionality in a way that gives us more control
# over which archive type is downloaded and what post-download steps are executed.
import hashlib
import json
from urllib import urlopen, URLopener
import sys

pkg_name = sys.argv[1]
pkg_version = sys.argv[2]
pkg_type = 'sdist' # Don't download wheel archives for now
pkg_info = json.loads(urlopen('https://pypi.python.org/pypi/%s/json' % pkg_name).read())

found = False
downloader = URLopener()
for pkg in pkg_info['releases'][pkg_version]:
  if pkg['packagetype'] == pkg_type:
    print "Downloading %s from %s " % (pkg['filename'], pkg['url'])
    downloader.retrieve(pkg['url'], pkg['filename'])
    expected_md5 = pkg['md5_digest']
    actual_md5 = hashlib.md5(open(pkg['filename']).read()).hexdigest()
    if actual_md5 != expected_md5:
      print "MD5 mismatch: %s v. %s" % (expected_md5, actual_md5)
      sys.exit(1)
    found = True
    break

if not found:
  print "Could not find archive to download for %s %s %s" % (pkg_name, pkg_version,
      pkg_type)
  sys.exit(1)
