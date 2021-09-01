Changelog
=========

0.4.3
------
  - Identical to 0.4.3a2

0.4.3a2
------
* **Improvements**
  - Use pure-sasl instead of sasl to avoid dependency on g++

0.4.3a1
------
* **Bug Fixes**
  - Replaced environment conditionals with environment markers in `setup.py` (#29)
  - Make sure frames are fully buffered when used together with thriftpy / thriftpy2 (#31)

* **Improvements**
  - Added build script to build sdist archive and universal wheel
  - Unpin thrift version with Python 2

0.4.2
------
* **Bug Fixes**
  - Fixes a bug where Thrift transport was not reading all data (#22)

0.4.1
------
* **Bug Fixes**
  - Fix compatibility with Python 2 and 3 (#19)

* **Improvements**
  - Add CHANGELOG.md and remove old contacts from setup.py (#20)
