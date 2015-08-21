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

import re
from datetime import datetime

NEW_GLOG_ENTRY_PATTERN = re.compile(r"[IWEF](?P<Time>\d{4} \d{2}:\d{2}:\d{2}\.\d{6}).*")

def parse_glog(text, start_time=None):
  '''Parses the log 'text' and returns a list of log entries. If a 'start_time' is
     provided only log entries that are after the time will be returned.
  '''
  year = datetime.now().year
  found_start = False
  log = list()
  entry = None
  for line in text.splitlines():
    if not found_start:
      found_start = line.startswith("Log line format: [IWEF]mmdd hh:mm:ss.uuuuuu")
      continue
    match = NEW_GLOG_ENTRY_PATTERN.match(line)
    if match:
      if entry:
        log.append("\n".join(entry))
      if not start_time or start_time <= datetime.strptime(
          match.group("Time"), "%m%d %H:%M:%S.%f").replace(year):
        entry = [line]
      else:
        entry = None
    elif entry:
      entry.append(line)
  if entry:
    log.append("\n".join(entry))
  return log


def parse_mem_to_mb(mem, units):
  mem = float(mem)
  if mem <= 0:
    return
  units = units.strip().upper()
  if units.endswith("B"):
    units = units[:-1]
  if not units:
    mem /= 10 ** 6
  elif units == "K":
    mem /= 10 ** 3
  elif units == "M":
    pass
  elif units == "G":
    mem *= 10 ** 3
  elif units == "T":
    mem *= 10 ** 6
  else:
    raise Exception('Unexpected memory unit "%s"' % units)
  return int(mem)
