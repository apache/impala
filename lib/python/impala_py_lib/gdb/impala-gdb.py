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
#
# A collection of useful Python GDB modules and commands for
# debugging Impala core dumps.
#
from __future__ import absolute_import, division, print_function
import gdb
from collections import defaultdict


def get_fragment_instances():
    # Walk the threadlist to find fragment instance ids. Assumes IMPALA-6416, so
    # this will not work with releases older than Impala 2.12. It may be possible
    # to search for FragmentInstanceState::Exec() in the older releases to get
    # to the FInstIDs. Returns a dictionary of FInstID->[gdb thread ID].
    # Note that multiple threads might be doing tasks for the same FInstID.

    fragment_instances = defaultdict(list)
    for thread in gdb.selected_inferior().threads():
        thread.switch()

        f = gdb.newest_frame()
        while f:
            # Skip unresolved frame
            if gdb.Frame.name(f) is None:
                f = gdb.Frame.older(f)
                continue
            if 'impala::Thread::SuperviseThread' in gdb.Frame.name(f):
                gdb.Frame.select(f)
                block = gdb.Frame.block(f)
                gdb.lookup_symbol('thread_debug_info', block)
                tdi = f.read_var('thread_debug_info')
                # No valid thread_debug_info
                if not tdi:
                    break
                # ANDing with 0xFFFFFFFFFFFFFFFF forces the value to be unsigned
                hi = int(tdi['instance_id_']['hi']) & 0xFFFFFFFFFFFFFFFF
                lo = int(tdi['instance_id_']['lo']) & 0xFFFFFFFFFFFFFFFF
                fi = "%lx:%lx" % (hi, lo)
                if fi != "0:0":
                    fragment_instances[fi.strip('"')].append(thread.num)
                break
            f = gdb.Frame.older(f)
    return fragment_instances


class FindFragmentInstances(gdb.Command):
    """Find all query fragment instance to thread Id mappings in this impalad."""

    def __init__(self):
        super(FindFragmentInstances, self).__init__(
            "find-fragment-instances",
            gdb.COMMAND_SUPPORT,
            gdb.COMPLETE_NONE,
            False)

    def invoke(self, arg, from_tty):
        fragment_instances = get_fragment_instances()
        print('Fragment Instance Id\tThread IDs\n')
        for fid in sorted(fragment_instances):
            print("{}\t{}".format(fid, fragment_instances[fid]))


class FindQueryIds(gdb.Command):
    """Find IDs of all queries this impalad is currently executing."""

    def __init__(self):
        super(FindQueryIds, self).__init__(
            "find-query-ids",
            gdb.COMMAND_SUPPORT,
            gdb.COMPLETE_NONE,
            False)

    def invoke(self, arg, from_tty):
        fragment_instances = get_fragment_instances()
        query_ids = set()
        for fi in fragment_instances:
            qid_hi, qid_low = fi.split(':')
            # The ANDing serves two purposes
            # - It forces the value to be unsigned
            # - For the low value, it masks out the fragment-specific bits to get
            #   the query id
            qid_low = format(int(qid_low, 16) & 0xFFFFFFFFFFFF0000, 'x')
            qid_hi = format(int(qid_hi, 16) & 0xFFFFFFFFFFFFFFFF, 'x')
            query_ids.add("{}:{}".format(qid_hi, qid_low))
        print('\n'.join(query_ids))


# Instantiate the command classes so they can be invoked from within GDB
FindFragmentInstances()
FindQueryIds()
