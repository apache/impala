# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for addiitional information
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

from __future__ import absolute_import, division, print_function
from tests.util.thrift_util import create_transport
from hive_metastore import ThriftHiveMetastore
from hive_metastore.ttypes import (AbortTxnRequest, AllocateTableWriteIdsRequest,
    CheckLockRequest, CommitTxnRequest, GetValidWriteIdsRequest, HeartbeatRequest,
    LockComponent, LockLevel, LockType, LockRequest, OpenTxnRequest, ShowLocksRequest,
    TruncateTableRequest, UnlockRequest)
from thrift.protocol import TBinaryProtocol

# HMS config
metastore_host = "localhost"
metastore_port = "9083"
service = "Hive Metastore Server"
trans_type = 'buffered'

# User config
user = 'AcidTxn - Impala test'
hostname = 'localhost'


# Utility class for interacting with Hive ACID transactions.
# It's basically a facade, i.e. it provides a simplified interface for HMS.
#
# You can also use it interactively from impala-python, e.g.:
# $> impala-python
# >>> from tests.util.acid_txn import AcidTxn
# >>> at = AcidTxn()
# >>> at.get_open_txns()
class AcidTxn(object):
  def __init__(self, hms_client=None):
    if hms_client:
      self.hms_client = hms_client
    else:
      hive_transport = create_transport(
          host=metastore_host,
          port=metastore_port,
          service=service,
          transport_type=trans_type)
      protocol = TBinaryProtocol.TBinaryProtocol(hive_transport)
      self.hms_client = ThriftHiveMetastore.Client(protocol)
      hive_transport.open()

  def get_hms_client(self):
    return self.hms_client

  def get_open_txns(self):
    return self.hms_client.get_open_txns()

  def get_open_txns_info(self):
    return self.hms_client.get_open_txns_info()

  def open_txns(self):
    open_txn_req = OpenTxnRequest()
    open_txn_req.num_txns = 1
    open_txn_req.user = user
    open_txn_req.hostname = hostname
    open_txn_resp = self.hms_client.open_txns(open_txn_req)
    return open_txn_resp.txn_ids[0]

  def allocate_table_write_ids(self, txn_id, db_name, table_name):
    allocate_req = AllocateTableWriteIdsRequest()
    allocate_req.dbName = db_name
    allocate_req.tableName = table_name
    allocate_req.txnIds = [txn_id]
    resp = self.hms_client.allocate_table_write_ids(allocate_req)
    return resp.txnToWriteIds[0].writeId

  def get_valid_write_ids(self, db_name, table_name):
    get_writeids_req = GetValidWriteIdsRequest()
    get_writeids_req.fullTableNames = ['{}.{}'.format(db_name, table_name)]
    return self.hms_client.get_valid_write_ids(get_writeids_req)

  def show_locks(self, db_name, table_name, part_name=None, is_extended=False):
    show_locks_req = ShowLocksRequest()
    show_locks_req.dbname = db_name
    show_locks_req.tablename = table_name
    show_locks_req.partname = part_name
    show_locks_req.isExtended = is_extended
    return self.hms_client.show_locks(show_locks_req)

  def lock(self, txn_id, db_name, table_name, type=LockType.SHARED_WRITE,
      level=LockLevel.TABLE):
    lock_comp = LockComponent()
    lock_comp.type = type
    lock_comp.level = level
    lock_comp.dbname = db_name
    lock_comp.tablename = table_name
    lock_req = LockRequest()
    lock_req.component = [lock_comp]
    lock_req.txnid = txn_id
    lock_req.user = user
    lock_req.hostname = hostname
    return self.hms_client.lock(lock_req)

  def check_lock(self, lock_id):
    check_lock_req = CheckLockRequest()
    check_lock_req.lockid = lock_id
    return self.hms_client.check_lock(check_lock_req)

  def unlock(self, lock_id):
    unlock_req = UnlockRequest()
    unlock_req.lockid = lock_id
    return self.hms_client.unlock(unlock_req)

  def heartbeat(self, txn_id=None, lock_id=None):
    heartbeat_req = HeartbeatRequest()
    heartbeat_req.txnid = txn_id
    heartbeat_req.lockid = lock_id
    self.hms_client.heartbeat(heartbeat_req)

  def commit_txn(self, txn_id):
    commit_req = CommitTxnRequest()
    commit_req.txnid = txn_id
    return self.hms_client.commit_txn(commit_req)

  def abort_txn(self, txn_id):
    abort_req = AbortTxnRequest()
    abort_req.txnid = txn_id
    return self.hms_client.abort_txn(abort_req)

  def truncate_table_req(self, db_name, table_name):
    truncate_req = TruncateTableRequest()
    truncate_req.dbName = db_name
    truncate_req.tableName = table_name
    return self.hms_client.truncate_table_req(truncate_req)

  def commit_all_open_txns(self):
    open_txns_resp = self.get_open_txns()
    min_open = open_txns_resp.min_open_txn
    for txn in open_txns_resp.open_txns:
      if txn >= min_open:
        try:
          self.commit_txn(txn)
        except Exception as e:
          print(str(e))
