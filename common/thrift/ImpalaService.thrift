// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Status.thrift"
include "beeswax.thrift"

// ImpalaService accepts query execution options through beeswax.Query.configuration in
// key:value form. For example, the list of strings could be:
//     "num_nodes:1", "abort_on_error:false"
// The valid keys are listed in this enum. They map to TQueryOptions.
// Note: If you add an option or change the default, you also need to update:
// - JavaConstants.DEFAULT_QUERY_OPTIONS
// - ImpalaInternalService.thrift: TQueryOptions
// - ImpaladClientExecutor.getBeeswaxQueryConfigurations()
// - ImpalaServer::QueryToTClientRequest()
// - ImpalaServer::InitializeConfigVariables()
enum TImpalaQueryOptions {
  // if true, abort execution on the first error
  ABORT_ON_ERROR,
  
  // maximum # of errors to be reported; Unspecified or 0 indicates backend default
  MAX_ERRORS,
  
  // if true, disable llvm codegen
  DISABLE_CODEGEN,
  
  // batch size to be used by backend; Unspecified or a size of 0 indicates backend
  // default
  BATCH_SIZE,
   
  // specifies the degree of parallelism with which to execute the query;
  // 1: single-node execution
  // NUM_NODES_ALL: executes on all nodes that contain relevant data
  // NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
  // > 1: executes on at most that many nodes at any point in time (ie, there can be
  //      more nodes than numNodes with plan fragments for this query, but at most
  //      numNodes would be active at any point in time)
  // Constants (NUM_NODES_ALL, NUM_NODES_ALL_RACKS) are defined in JavaConstants.thrift.
  NUM_NODES,
  
  // maximum length of the scan range; only applicable to HDFS scan range; Unspecified or
  // a length of 0 indicates backend default;  
  MAX_SCAN_RANGE_LENGTH,
  
  // file buffer size used by text parsing; size of 0 indicates the backend's default
  // file buffer size
  FILE_BUFFER_SIZE,

  // Maximum number of io buffers (per disk)
  MAX_IO_BUFFERS,

  // Number of scanner threads.
  NUM_SCANNER_THREADS,
}

// The summary of an insert.
struct TInsertResult {
  // List of partitions that were modified by an insert.
  // Doesn't apply HBase tables.
  // For non-partitioned table, the list will be empty.
  1: required list<string> modified_hdfs_partitions,
  
  // Number of appended rows per modified partition.
  2: required list<i64> rows_appended
}

// For all rpc that return a TStatus as part of their result type,
// if the status_code field is set to anything other than OK, the contents
// of the remainder of the result type is undefined (typically not set)
service ImpalaService extends beeswax.BeeswaxService {
  // Cancel execution of query. Returns RUNTIME_ERROR if query_id
  // unknown.
  // This terminates all threads running on behalf of this query at
  // all nodes that were involved in the execution.
  // Throws BeeswaxException if the query handle is invalid (this doesn't
  // necessarily indicate an error: the query might have finished).
  Status.TStatus Cancel(1:beeswax.QueryHandle query_id)
      throws(1:beeswax.BeeswaxException error);
        
  // Invalidates all catalog metadata, forcing a reload
  Status.TStatus ResetCatalog();
  
  // Closes the query handle and return the result summary of the insert.
  TInsertResult CloseInsert(1:beeswax.QueryHandle handle)
      throws(1:beeswax.QueryNotFoundException error, 2:beeswax.BeeswaxException error2);
}
