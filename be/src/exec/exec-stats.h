// (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_EXEC_STATS_H
#define IMPALA_EXEC_EXEC_STATS_H

namespace impala {

// A simple container class for summary statistics gathered by a coordinator about a
// single query. We don't use counters here because a) there's a non-zero overhead
// associated with them and b) they can be compiled out; these stats are required for
// the correct operation of the query.
class ExecStats {
public:
  int num_rows() { return num_rows_; }

  enum QueryType { SELECT = 0, INSERT };
  QueryType query_type() { return query_type_; }

  bool is_insert() { return query_type_ == INSERT; }

private:
  // Number of rows returned, or written to a table sink by this query
  int num_rows_;

  // Whether this query is an INSERT or a SELECT
  QueryType query_type_;

  // Coordinators / executors can update these stats directly, this
  // saves writing accessor methods.
  friend class Coordinator;
  friend class QueryExecutor;
};

}

#endif
