#ifndef IMPALA_EXPERIMENTS_HASHING_TUPLE_TYPES_H
#define IMPALA_EXPERIMENTS_HASHING_TUPLE_TYPES_H

#include <inttypes.h>
#include <stdlib.h>

#include <glog/logging.h>

namespace impala {

// aggregating tuple stored in the hashtable
struct BuildTuple {
  uint32_t hash; // hash of the column we're grouping on (id)
  int32_t id; // id column, what we're aggregating on
  int64_t count; // current value of aggregate function (COUNT(*))
} __attribute__((__packed__));

// tuple from the db
struct ProbeTuple {
  int32_t id;
  uint32_t hash;
} __attribute__((__packed__));

// Generate n random tuples, with ids in the range [0, max_id).
inline ProbeTuple* GenTuples(uint64_t n, int max_id) {
  ProbeTuple* tuples
    = static_cast<ProbeTuple*>(malloc(n * sizeof(ProbeTuple)));

  CHECK(tuples != NULL);

  for (uint64_t i = 0; i < n; ++i) {
    tuples[i].id = rand() % max_id;
  }
  return tuples;
}



}

#endif
