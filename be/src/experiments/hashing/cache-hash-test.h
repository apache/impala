#ifndef IMPALA_EXPERIMENTS_HASHING_CACHE_HASH_TEST_H
#define IMPALA_EXPERIMENTS_HASHING_CACHE_HASH_TEST_H

#include <inttypes.h>


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
} __attribute__((__packed__));


}

#endif
