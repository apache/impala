#include "standard-hash-table.h"
#include "standard-hash-table.inline.h"

#include <inttypes.h>
#include <sstream>
#include <stdlib.h>

#include <vector>

using namespace impala;

StandardHashTable::StandardHashTable() {
  num_nodes_ = 0;
}

void StandardHashTable::BucketSizeDistribution() {
  std::vector<int> bucket_size;
  for (int i = 0; i < BUCKETS; ++i) {
    int len = 0;
    for (int j = buckets_[i].node_idx_; j != NULL_CONTENT; j = nodes_[j].next_idx_) {
      ++len;
    }
    if (len >= bucket_size.size()) {
      // grow bucket_size to fit this size
      bucket_size.resize(len + 1, 0);
    }
    ++bucket_size[len];
  }

  std::stringstream distr;
  for (int i = 0; i < bucket_size.size(); ++i) {
    distr << i << ": " << bucket_size[i] << "\n";
  }
  LOG(INFO) << "Bucket Size Distribution\n" << distr.str();
}
