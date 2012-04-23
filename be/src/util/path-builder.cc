// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/path-builder.h"

#include <sstream>
#include <stdlib.h>

using namespace impala;
using namespace std;

const char* PathBuilder::impala_home_;

void PathBuilder::LoadImpalaHome() {
  if (impala_home_ != NULL) return;
  impala_home_ = getenv("IMPALA_HOME");
}

void PathBuilder::GetFullPath(const string& path, string* full_path) {
  LoadImpalaHome();
  stringstream s;
  s << impala_home_ << "/" << path;
  *full_path = s.str();
}

void PathBuilder::GetFullBuildPath(const string& path, string* full_path) {
  LoadImpalaHome();
  stringstream s;
#ifdef NDEBUG
  s << impala_home_ << "/be/build/release/" << path;
#else
  s << impala_home_ << "/be/build/debug/" << path;
#endif
  *full_path = s.str();
}


