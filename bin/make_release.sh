#!/usr/bin/env bash
# Copyright 2012 Cloudera Inc.
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

set -e
set -u

USE_PGO=0
TARGET_BUILD_TYPE=RELEASE

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -nopgo)
      USE_PGO=0
      ;;
    -codecoverage)
      TARGET_BUILD_TYPE=CODE_COVERAGE_RELEASE
      ;;
    -help|*)
      echo "make_release.sh [-nopgo] [-codecoverage]"
      echo "[-nopgo] : do not use performance guided optimizations for building"
      echo "[-codecoverage] : build with 'gcov' code coverage instrumentation"
      exit 1
      ;;
  esac
done

cd $IMPALA_HOME
bin/gen_build_version.py
rm ./CMakeCache.txt

rm -f $IMPALA_HOME/llvm-ir/impala-nosse.ll
rm -f $IMPALA_HOME/llvm-ir/impala-sse.ll

if [ $USE_PGO -eq 1 ]
then

  # build with profile gen enabled
  cmake -DCMAKE_BUILD_TYPE=PROFILE_GEN .
  make clean
  cd $IMPALA_HOME/common/function-registry
  make
  cd $IMPALA_HOME/common/thrift
  make
  cd $IMPALA_BE_DIR
  make -j4
  cd $IMPALA_HOME

  # Run sample queries - outputs .gcda files
  # TODO: this is no longer the way to run queries.
  be/build/release/service/runquery -query="\
    select count(field) from grep1gb where field like '%xyz%';\
    select count(field) from grep1gb_rc_def where field like '%xyz%';\
    select count(field) from grep1gb_seq_bzip where field like '%xyz%';\
    select count(field) from grep1gb_trevni_snap where field like '%xyz%';\
    select sourceIP, SUM(adRevenue) FROM uservisits_seq \
      GROUP by sourceIP order by SUM(adRevenue) desc limit 10;\
    select sourceIP, SUM(adRevenue) FROM uservisits_trevni \
      GROUP by sourceIP order by SUM(adRevenue) desc limit 10;\
    select sourceIP, SUM(adRevenue) FROM uservisits \
      GROUP by sourceIP order by SUM(adRevenue) desc limit 10;\
    select sourceIP, SUM(adRevenue) FROM uservisits_rc GROUP by sourceIP \
      order by SUM(adRevenue) desc limit 10;select sourceIP, SUM(adRevenue) \
      FROM uservisits \
      GROUP by sourceIP order by SUM(adRevenue) desc limit 10;\
    select uv.sourceip, avg(r.pagerank), sum(uv.adrevenue) as totalrevenue \
      from uservisits uv join rankings r on \
      (r.pageurl = uv.desturl) \
      where uv.visitdate > '1999-01-01' and uv.visitdate < '2000-01-01' \
      group by uv.sourceip order by totalrevenue desc limit 1; \
    select uv.sourceip, avg(r.pagerank), sum(uv.adrevenue) as totalrevenue \
      from uservisits_trevni_def  uv join rankings_trevni_def  r on \
      (r.pageurl = uv.desturl) \
      where uv.visitdate > '1999-01-01' and uv.visitdate < '2000-01-01' \
      group by uv.sourceip order by totalrevenue desc limit 1"\
    -profile_output_file=""

  # Build again using the PGO data
  cmake -DCMAKE_BUILD_TYPE=PROFILE_BUILD .
  make clean
  cd $IMPALA_HOME/common/function-registry
  make
  cd $IMPALA_HOME/common/thrift
  make
  cd $IMPALA_BE_DIR
  make -j4
  cd $IMPALA_HOME

  # Remove all the PGO intermediates and set build to release.  This seems to be the most
  # convenient workflow.  Otherwise, changing one src file after a PGO build causes many warnings
  # about stale PGO data.
  find . -type f -name "*.gcda" -exec rm -rf {} \;
  cmake -DCMAKE_BUILD_TYPE=RELEASE
else
  cmake -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE
  make clean

  cd $IMPALA_HOME/common/function-registry
  make
  cd $IMPALA_HOME/common/thrift
  make
  cd $IMPALA_BE_DIR
  make -j4
fi
