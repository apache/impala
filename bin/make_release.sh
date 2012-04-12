#!/usr/bin/env bash
cd $IMPALA_HOME

# build with profile gen enabled
cmake -DCMAKE_BUILD_TYPE=PROFILE_GEN .
make clean 
cd $IMPALA_HOME/common/function-registry
make
cd $IMPALA_HOME/common/thrift
make
cd $IMPALA_BE_DIR
make -j
cd $IMPALA_HOME

# Run sample queries - outputs .gcda files 
be/build/release/service/runquery -query="select count(field) from grep1gb where field like '%xyz%';select count(field) from grep1gb_seq_snap where field like '%xyz%';select sourceIP, SUM(adRevenue) FROM uservisits_seq GROUP by sourceIP order by SUM(adRevenue) desc limit 10;select sourceIP, SUM(adRevenue) FROM uservisits GROUP by sourceIP order by SUM(adRevenue) desc limit 10;select uv.sourceip, avg(r.pagerank), sum(uv.adrevenue) as totalrevenue from uservisits uv join rankings r on (r.pageurl = uv.desturl) where uv.visitdate > '1999-01-01' and uv.visitdate < '2000-01-01' group by uv.sourceip order by totalrevenue desc limit 1" -profile_output_file=""

# Build again using the PGO data
cmake -DCMAKE_BUILD_TYPE=PROFILE_BUILD .
make clean 
cd $IMPALA_HOME/common/function-registry
make
cd $IMPALA_HOME/common/thrift
make
cd $IMPALA_BE_DIR
make -j
cd $IMPALA_HOME

# Remove all the PGO intermediates and set build to release.  This seems to be the most 
# convenient workflow.  Otherwise, changing one src file after a PGO build causes many warnings 
# about stale PGO data.
find . -type f -name "*.gcda" -exec rm -rf {} \;
cmake -DCMAKE_BUILD_TYPE=RELEASE

