# To work around the HBase bug (HBASE-4467), unset $HADOOP_HOME before calling hbase
HADOOP_HOME=

# create the splits
#   HBase bug (HBASE-3495) re-appearing:, the split might fail.
#   Call a splits a few times and it usually can create all the splits.
#   sleep for some time at the end to let the balancer kicks in
sleep 10
$HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/bin/split.hbase
sleep 2
$HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/bin/split.hbase
sleep 2
$HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/bin/split.hbase
sleep 2
$HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/bin/split.hbase
sleep 2
$HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/bin/split.hbase
sleep 10
