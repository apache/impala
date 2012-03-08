# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-hbase.sh 2&>1

# To work around HBase bug (HBASE-4467), unset $HADOOP_HOME before calling hbase
HADOOP_HOME=

# Start HBase and 3 regionserver
$HBASE_HOME/bin/start-hbase.sh
$HBASE_HOME/bin/local-regionservers.sh start 1 2 3
