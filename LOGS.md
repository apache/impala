All logs, test results and SQL files generated during data loading
and testing are consolidated under $IMPALA_HOME/logs with the
following directory structure:

$IMPALA_HOME/logs/cluster
- logs of Hadoop components and Impala

$IMPALA_HOME/logs/data_loading
- logs and SQL files produced in data loading

$IMPALA_HOME/logs/fe_tests
- logs and test output of Frontend unit tests

$IMPALA_HOME/logs/be_tests
- logs and test output of Backend unit tests

$IMPALA_HOME/logs/ee_tests
- logs and test output of end-to-end tests

$IMPALA_HOME/logs/custom_cluster_tests
- logs and test output of custom cluster tests
