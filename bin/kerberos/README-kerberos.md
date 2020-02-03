# Kerberized Minicluster Setup
The kerberized minicluster is enabled by setting IMPALA_KERBERIZE=true in
impala-config-*.sh.

After setting it you must run ./bin/create-test-configuration.sh then
restart the minicluster (e.g. with testdata/bin/run-all.sh).

The Kerberized minicluster requires a KDC to be setup and configured
and service users to be added to the keytab at $KRB5_KTNAME.
This step is not automated. experimental-kerberos-setup.sh automates
some of the setup but is experimental at this point.

# Limitations
Not all minicluster services actually work or are kerberized at this point.
I was able to run queries against pre-existing HMS tables stored in HDFS.

Kerberos is finicky about hostnames - you may need to tweak your /etc/hosts
to get hosts to authenticate if kerberos thinks that hostnames are distinct.

Killing minicluster services seems to be broken when using a kerberized
minicluster. You may need to manually kill the services.
