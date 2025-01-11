# Impala Interactive Shell

You can use the Impala shell tool (impala-shell) to connect to an Impala
service. The shell allows you to set up databases and tables, insert data,
and issue queries. For ad hoc queries and exploration, you can submit SQL
statements in an interactive session. The impala-shell interpreter accepts
all the same SQL statements listed in
[Impala SQL Statements](http://impala.apache.org/docs/build/html/topics/impala_langref_sql.html),
plus some shell-only commands that you can use for tuning performance and
diagnosing problems.

To automate your work, you can specify command-line options to process a single
statement or a script file. (Other avenues for Impala automation via python
are provided by Impyla or ODBC.)

## Installing

```
$ pip install impala-shell
```

## Online documentation

* [Impala Shell Documentation](http://impala.apache.org/docs/build/html/topics/impala_impala_shell.html)
* [Apache Impala Documentation](http://impala.apache.org/impala-docs.html)

## Quickstart

### Non-interactive mode

Processing a single query, e.g., ```show tables```:

```
$ impala-shell -i impalad-host.domain.com -d some_database -q 'show tables'
```

Processing a text file with a series of queries:

```
$ impala-shell -i impalad-host.domain.com -d some_database -f /path/to/queries.sql
```

### Launching the interactive shell

To connect to an impalad host at the default service port (21000):

```
$ impala-shell -i impalad-host.domain.com
Starting Impala Shell without Kerberos authentication
Connected to impalad-host.domain.com:21000
Server version: impalad version 2.11.0-SNAPSHOT RELEASE (build d4596f9ca3ea32a8008cdc809a7ac9a3dea47962)
***********************************************************************************
Welcome to the Impala shell.
(Impala Shell v3.0.0-SNAPSHOT (73e90d2) built on Thu Mar  8 00:59:00 PST 2018)

The '-B' command line flag turns off pretty-printing for query results. Use this
flag to remove formatting from results you want to save for later, or to benchmark
Impala.
***********************************************************************************
[impalad-host.domain.com:21000] >
```

### Launching the interactive shell (secure mode)

To connect to a secure host using kerberos and SSL:

```
$ impala-shell -k --ssl -i impalad-secure-host.domain.com
```

### Disconnecting

To exit the shell when running interactively, press ```Ctrl-D``` at the shell prompt.
