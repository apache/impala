Welcome to the Impala Frontend

Loading Test Data
-----------------

Before running any of the tests for the first time you need to load the test data.

From the fe directory: mvn clean install
From ../testdata: ./recreate_store.sh
This will generate the test data (written to testdata/target).

For now, you also need to delete the existing metastore instance, due to some
incompatibility between the hive cli and the metastore client that's part of Impala:
from the fe directory: rm -rf target/test_metastore_db

(If you see the following error message when trying to load the data, you forgot to rm test_metastore_db:
"[exec] Caused by: java.sql.SQLException: Database at /home/marcel/impala/fe/target/test_metastore_db has an incompatible format with the current version of the software.  The database was created by or upgraded by version 10.6.")

From the fe directory: mvn -Pload-testdata process-test-resources
This creates the test tables and loads the data.

Running Tests
-------------

From the fe directory ("impala/fe" typically) type

  $ mvn test

this will generate code, compile, run all the tests and report
success/failure.

If you want to install the generated artifacts in your local
repository (typically not necessary for developers) type

  $ mvn install

It may take a bit of time the first time through - pulling
dependencies.

If you get a java.lang.OutOfMemoryError: Java heap space, add this to your ~/.bashrc+ or ~/.bash_profile:

export MAVEN_OPTS="-server -Xms256m -Xmx512m"

If you want to run just one test, try

  $ mvn -Dtest=<TestName> test

That looks for TestName and runs that tests.


Setting up Eclipse
------------------

1) Install the M2Eclipse Maven plugin: http://m2eclipse.sonatype.org/

2) Start Eclipse and navigate to Preferences->Maven

   - Check the box for "Update Maven projects on startup"
   
   - Add "process-resources" to the list of goals to run on project import.

   - Add "process-resources" to the list of goals to run when updating
     the project configuration.

   Background: http://m2eclipse.sonatype.org/m2eclipse-faq.html#7

3) Import the Maven project into Eclipse:

   - Navigate to File->Import->Maven->Existing Maven Projects

   - Select the "fe" directory as your "Root Directory".

   - Click on "Finish".

Whitespace
----------
See Cloudera style guide here: https://wiki.cloudera.com/display/engineering/Code+Style+Guides

In summary:

* Use 2 spaces for Java, no tabs.


Generating JavaDoc
------------------
  $ mvn javadoc:javadoc


Running Clover
--------------

  ## maven2 only from what I've seen
  $ mvn -Dmaven.clover.licenseLocation=/path-to-license/clover.license clover2:setup test clover2:aggregate clover2:clove
  ## toplevel
  $ google-chrome target/site/clover/index.html


Running the Impala Frontend
---------------------------

TBD


Setting up the Hive Metastore
-----------------------------

TBD


Running a query
---------------
bin/runquery.sh "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col from alltypessmall" 

or

mvn exec:java -Dexec.classpathScope=test -Dexec.args="'select * from foo'"


Running the CLI
---------------
The IMPALA_HOME environment variable needs to be set, e.g., for me it is /home/abehm/impala

From impala/fe:

  ## the package phase will copy all dependency jars into target/dependency
  $ mvn package
  $ cd IMPALA_HOME/bin
  $ ./cli.sh [.properties_file]
  ## if you didn't specify a .properties file, 
  ## "connect" to impala: Load the metadata from hive's metastore into the Impala in-memory metadata representation.
  ## Note that sqlline supports tab completion.
  sqlline>!properties impala-default.properties;
  ## Enter any password.
  sqlline>select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col from alltypessmall;
  ## querying metadata
  sqlline>!tables
  sqlline>!columns table_name
  sqlline>!describe tabl_name
  
  
