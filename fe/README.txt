Welcome to the Impala Frontend

Loading Test Data
-----------------

Before running any of the tests for the first time you need to load the test data.

From the fe directory: mvn clean install
From ../testdata: ./recreate_store.sh
This will generate the test data (written to testdata/target).

(For now, you also need to delete the existing metastore instance, due to some
incompatibility between the hive cli and the metastore client that's part of Impala:
from the fe directory: rm -rf target/test_metastore_db)

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


Running the Parser
------------------
From the impala/fe directory you can run the parser via

  $ mvn exec:java -Dexec.mainClass=com.cloudera.impala.parser.Main -Dexec.args="'select * from foo'"

or in parser debug mode (spits out transitions to stdout):

  $ mvn exec:java -Dexec.mainClass=com.cloudera.impala.parser.Main -Dexec.args="-d 'select * from foo'"


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


mvn exec:java -Dexec.mainClass=com.cloudera.impala.parser.Main
-Dexec.args="'select * from foo'"

