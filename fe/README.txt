Welcome to the Impala Frontend

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

Background: http://maven.apache.org/guides/mini/guide-ide-eclipse.html

cd into the fe directory ("impala/fe" typically)

If you haven't already:
mvn install
mvn -Declipse.workspace=<path-to-eclipse-workspace> eclipse:configure-workspace

One of the things this will do is to add the M2_REPO classpath variable to
Eclipse. You can verify this in the Eclipse Preferences:

  Java->Build Path->Classpath Variables

It should be set to /home/<user>/.m2/repository on a Linux machine, and
something equivalent on a Mac.

Then generate the eclipse projects:
mvn -DdownloadSources=true -DdownloadJavadocs=true eclipse:eclipse

Then, in Eclipse do the following:

1. File->Import...
2. General->Existing projects into workspace 
3. select the "next" button
4. select the "impala/fe" directory
5. select the "finish" button


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

