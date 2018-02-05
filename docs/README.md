# Generating HTML or a PDF of Apache Impala Documentation

## Prerequisites

Make sure that you have a recent version of a Java JDK installed and that your
JAVA_HOME environment variable is set. This procedure has been tested with JDK
1.8.0. See [Setting JAVA_HOME](#setting-java_home) at the end of these
instructions.

## Download Docs Source

* Open a terminal window and run the following commands to get the Impala
  documentation source files from Git:
    ```
    git clone https://git-wip-us.apache.org/repos/asf/impala.git/docs
    cd <local_directory>
    git checkout master
    ```

    Where `master` is the branch where Impala documentation source files
    are uploaded.

## Download DITA Open Toolkit

* Download the DITA Open Toolkit version 2.3.3 from the DITA Open Toolkit web site:

   [https://github.com/dita-ot/dita-ot/releases/download/2.3.3/dita-ot-2.3.3.zip](https://github.com/dita-ot/dita-ot/releases/download/2.3.3/dita-ot-2.3.3.zip)

  **Note:** A DITA-OT 2.3.3 User Guide is included in the toolkit. Look
  for `userguide.pdf` in the `doc` directory of the toolkit after you
  extract it. For example, if you extract the toolkit package to the
  `/Users/<username>/DITA-OT` directory on Mac OS, you will find the
  `userguide.pdf` at the following location:

  ```
  /Users/<username>/DITA-OT/doc/userguide.pdf
  ```

## Add dita Executable to Your PATH

1. Identify the directory into which you extracted DITA-OT. For this
   exercise, we'll assume it's `/Users/<username>/DITA-OT`
2. Find your `.bash_profile`. On Mac OS X, it is probably
   `/Users/<username>/.bash_profile`.
3. Edit your `<path_to_bash_profile>/.bash_profile` file and add the
   following lines to the end of the file.
    ```
    # Add dita to path
    export PATH="/Users/<username>/DITA-OT/bin:$PATH"
    ```
   Save the file.
4. Open a new terminal, or run `source <path_to_bash_profile>/.bash_profile`.
5. Verify `dita` is in your `PATH`. A command like `which dita` should
   print the location of the `dita` executable, like:
   ```
   $ which dita
   /Users/<username>/DITA-OT/bin/dita
   ```

## Verify dita Executable Can Run

In a terminal, try `dita --help`. You should get brief usage, like:
   ```
   Usage: dita -i <file> -f <name> [options]
      or: dita -install [<file>]
      or: dita -uninstall <id>
      or: dita -help
      or: dita -version
   Arguments:
     -i, -input <file>      input file
     -f, -format <name>     output format (transformation type)
     -install [<file>]      install plug-in from a ZIP file or reload plugins
     -uninstall <id>        uninstall plug-in with the ID
     -h, -help              print this message
     -version               print version information and exit
   Options:
     -o, -output <dir>      output directory
     -filter <file>         filter and flagging file
     -t, -temp <dir>        temporary directory
     -v, -verbose           verbose logging
     -d, -debug             print debugging information
     -l, logfile <file>     use given file for log
     -D<property>=<value>   use value for given property
     -propertyfile <name>   load all properties from file with -D
                            properties taking precedence
   ```

If you don't get this, or you get an error, see [Setting
JAVA_HOME](#setting-java_home) and [Troubleshooting](#troubleshooting)
at the end of these instructions.

## Oneshot Docs Build

The easiest way to build the docs is to run `make` from the `docs/`
directory corresponding to your `git clone`. It takes about 1 minute.
This works because the `make` uses the provided `Makefile` to call
`dita` properly.

Docs will end up in `docs/build` (both HTML and PDF).

## Details, Advanced Usage

1. In the directory where you cloned the Impala documentation files, you
   will find the following important configuration files in the `docs`
   subdirectory. These files are used to convert the XML source you
   downloaded from the Apache site to PDF and HTML:
    * `impala.ditamap`: Tells the DITA Open Toolkit what topics to
      include in the Impala User/Administration Guide. This guide also
      includes the Impala SQL Reference.
    * `impala_html.ditaval`: Further defines what topics to include in
      the Impala HTML output.
    * `impala_pdf.ditaval`: Further defines what topics to include in
      the Impala PDF output.
2. Run one of the following commands, depending on what you want to
   generate:
    * **To generate HTML output of the Impala User and Administration
      Guide, which includes the Impala SQL Reference, run the following
      command:**
        ```
        dita -input <path_to_impala.ditamap> -format html5 \
          -output <path_to_build_output_directory> \
          -filter <path_to_impala_html.ditaval>
        ```

    * **To generate PDF output of the Impala User and Administration
      Guide, which includes the Impala SQL Reference, run the following
      command:**

        ```
        dita -input <path_to_impala.ditamap> -format pdf \
          -output <path_to_build_output_directory> \
          -filter <path_to_impala_pdf.ditaval>
        ```

    **Note:** For a description of all command-line options, see the
    _DITA Open Toolkit User Guide_ in the `doc` directory of your
    downloaded DITA Open Toolkit.

# Setting JAVA_HOME

Set your JAVA_HOME environment variable to tell your computer where to
find the Java executable file. For example, to set your JAVA_HOME
environment on Mac OS X when you have the 1.8.0_101 version of the Java
Development Kit (JDK) installed and you are using the Bash version 3.2
shell, perform the following steps:

1. Find your `.bash_profile`. On Mac OS X, it is probably
   `/Users/<username>/.bash_profile`. Edit your
   `<path_to_bash_profile>/.bash_profile` file and add the following
   lines to the end of the file.
    ```
    # Set JAVA_HOME
    JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_101.jdk/Contents/Home
    export JAVA_HOME
    ```

   Where `jdk1.8.0_101.jdk` is the version of JDK that you have
   installed. For example, if you have installed `jdk1.8.0_102.jdk`, you
   would use that value instead.

2. Open a new terminal, or run `source <path_to_bash_profile>/.bash_profile`.
3. Test to make sure you have set your JAVA_HOME correctly:
    * Open a terminal window and type: `$JAVA_HOME/bin/java -version`
    * Press return. If you see something like the following:
      ```
      java version "1.8.0_101"
      Java(TM) 2 Runtime Environment, Standard Edition (build 1.8.0_101-b06-284)
      Java HotSpot (TM) Client VM (build 1.8.0_101-133, mixed mode, sharing)
      ```

      Then you've successfully set your JAVA_HOME environment variable
      to the binary stored in
      `/Library/Java/JavaVirtualMachines/jdk1.8.0_101.jdk/Contents/Home`.

      **Note:** The exact version and build number on your system may
      differ. The point is you want a message like the above.

# Troubleshooting

## Ant

If you're trying to use DITA-OT to build docs and you get an exception like this
```
java.lang.NoSuchMethodError: org.apache.tools.ant.Main: method <init>()V not found
    at org.dita.dost.invoker.Main.<init>(Main.java:418)
    at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
    at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:57)
    at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
    at java.lang.reflect.Constructor.newInstance(Constructor.java:526)
    at java.lang.Class.newInstance(Class.java:379)
    at org.apache.tools.ant.launch.Launcher.run(Launcher.java:279)
    at org.apache.tools.ant.launch.Launcher.main(Launcher.java:109)
```

... your `CLASSPATH` may be interfering with DITA-OT's ability to find
the proper Ant. While you're free to fix the `CLASSPATH` yourself, it
may be easier just to run

`unset CLASSPATH`

and try again. This will use the libraries and Ant provided by the
DITA-OT package.
