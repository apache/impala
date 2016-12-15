#Generating HTML or a PDF of Apache Impala (Incubating) Documentation

##Prerequisites:
Make sure that you have a recent version of a Java JDK installed and that your JAVA\_HOME environment variable is set. This procedure has been tested with JDK 1.8.0. See [Setting JAVA\_HOME](#settingjavahome) at the end of these instructions.

* Open a terminal window and run the following commands to get the Impala documentation source files from Git:
     
     <pre><code>git clone https://git-wip-us.apache.org/repos/asf/incubator-impala.git/docs
    cd \<local\_directory\>
    git checkout doc\_prototype</code></pre>
    
    Where <code>doc\_prototype</code> is the branch where Impala documentation source files are uploaded.

* Download the DITA Open Toolkit version 2.3.3 from the DITA Open Toolkit web site:
   
   [https://github.com/dita-ot/dita-ot/releases/download/2.3.3/dita-ot-2.3.3.zip] (https://github.com/dita-ot/dita-ot/releases/download/2.3.3/dita-ot-2.3.3.zip)
   
  **Note:** A DITA-OT 2.3.3 User Guide is included in the toolkit. Look for <code>userguide.pdf</code> in the <code>doc</code> directory of the toolkit after you extract it. For example, if you extract the toolkit package to the <code>/Users/\<_username_\>/DITA-OT</code> directory on Mac OS, you will find the <code>userguide.pdf</code> at the following location:
  
  <code>/Users/\<_username_\>/DITA-OT/doc/userguide.pdf</code>

##To generate HTML or PDF:

1. In the directory where you cloned the Impala documentation files, you will find the following important configuration files in the <code>docs</code> subdirectory. These files are used to convert the XML source you downloaded from the Apache site to PDF and HTML:
    * <code>impala.ditamap</code>: Tells the DITA Open Toolkit what topics to include in the Impala User/Administration Guide. This guide also includes the Impala SQL Reference.
    * <code>impala\_sqlref.ditamap</code>: Tells the DITA Open Toolkit what topics to include in the Impala SQL Reference.
    * <code>impala\_html.ditaval</code>: Further defines what topics to include in the Impala HTML output.
    * <code>impala\_pdf.ditaval</code>: Further defines what topics to include in the Impala PDF output.
2. Extract the contents of the DITA-OT package into a directory where you want to generate the HTML or the PDF.
3. Open a terminal window and navigate to the directory where you extracted the DITA-OT package.
4.  Run one of the following commands, depending on what you want to generate:
    * **To generate HTML output of the Impala User and Administration Guide, which includes the Impala SQL Reference, run the following command:**
    
        <code>./bin/dita -input \<path\_to\_impala.ditamap\> -format html5 -output \<path\_to\_build\_output\_directory\> -filter \<path\_to\_impala\_html.ditaval\></code>
     
    * **To generate PDF output of the Impala User and Administration Guide, which includes the Impala SQL Reference, run the following command:**
     
        <code>./bin/dita -input \<path\_to\_impala.ditamap\> -format pdf -output \<path\_to\_build\_output\_directory\> -filter \<path\_to\_impala\_pdf.ditaval\></code>
        
    * **To generate HTML output of the Impala SQL Reference, run the following command:**
     
        <code>./bin/dita -input \<path\_to\_impala\_sqlref.ditamap\> -format html5 -output \<path\_to\_build\_output\_directory\> -filter \<path\_to\_impala\_html.ditaval\></code>
     
    * **To generate PDF output of the Impala SQL Reference, run the following command:**
     
        <code>./bin/dita -input \<path\_to\_impala\_sqlref.ditamap\> -format pdf -output \<path\_to\_build\_output\_directory\> -filter \<path\_to\_impala\_pdf.ditaval\></code>

    **Note:** For a description of all command-line options, see the _DITA Open Toolkit User Guide_ in the <code>doc</code> directory of your downloaded DITA Open Toolkit.
 
5. Go to the output directory that you specified in Step 3 to view the HTML or PDF that you generated. If you generated HTML, open the <code>index.html</code> file with a browser to view the output.

<a name="settingjavahome" />
#Setting JAVA\_HOME
</a>

Set your JAVA\_HOME environment variable to tell your computer where to find the Java executable file. For example, to set your JAVA\_HOME environment on Mac OS X when you the the 1.8.0\_101 version of the Java Development Kit (JDK) installed and you are using the Bash version 3.2 shell, perform the following steps:

1. Edit your <code>/Users/\<username\>/.bash\_profile</code> file and add the following lines to the end of the file:

    <pre><code>#Set JAVA_HOME
    JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_101.jdk/Contents/Home
    export JAVA_HOME;</code></pre>
    
   Where <code>jdk1.8.0\_101.jdk</code> is the version of JDK that you have installed. For example, if you have installed <code>jdk1.8.0\_102.jdk</code>, you would use that value instead.
   
2. Test to make sure you have set your JAVA\_HOME correctly:
    * Open a terminal window and type: <code>$JAVA\_HOME/bin/java -version</code>
    * Press return. If you see something like the following:
      <pre><code>java version "1.5.0_16"
      Java(TM) 2 Runtime Environment, Standard Edition (build 1.5.0_16-b06-284)
      Java HotSpot (TM) Client VM (build 1.5.0\_16-133, mixed mode, sharing)</code></pre>
      
      Then you've successfully set your JAVA\_HOME environment variable to the binary stored in <code>/Library/Java/JavaVirtualMachines/jdk1.8.0\_101.jdk/Contents/Home</code>. 