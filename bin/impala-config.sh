#  Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# Source this file from the $IMPALA_HOME directory to
# setup your environment. If $IMPALA_HOME is undefined
# this script will set it to the current working directory.

if [ -z $IMPALA_HOME ]; then
    this="$0"
    if [ "$this" = "$SHELL" ]; then
        # Assume we're already in IMPALA_HOME
        interactive=1
        export IMPALA_HOME=`pwd`
    else
        interactive=0
        while [ -h "$this" ]; do
            ls=`ls -ld "$this"`
            link=`expr "$ls" : '.*-> \(.*\)$'`
            if expr "$link" : '.*/.*' > /dev/null; then
                this="$link"
            else
                this=`dirname "$this"`/"$link"
            fi
        done
        
        # convert relative path to absolute path
        bin=`dirname "$this"`
        script=`basename "$this"`
        bin=`cd "$bin"; pwd`
        this="$bin/$script"
        
        export IMPALA_HOME=`dirname "$bin"`
    fi
fi


export IMPALA_FE_DIR=$IMPALA_HOME/fe
export IMPALA_BE_DIR=$IMPALA_HOME/be
export PATH=$IMPALA_HOME/bin:$PATH

export HADOOP_HOME=$IMPALA_HOME/thirdparty/hadoop-0.20.2-cdh3u1
export PATH=$HADOOP_HOME/bin:$PATH

export HIVE_HOME=$IMPALA_HOME/thirdparty/hive-0.7.1-cdh3u1
export PATH=$HIVE_HOME/bin:$PATH
export HIVE_CONF_DIR=$IMPALA_HOME/fe/src/test/resources

export HBASE_HOME=$IMPALA_HOME/thirdparty/hbase-0.90.3-cdh3u1
export PATH=$HBASE_HOME/bin:$PATH
export HBASE_CONF_DIR=$HIVE_CONF_DIR

if [[ $interactive -eq 1 || -n $IMPALA_DEBUG ]]; then
    echo "IMPALA_HOME    = $IMPALA_HOME"
    echo "HADOOP_HOME    = $HADOOP_HOME"
    echo "HIVE_HOME      = $HIVE_HOME"
    echo "HIVE_CONF_DIR  = $HIVE_CONF_DIR"
fi

