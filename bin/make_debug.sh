cd $IMPALA_HOME
cmake -DCMAKE_BUILD_TYPE=Debug .
make clean 

cd $IMPALA_HOME/common/function-registry
make
cd $IMPALA_HOME/common/thrift
make
cd $IMPALA_BE_DIR
make -j
