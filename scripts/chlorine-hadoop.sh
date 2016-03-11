CHLORINE_JAR=<location of the chlorine jar>
HADOOP_HOME=/apache/hadoop-2.7.1
HADOOP_CONF=$HADOOP_HOME/etc/hadoop
HADOOP_JAR_BASE=$HADOOP_HOME/share/hadoop
HADOOP_JARS=$HADOOP_JAR_BASE/common/*:$HADOOP_JAR_BASE/common/lib/*:$HADOOP_JAR_BASE/hdfs/*:$HADOOP_JAR_BASE/hdfs/lib/*:$HADOOP_JAR_BASE/yarn/*:$HADOOP_JAR_BASE/yarn/lib/*:$HADOOP_JAR_BASE/mapreduce/*:$HADOOP_JAR_BASE/mapeduce/lib/*
java -Xmx1000m -XX:MaxPermSize=128M  -cp $HADOOP_CONF:$CHLORINE_JAR:$HADOOP_JARS io.dataapps.chlorine.hadoop.Scan $*
