# chlorine-hadoop
*Detect sensitive data in Hadoop*

The chlorine-hadoop library defines a mapreduce program which can be used to detect sensitive elements in HDFS directoires. It is Java based.

Chlorine-hadoop can detect different types of Credit card numbers, SSN, Phone Numbers, email adddresses, Ip Addresses, Street Addresses in HDFS datasets. It works with compressed text files.


###To Download source code

*git clone https://github.com/dataApps/chlorine-hadoop.git*

###To build chlorine-hadoop

*mvn install*

###To use Chlorine-hadoop

```
usage: Scan [-help] [-i <path>] [-o <path>] [-q <name>]
 -help                     print this message
 -i,--input_path <path>    input path to scan
 -o,--output_path <path>   Output path to store results
 -q,--queue <name>         job queue
```

- A sample script to run chlorine-hadoop job on an HDFS directory. The script template available under  [/scripts/chlorine-hadoop.sh](https://github.com/dataApps/chlorine-hadoop/scripts/chlorine-hadoop.sh)

```
 CHLORINE_JAR=/home/benoy/work/chlorine-hadoop/target/chlorine-hadoop-1.1.5-SNAPSHOT.jar
HADOOP_HOME=/apache/hadoop-2.7.1
HADOOP_CONF=$HADOOP_HOME/etc/hadoop
HADOOP_JAR_BASE=$HADOOP_HOME/share/hadoop
HADOOP_JARS=$HADOOP_JAR_BASE/common/*:$HADOOP_JAR_BASE/common/lib/*:$HADOOP_JAR_BASE/hdfs/*:$HADOOP_JAR_BASE/hdfs/lib/*:$HADOOP_JAR_BASE/yarn/*:$HADOOP_JAR_BASE/yarn/lib/*:$HADOOP_JAR_BASE/mapreduce/*:$HADOOP_JAR_BASE/mapeduce/lib/*
java -Xmx1000m -XX:MaxPermSize=128M  -cp $HADOOP_CONF:$CHLORINE_JAR:$HADOOP_JARS io.dataapps.chlorine.hadoop.Scan $*

```

###A sample run

```
benoy:~/work/chlorine-hadoop$ ../scripts/chlorine-hadoop.sh -i /data/csv_nohive -o /user/benoy/2 -q regular

Creating a DeepScan with the following inputs
input-path=/data/csv_nohive
output-path=/user/benoy/2
queue=regular
16/03/11 11:06:25 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
16/03/11 11:06:26 INFO Configuration.deprecation: mapred.output.compress is deprecated. Instead, use mapreduce.output.fileoutputformat.compress
16/03/11 11:06:26 INFO Configuration.deprecation: mapred.job.queue.name is deprecated. Instead, use mapreduce.job.queuename
16/03/11 11:06:26 INFO hadoop.AbstractPipeline: ***********Run job: HDFSScan
16/03/11 11:06:26 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
16/03/11 11:06:26 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
16/03/11 11:06:26 INFO input.FileInputFormat: Total input paths to process : 4
16/03/11 11:06:26 INFO mapreduce.JobSubmitter: number of splits:4
16/03/11 11:06:26 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1457715024866_0009
16/03/11 11:06:27 INFO impl.YarnClientImpl: Submitted application application_1457715024866_0009
16/03/11 11:06:27 INFO mapreduce.Job: The url to track the job: http://benoy-Satellite-C55-C:8088/proxy/application_1457715024866_0009/
16/03/11 11:06:42 INFO hadoop.AbstractPipeline: Job HDFSScan successful.
16/03/11 11:06:42 INFO hadoop.DeepScanPipeline: Total bytes scanned = 896
16/03/11 11:06:42 INFO hadoop.DeepScanPipeline: Total records scanned = 8
16/03/11 11:06:42 INFO hadoop.DeepScanPipeline: Total Matched records = 8
16/03/11 11:06:42 INFO hadoop.DeepScanPipeline: Total matches = 32
16/03/11 11:06:42 INFO hadoop.DeepScanPipeline: Email = 32
16/03/11 11:06:42 INFO hadoop.DeepScanPipeline: IPV4 = 32
16/03/11 11:06:42 INFO hadoop.DeepScanPipeline: Street Address = 32
16/03/11 11:06:42 INFO hadoop.DeepScanPipeline: SSN-dashes = 32

```

### Sample results

```
hadoop fs -cat /user/benoy/1/scan_result0

Email	1	tests@tetts.com
SSN-dashes	1	608-34-2345
IPV4	1	123.34.456.23
Street Address	1	1234 sdsfd dr
Email	1	teret@dfdfd.com
SSN-dashes	1	604-13-4345
IPV4	1	123.33.234.13
Street Address	1	1234 gsfsffs pl
Email	1	tests@tetts.com
SSN-dashes	1	608-34-2345
IPV4	1	123.34.456.23
Street Address	1	1234 sdsfd dr
Email	1	teret@dfdfd.com
SSN-dashes	1	604-13-4345
IPV4	1	123.33.234.13
Street Address	1	1234 gsfsffs pl
Email	1	three@three.com
SSN-dashes	1	609-34-2345
IPV4	1	131.34.456.23
Street Address	1	1234 sdsfd dr
Email	1	four@four.com
SSN-dashes	1	607-13-4345
IPV4	1	153.33.234.13
Street Address	1	1234 gs pl
Email	1	three@three.com
SSN-dashes	1	609-34-2345
IPV4	1	131.34.456.23
Street Address	1	1234 sdsfd dr
Email	1	four@four.com
SSN-dashes	1	607-13-4345
IPV4	1	153.33.234.13
Street Address	1	1234 gs pl


```

### Explanation of the output

One row will be emitted for each line containing a sensitive element. If multiple types of sensitive elements are found in a column value, then a row is emitted per type per column.
The out schema is as follows:

| column position | name | description |
| --- | --- |--- | 
|1|Type| sensitive data type. eg: email, credit card. This will be the name of the finder|
|2|count| number of sensitive elements found in a specific column of a specific sensitive element type|
|3|values| comma spearated list of sensitive data values found|


### Download library jar

The latest chlorine-finder and chlorine-hadoop libraries can be downloaded [here](https://dataapps.io/chlorine.html#Opensource).

 
###Further Documentation
[chlorine-hive wiki](https://github.com/dataApps/chlorine-hadoop/wiki)
  
###Related projects
 
###Java Docs
The java docs for chlorine-finder are available [here](https://dataApps.io/files/chlorine-hadoop/javadoc/index.html).

We welcome all contributions. You can contribute features, enhancements, bug fixes or new Finders.

##Want to contribute features, bug fixes, enhancements?

    Fork it
    Create your feature branch (git checkout -b my-new-feature)
    Take a look at the issues
    Commit your changes (git commit -am 'Add some feature')
    Push to the branch (git push origin my-new-feature)
    Create new Pull Request
    
 

 
 
