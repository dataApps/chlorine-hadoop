# chlorine-hadoop
*Detect sensitive data in Hadoop*

The chlorine-hadoop library defines a mapreduce program which can be used to detect and mask sensitive elements in HDFS directoires. It is Java based.

Chlorine-hadoop can detect different types of Credit card numbers, SSN, Phone Numbers, email adddresses, Ip Addresses, Street Addresses in HDFS datasets. It works with compressed text files. Optionally, it can mask the sensitive elements detected.


###To Download source code

*git clone https://github.com/dataApps/chlorine-hadoop.git*

###To build chlorine-hadoop

*mvn install*


###To develop chlorine-hadoop in eclipse

*mvn eclipse:clean eclipse:eclipse*

Then import the projects into the eclipse workspace.

###To use Chlorine-hadoop

```
usage: Scan [-help] [-i <path>] [-inc <file>] [-m <path>] [-o <path>] [-q
       <name>] [-s <timeinms>] [-sm <path>]
 -help                       print this message
 -i,--input_path <path>      input path to scan
 -inc,--incremental <file>   specify scan as incremental and use the
                             timestemp in the file to determine the files
                             to scan. If the file is present, the
                             timestamp will be read from the file. If file
                             is not present, file is automatically
                             geneated and updated with a timestamp for
                             subsequent scans. If both incremental and
                             scanfrom are specified, then incremental is
                             ignored.
 -m,--mask <path>            Copy the input to the specified path with
                             sensitive values masked. The directory
                             structure is retained.
 -o,--output_path <path>     Output path
 -q,--queue <name>           job queue
 -s,--scanfrom <timeinms>    Scan only files modified on or after the
                             specific time. The time is specified in
                             milliseconds after the epoch.
 -sm,--save_match <path>     path to save matches
```

## A sample script to run chlorine-hadoop job on an HDFS directory
 
The script template is available under  [/scripts/chlorine-hadoop.sh](https://github.com/dataApps/chlorine-hadoop/scripts/chlorine-hadoop.sh)

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
./scripts/chlorine-hadoop.sh -i /data/csv_nohive -o /user/benoy/23 -sm /user/benoy/matches -q regular -m /user/benoy/data

Creating a DeepScan with the following inputs
input-path=/data/csv_nohive
output-path=/user/benoy/23
queue=regular
save Matches at=/user/benoy/matches
Mask and save masked files at=/user/benoy/data
16/04/22 10:50:04 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
16/04/22 10:50:05 INFO Configuration.deprecation: mapred.output.compress is deprecated. Instead, use mapreduce.output.fileoutputformat.compress
16/04/22 10:50:05 INFO Configuration.deprecation: mapred.job.queue.name is deprecated. Instead, use mapreduce.job.queuename
16/04/22 10:50:05 INFO hadoop.AbstractPipeline: ***********Run job: Chlorine Scan and Mask
16/04/22 10:50:05 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
16/04/22 10:50:05 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
16/04/22 10:50:05 INFO input.FileInputFormat: Total input paths to process : 2
16/04/22 10:50:05 INFO mapreduce.JobSubmitter: number of splits:2
16/04/22 10:50:06 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1461174733991_0029
16/04/22 10:50:06 INFO impl.YarnClientImpl: Submitted application application_1461174733991_0029
16/04/22 10:50:06 INFO mapreduce.Job: The url to track the job: http://benoy-Satellite-C55-C:8088/proxy/application_1461174733991_0029/
16/04/22 10:50:21 INFO hadoop.AbstractPipeline: Job Chlorine Scan and Mask successful.
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: Total bytes scanned = 448
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: Total records scanned = 4
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: Total Matched records = 4
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: Total matches = 16
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: Email = 16
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: IPV4 = 16
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: StreetAddress = 16
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: SSN-dashes = 16
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: The matches detected are stored in /user/benoy/matches/scan_result_-996967296_-1
16/04/22 10:50:21 INFO hadoop.DeepScanPipeline: The matches in the input are masked and a copy is kept under /user/benoy/data

```

### Sample results

```
hadoop fs -cat /user/benoy/1/scan_result0

/data/csv_nohive/2015/08/31/user1.csv	Email	1	tests@tetts.com
/data/csv_nohive/2015/08/31/user1.csv	SSN-dashes	1	608-34-2345
/data/csv_nohive/2015/08/31/user1.csv	IPV4	1	123.34.456.23
/data/csv_nohive/2015/08/31/user1.csv	StreetAddress	1	1234 sdsfd dr
/data/csv_nohive/2015/08/31/user1.csv	Email	1	teret@dfdfd.com
/data/csv_nohive/2015/08/31/user1.csv	SSN-dashes	1	604-13-4345
/data/csv_nohive/2015/08/31/user1.csv	IPV4	1	123.33.234.13
/data/csv_nohive/2015/08/31/user1.csv	StreetAddress	1	1234 gsfsffs pl
/data/csv_nohive/2015/09/01/user2.csv	Email	1	three@three.com
/data/csv_nohive/2015/09/01/user2.csv	SSN-dashes	1	609-34-2345
/data/csv_nohive/2015/09/01/user2.csv	IPV4	1	131.34.456.23
/data/csv_nohive/2015/09/01/user2.csv	StreetAddress	1	1234 sdsfd dr
/data/csv_nohive/2015/09/01/user2.csv	Email	1	four@four.com
/data/csv_nohive/2015/09/01/user2.csv	SSN-dashes	1	607-13-4345
/data/csv_nohive/2015/09/01/user2.csv	IPV4	1	153.33.234.13
/data/csv_nohive/2015/09/01/user2.csv	StreetAddress	1	1234 gs pl


```

### Explanation of the output

One row will be emitted for each line containing a sensitive element. If multiple types of sensitive elements are found in a column value, then a row is emitted per type per column.
The out schema is as follows:

| column position | name | description |
| --- | --- |--- | 
|1|Source File Path| Path to the file which contains the sensitive data|
|2|Type| sensitive data type. eg: email, credit card. This will be the name of the finder|
|3|count| number of sensitive elements found in a specific column of a specific sensitive element type|
|4|values| comma spearated list of sensitive data values found|


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
    
 

 
 
