/**
 * Copyright (C) DataApps Corporation - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is 
 * strictly prohibited.
 * Proprietary and confidential.
 * Written by Benoy Antony <benoy@DataApps.io>, 2015
 */
package io.dataapps.chlorine.pip.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import io.dataapps.chlorine.finder.FinderEngine;

/**
 * Feature generation happens in the map phase. For a set of samples, generate a feature vector
 */
public class HDFSScanMR {
	static final Log LOG = LogFactory.getLog(HDFSScanMR.class);

	public static class DeepScanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private static FinderEngine engine;
		public static final String FIELD_DELIMITER = new String(new char[] {'\t'});
		protected void setup(Context context) throws IOException {

			//print class path
			ClassLoader cl = ClassLoader.getSystemClassLoader();
			URL[] urls = ((URLClassLoader)cl).getURLs();
			for(URL url: urls){
				LOG.info((url.getFile()));
			}
			Configuration conf  = context.getConfiguration();
			if (conf.get("finder_file") != null) {
				engine = new FinderEngine(conf.get("finder_file"));
			} else {
				engine = new FinderEngine();
			}

		}

		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			boolean matchedValue =false;

			if (engine == null) throw new IOException("FInderEngine is not set.");
			Map<String, List<String>> matchesByType = engine.findWithType(value.toString());
			for (Map.Entry<String, List<String>> match: matchesByType.entrySet()) {
				matchedValue = true;
				StringBuilder record = new StringBuilder();
				record.append(match.getKey());
				record.append(FIELD_DELIMITER);
				record.append(match.getValue().size());
				record.append(FIELD_DELIMITER);
				record.append(StringUtils.join(match.getValue(), ','));					
				context.write(NullWritable.get(), new Text(record.toString()));
				context.getCounter("Feature", "TotalMatches").increment(match.getValue().size());
				context.getCounter("Feature", match.getKey()).increment(match.getValue().size());
			}
			if (matchedValue) {
				context.getCounter("Feature", "MatchedRecords").increment(1);
			}
			context.getCounter("Feature", "TotalSize").increment(value.getLength());
		}
	}

	public static Job makeJob(Configuration conf, Path in, Path out, long scanSince, String chlorineConfigFilePath, String queue) throws IOException {
		conf.setBoolean("mapred.output.compress", false);
		conf.setLong("scanSince", scanSince);
		if (queue != null) {
			conf.set("mapred.job.queue.name", queue);
		}
		conf.set("fs.permissions.umask-mode", 
				"007");
		Job job = Job.getInstance(conf, "HDFSScan");
		job.setJarByClass(HDFSScanMR.class);
		if (chlorineConfigFilePath != null) {
			try {
				job.addCacheFile(new URI(chlorineConfigFilePath));
				conf.set("finder_file", (new File(chlorineConfigFilePath)).getName());
			} catch (URISyntaxException e) {
				LOG.error(e);
			}
		}
		job.setMapperClass(DeepScanMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, in);
		TextInputFormat.setInputDirRecursive(job, true);
		TextInputFormat.setInputPathFilter(job, NewFilesFilter.class);
		FileOutputFormat.setOutputPath(job, out);
		return job;
	}
}