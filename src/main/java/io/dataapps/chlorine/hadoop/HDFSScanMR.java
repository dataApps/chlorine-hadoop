/*
 * Copyright 2016, DataApps Corporation (http://dataApps.io) .
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dataapps.chlorine.hadoop;

import io.dataapps.chlorine.finder.FinderEngine;
import io.dataapps.chlorine.mask.MaskFactory;
import io.dataapps.chlorine.mask.Masker;

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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Feature generation happens in the map phase. For a set of samples, generate a feature vector
 */
public class HDFSScanMR {
	static final Log LOG = LogFactory.getLog(HDFSScanMR.class);

	public static class DeepScanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private static FinderEngine engine;
		private static Masker masker;
		public static final String FIELD_DELIMITER = new String(new char[] {'\t'});
		private RecordWriter<NullWritable, Text> matchwriter;	
		private RecordWriter<NullWritable, Text> maskwriter;
		private String fullFilePath;	
		protected void setup(Context context) throws IOException, InterruptedException {

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
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();

			// extract parent folder and filename
			int input_path_depth = conf.getInt("input_path_depth", 0);
			String filenameKey = path.getName();
			Path tempPath = path.getParent();
			while ( tempPath.depth() > input_path_depth) {
				filenameKey = tempPath.getName() + Path.SEPARATOR + filenameKey;
				tempPath = tempPath.getParent();
			}
			
			fullFilePath = conf.get("inputPath") + Path.SEPARATOR + filenameKey;

			String maskPath = conf.get("maskPath");
			if (maskPath !=null) {
				masker = new MaskFactory(engine).getMasker();
				// output file name
				final Path outputFilePath = new Path(maskPath, filenameKey);

				// We need to override the getDefaultWorkFile path to stop the file being created in the _temporary/taskid folder
				TextOutputFormat<NullWritable, Text> tof = new TextOutputFormat<NullWritable, Text>() {
					@Override
					public Path getDefaultWorkFile(TaskAttemptContext context,
							String extension) throws IOException {
						return outputFilePath;
					}
				};
				// create a record writer that will write to the desired output subfolder
				maskwriter = tof.getRecordWriter(context);
			}
			final String matchPath = conf.get("matchPath");

			if (matchPath !=null) {
				// output file name
				final Path baseOutputPath = new Path(FileOutputFormat.getOutputPath(context), "_temp");

				// We need to override the getDefaultWorkFile path to stop the file being created in the _temporary/taskid folder
				TextOutputFormat<NullWritable, Text> tof = new TextOutputFormat<NullWritable, Text>() {
					@Override
					public Path getDefaultWorkFile(TaskAttemptContext context,
							String extension) throws IOException {
						return new Path(baseOutputPath, context.getTaskAttemptID().toString());
					}
				};
				// create a record writer that will write to the desired output subfolder
				matchwriter = tof.getRecordWriter(context);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			boolean matchedValue =false;
			String input = value.toString();

			if (engine == null) throw new IOException("FInderEngine is not set.");
			Map<String, List<String>> matchesByType = engine.findWithType(input);
			for (Map.Entry<String, List<String>> match: matchesByType.entrySet()) {
				matchedValue = true;
				if (matchwriter !=null) {
					StringBuilder record = new StringBuilder();
					record.append(fullFilePath);
					record.append(FIELD_DELIMITER);
					record.append(match.getKey());
					record.append(FIELD_DELIMITER);
					record.append(match.getValue().size());
					record.append(FIELD_DELIMITER);
					record.append(StringUtils.join(match.getValue(), ','));					
					matchwriter.write(NullWritable.get(), new Text(record.toString()));
				}
				context.getCounter("Feature", "TotalMatches").increment(match.getValue().size());
				context.getCounter("Feature", match.getKey()).increment(match.getValue().size());
			}
			if (matchedValue) {
				context.getCounter("Feature", "MatchedRecords").increment(1);
			}
			if (maskwriter !=null) {
				maskwriter.write(NullWritable.get(), new Text(masker.mask(input)));
			}
			context.getCounter("Feature", "TotalSize").increment(value.getLength());
		}

		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			maskwriter.close(context);
		}
	}

	public static Job makeJob(Configuration conf, Path in, Path out, String matchPath, long scanSince, 
			String chlorineConfigFilePath, String queue, String maskPath) throws IOException {
		conf.setBoolean("mapred.output.compress", false);
		conf.setLong("scanSince", scanSince);
		conf.set("matchPath", matchPath);
		conf.set("maskPath", maskPath);
		conf.set("inputPath", in.toString());
		if (queue != null) {
			conf.set("mapred.job.queue.name", queue);
		}
		conf.set("fs.permissions.umask-mode", 
				"007");
		conf.setInt("input_path_depth", in.depth());
		Job job = Job.getInstance(conf, "Chlorine_HDFS_Scan");
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
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); 
		return job;
	}
}