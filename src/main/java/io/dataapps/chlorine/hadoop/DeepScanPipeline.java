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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import io.dataapps.chlorine.finder.Finder;
import io.dataapps.chlorine.finder.FinderEngine;

public class DeepScanPipeline  extends AbstractPipeline{
	static final Log LOG = LogFactory.getLog(DeepScanPipeline.class);
	String scanPath;
	String jobOutputDir;
	String findersFilePath;
	long scanSince;
	String queue;

	public void run() {
		try {
			final Path fsScanPath = new Path(scanPath);
			final Configuration conf = new Configuration();
			FileSystem  fs = FileSystem.get(conf);
			String tempJobOutputDir = jobOutputDir +"_temp";
			fs.copyFromLocalFile(false, true, new Path(findersFilePath), 
					new Path("chlorine_finders.xml"));
			Job job = HDFSScanMR.makeJob(conf, fsScanPath, 
					new Path(tempJobOutputDir), scanSince, findersFilePath, queue);
			boolean bResult = runJobToCompletion(job) ;
			if (bResult) {
				LOG.info("Total bytes scanned = " +
						job.getCounters().findCounter("Feature", 
								"TotalSize").getValue());
				LOG.info("Total records scanned = " +
						job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
								"MAP_INPUT_RECORDS").getValue());
				LOG.info("Total Matched records = " +
						job.getCounters().findCounter("Feature", 
								"MatchedRecords").getValue());
				LOG.info("Total matches = " +
						job.getCounters().findCounter("Feature", 
								"TotalMatches").getValue());
				FinderEngine engine = new FinderEngine();
				for (Finder finder : engine.getFinders()) {
					long l = job.getCounters().findCounter("Feature", finder.getName()).getValue();
					if (l > 0) {
						LOG.info(finder.getName() + " = " +
								job.getCounters().findCounter("Feature", 
										"TotalMatches").getValue());
					}
				}
			}

			FileUtil.copyMerge(fs, new Path(tempJobOutputDir), fs, 
					new Path(jobOutputDir + Path.SEPARATOR + "scan_result" +scanSince), 
					true, conf, null);

		} catch ( IOException e) {
			LOG.error(e);
		}
	}
}
