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

	public DeepScanPipeline(String scanPath, String jobOutputDir, String matchPath, String queue, long scanSince, String maskPath) {
		super();
		this.scanPath = scanPath;
		this.jobOutputDir = jobOutputDir;
		this.queue = queue;
		this.scanSince = scanSince;
		this.maskPath = maskPath;
		this.matchPath = matchPath;
	}

	static final Log LOG = LogFactory.getLog(DeepScanPipeline.class);
	String scanPath;
	String jobOutputDir;
	String findersFilePath;
	long scanSince;
	String queue;
	String maskPath;
	String matchPath;


	public void run() {
		try {
			final Path fsScanPath = new Path(scanPath);
			final Configuration conf = new Configuration();
			FileSystem  fs = FileSystem.get(conf);
			if (findersFilePath!= null) {
				fs.copyFromLocalFile(false, true, new Path(findersFilePath), 
						new Path("chlorine_finders.xml"));
			}
			Job job = HDFSScanMR.makeJob(conf, fsScanPath, 
					new Path(jobOutputDir), matchPath, scanSince, findersFilePath, queue, maskPath);
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
				if (matchPath != null) {
					String tempMatchesPath = jobOutputDir + Path.SEPARATOR + "_temp";
					String matchOutputPath = matchPath + Path.SEPARATOR + "scan_result_" + 
							scanPath.hashCode() + "_" + scanSince;
					FileUtil.copyMerge(fs, new Path(tempMatchesPath), fs, new Path(matchOutputPath), true, conf, null);
					LOG.info("The matches detected are stored in " + matchOutputPath);
				}
				
				if (maskPath != null) {
					LOG.info("The matches in the input are masked and a copy is kept under " + maskPath);

				}
			}

		} catch ( IOException e) {
			LOG.error(e);
		}
	}
}
