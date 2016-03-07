/**
 * Copyright (C) DataApps Corporation - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is 
 * strictly prohibited.
 * Proprietary and confidential.
 * Written by Benoy Antony <benoy@DataApps.io>, 2015
 */
package io.dataapps.chlorine.pip.hadoop;

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
