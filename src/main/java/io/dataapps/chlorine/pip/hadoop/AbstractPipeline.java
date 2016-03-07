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
import org.apache.hadoop.mapreduce.Job;

public abstract class AbstractPipeline {
	static final Log LOG = LogFactory.getLog(AbstractPipeline.class);

	protected boolean runJobToCompletion(Job j) {

		LOG.info("***********Run job: " + j.getJobName());
		try {
			if (!waitForCompletion(j, true)) {
				LOG.error("Job " + j.getJobName() + " failed.");
			}
		} catch (Exception e) {
			LOG.error(e);
			return false;
		}
		LOG.info("Job " + j.getJobName() + " successful.");
		return true;
	}



	private synchronized boolean waitForCompletion(Job j, boolean verbose)
			throws IOException, InterruptedException, ClassNotFoundException {
		j.submit();
		while (!j.isComplete()) {
			try {
				wait(5000);
			} catch (InterruptedException ie) {
			}
		}
		return j.isSuccessful();
	}

	abstract public void run ();


}
