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
import org.apache.hadoop.mapreduce.Job;

public abstract class AbstractPipeline {
	static final Log LOG = LogFactory.getLog(AbstractPipeline.class);

	protected boolean runJobToCompletion(Job j) {

		LOG.info("***********Run job: " + j.getJobName());
		try {
			if (!waitForCompletion(j, true)) {
				LOG.error("Job " + j.getJobName() + " failed.");
				return false;
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
