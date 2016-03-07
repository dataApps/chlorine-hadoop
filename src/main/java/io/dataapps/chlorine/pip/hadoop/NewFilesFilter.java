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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class NewFilesFilter  implements Configurable, PathFilter {
	static final Log LOG = LogFactory.getLog(NewFilesFilter.class);
	Configuration conf;
	long cutOffTime;

	@Override
	public boolean accept(Path path) {
		try {
			FileSystem  fs = FileSystem.get(conf);
			if(fs.isDirectory(path)){
				return true;
			}
			FileStatus file = fs.getFileStatus(path);
			long time = file.getModificationTime();
			boolean result =  (time>cutOffTime); 
			return result;
		} catch (IOException e) {
			LOG.error(e);
			return true;
		}
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		cutOffTime = conf.getLong("scanSince", 0);
	}

	public Configuration getConf() {
		return conf;
	}

}
