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
			boolean result = (time>cutOffTime); 
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
