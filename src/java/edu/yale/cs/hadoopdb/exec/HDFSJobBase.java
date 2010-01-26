/**
 * Copyright 2009 HadoopDB Team (http://db.cs.yale.edu/hadoopdb/hadoopdb.html)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.yale.cs.hadoopdb.exec;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HDFSJobBase is an abstract class for MapReduce jobs that read data from HDFS.
 * Extensions need to configure jobs by implementing
 * configure and implementing necessary Map/Reduce functions.
 */
public abstract class HDFSJobBase extends Configured implements Tool {

	public static final Log LOG = LogFactory.getLog(HDFSJobBase.class.getName());
	
	/**
	 * Override this method to set job-specific options
	 */
	protected abstract JobConf configureJob(String... args) throws Exception;

	public int run(String[] args) throws Exception {
		
		long startTime = System.currentTimeMillis();
		
		List<String> other_args = new ArrayList<String>();
		
		for (int i = 0; i < args.length; ++i) {
			other_args.add(args[i]);
		}

		JobConf conf = null;
		try {
			conf = configureJob(other_args.toArray(new String[0]));
		}
		catch (Exception e) {
			System.err.print("ERROR: " + StringUtils.stringifyException(e));
			return printHDFSUsage();
		}

		LOG.info(conf.getJobName());
		JobClient.runJob(conf);
		
		long endTime = System.currentTimeMillis();
		LOG.info("\nJOB TIME : " + (endTime - startTime)
				+ " ms.\n");
		
		return 0;
	}

	/**
	 * Provide job-specific command-line help
	 */	
	protected abstract int printUsage();

	public int printHDFSUsage() {
		printUsage();
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
}
