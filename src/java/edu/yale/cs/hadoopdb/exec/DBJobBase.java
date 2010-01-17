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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.connector.DBConst;
import edu.yale.cs.hadoopdb.connector.DBInputFormat;
import edu.yale.cs.hadoopdb.connector.DBWritable;

/**
 * DBJobBase is an abstract class for MapReduce jobs that read from a
 * single HadoopDB relation partitioned across multiple databases. It reads in a
 * partition configuration file and retrieves all information about the
 * distribution of the relation and sets up appropriate connections
 * depending on the replication factor setup. Extensions need to
 * configure jobs by implementing configure and implementing necessary
 * Map/Reduce functions.
 */
public abstract class DBJobBase extends Configured implements Tool {

	public static final Log LOG = LogFactory.getLog(DBJobBase.class.getName());

	
	/**
	 * Override this method to set job-specific options
	 */
	protected abstract JobConf configureJob(String... args) throws Exception;


	/**
	 * Job config initialization (command-line params etc).  
	 */
	protected JobConf initConf(String[] args) throws Exception {

		List<String> other_args = new ArrayList<String>();

		Path configuration_file = null;
		boolean replication = false;
		

		for (int i = 0; i < args.length; ++i) {
			if (("-"+DBConst.DB_CONFIG_FILE).equals(args[i])) {
				configuration_file = new Path(args[++i]);
			} else if ("-replication".equals(args[i])) {
				replication = true;
			} else {
				other_args.add(args[i]);
			}
		}
		
		JobConf conf = null;

		conf = configureJob(other_args.toArray(new String[0]));
		LOG.info(conf.get(DBConst.DB_SQL_QUERY));

		if (conf.get(DBConst.DB_RELATION_ID) == null || conf.get(DBConst.DB_SQL_QUERY) == null
				|| conf.get(DBConst.DB_RECORD_READER) == null) {
			throw new Exception(
					"ERROR: DB Job requires a relation, an SQL Query and a Record Reader class to be configured.\n"
							+ "Please specify using: conf.set(\"" + DBConst.DB_RELATION_ID + "\", <relation name>), conf.set(\"" + DBConst.DB_SQL_QUERY + "\", <SQL QUERY>)\n"
							+ "and code an appropriate Record Reader and specify conf.set(\"" + DBConst.DB_RECORD_READER + "\", <Record reader class name>)\n");
		}
		
		if(replication) {
			conf.setBoolean(DBConst.DB_REPLICATION, true);
		}

		if (configuration_file == null) {
			if(conf.get(DBConst.DB_CONFIG_FILE) == null) {
				throw new Exception("No HadoopDB config file!");
			}
		}
		else {
			conf.set(DBConst.DB_CONFIG_FILE, configuration_file.toString());
		}				

		setInputFormat(conf);

		return conf;
	}
	
	/**
	 * Sets an input format (DBJobBaseInputFormat by default)
	 */
	protected void setInputFormat(JobConf conf) {
		conf.setInputFormat(DBJobBaseInputFormat.class);
	}

	public int run(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		JobConf conf = null;
		try {
			conf = initConf(args);
		} catch (Exception e) {
			System.err.print("ERROR: " + StringUtils.stringifyException(e));
			return printDbUsage();
		}
		JobClient.runJob(conf);

		long endTime = System.currentTimeMillis();
		LOG.info("\nJOB TIME : " + (endTime - startTime) + " ms.\n");

		return 0;
	}
	
	protected static class DBJobBaseInputFormat extends
			DBInputFormat<DBWritable> {
		
		@Override
		public void configure(JobConf conf) {
			super.configure(conf);
			long startTime = System.currentTimeMillis();
			dbConf.setSqlQuery(conf.get(DBConst.DB_SQL_QUERY));
			conf.setInt(DBConst.DB_FETCH_SIZE, conf.getInt(DBConst.DB_FETCH_SIZE,
					DBConst.SQL_DEFAULT_FETCH_SIZE));
			try {
				dbConf.setValueClass(Class.forName(conf.get(DBConst.DB_RECORD_READER)));
			} catch (ClassNotFoundException e) {
				LOG.error("No RecordReader class specified.", e);
			}			

			long endTime = System.currentTimeMillis();
			LOG.debug(DBJobBaseInputFormat.class.getName() + ".configure() time (ms): "
					+ (endTime - startTime));
		}
	}

	/**
	 * Provide job-specific command-line help
	 */
	protected abstract int printUsage();

	public int printDbUsage() {
		printUsage();
		System.out
				.println("-" + DBConst.DB_CONFIG_FILE + " <xml catalog file> [-replication]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
}
