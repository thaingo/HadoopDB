package edu.yale.cs.hadoopdb.connector;

import org.apache.hadoop.mapred.JobConf;

/**
 * Prepares SQL query (e.g. substitute some parameters, tables names etc.) 
 *
 */
public interface SQLPreparer {

	public String prepare(String sqlQuery, DBInputSplit split, JobConf conf);
}
