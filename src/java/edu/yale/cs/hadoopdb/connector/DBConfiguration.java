package edu.yale.cs.hadoopdb.connector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;

import edu.yale.cs.hadoopdb.catalog.BaseDBConfiguration;

/**
 * Extends BaseDBConfiguration class for MapReduce jobs that connect to databases. Assumes
 * a single SQL query which is read in from the job configuration file. This is set by the
 * HadoopDB job. Also, specifies the value class needed to recreate the value object from the
 * records read in.
 */
public class DBConfiguration extends BaseDBConfiguration {
	
	public static final Log LOG = LogFactory.getLog(DBConfiguration.class
			.getName());
	

	private JobConf jobConf;

	private String sqlQuery;
	@SuppressWarnings("unchecked")
	private Class valueClass;	


	public DBConfiguration() {
		super();	
	}

	public DBConfiguration(JobConf jobConf) {
		this.jobConf = jobConf;
	}

	
	public JobConf getJobConf() {
		return jobConf;
	}

	public void setJobConf(JobConf jobConf) {
		this.jobConf = jobConf;
	}

	
	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	@SuppressWarnings("unchecked")
	public Class getValueClass() {
		return valueClass;
	}

	@SuppressWarnings("unchecked")
	public void setValueClass(Class valueClass) {
		this.valueClass = valueClass;
	}

	
	
}
