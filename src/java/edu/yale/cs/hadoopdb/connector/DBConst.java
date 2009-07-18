package edu.yale.cs.hadoopdb.connector;

/**
 * Constants and property names used by HadoopDB 
 */
public interface DBConst {

	public static final String DB_RECORD_READER = "hadoopdb.record.reader";
	public static final String DB_SQL_QUERY = "hadoopdb.sql.query";
	public static final String DB_RELATION_ID = "hadoopdb.relation.id";
	public static final String DB_FETCH_SIZE = "hadoopdb.fetch.size";
	public static final String DB_CONFIG_FILE = "hadoopdb.config.file";
	public static final String DB_REPLICATION = "hadoopdb.config.replication";
	/**
	 * Required for large datasets in MySQL
	 */
	public static final int SQL_STREAMING_FETCH_SIZE = Integer.MIN_VALUE;
	/**
	 * Limits the number of rows fetched from a database at once
	 */
	public static final int SQL_DEFAULT_FETCH_SIZE = 1000;
	

}
