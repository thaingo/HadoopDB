package edu.yale.cs.hadoopdb.connector;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Implementation of DBRecordReader for HadoopDB jobs. Records returned include a key (meaningless long) and an value object.
 * All jobs need to specify this class.
 * @param <T>
 */
public class DBRecordReader<T extends DBWritable> extends AbstractDBRecordReader implements RecordReader<LongWritable, T> {
	
	public static final Log LOG = LogFactory.getLog(DBRecordReader.class.getName());
	
	private Class<T> valueClass;
	private JobConf conf;
	private DBConfiguration dbConf;

	/**
	 * Constructor requires DBConfiguration to set up DB connection.
	 */
	@SuppressWarnings("unchecked")
	public DBRecordReader(DBConfiguration dbConf, DBInputSplit split, 
			JobConf conf) throws SQLException {
		
		this.dbConf = dbConf; 
		this.valueClass = dbConf.getValueClass();
		this.conf = conf;

		setupDB(split, conf);
		
	}
	
	/**
	 *  Provides a SQL query
	 */
	@Override
	protected String getSqlQuery() {
		return dbConf.getSqlQuery();
	}

	@Override
	public T createValue() {
		return ReflectionUtils.newInstance(valueClass, conf);
	}

	/**
	 * Reads the next record from the result set and passes the result set to the value Object to
	 * extract necessary fields. Increments the number of rows read in.
	 * @return false if no more rows exist.
	 */
	@Override
	public boolean next(LongWritable key, T value) throws IOException {
		try {
			if (!results.next())
				return false;
			
			key.set(pos);
			value.readFields(results);

			pos++;
		} catch (SQLException e) {
			throw new IOException(e);
		}
		return true;
	}

}