package edu.yale.cs.hadoopdb.connector;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DBWritable Interface. All value classes (key-value pairs read in by MapReduce jobs
 * that access a database) need to extend DBWritable. See {@link DBRecordReader} for
 * more information.
 */

public interface DBWritable {

  /**
   * Sets the fields of the object in the {@link PreparedStatement} (not used currently).
   * @param statement the statement that the fields are put into.
   * @throws SQLException
   */
	public void write(PreparedStatement statement) throws SQLException;
	
	/**
	 * Reads the fields of the object from the {@link ResultSet}. 
	 * @param resultSet the {@link ResultSet} to get the fields from.
	 * @throws SQLException
	 */
	public void readFields(ResultSet resultSet) throws SQLException ; 
	
}
