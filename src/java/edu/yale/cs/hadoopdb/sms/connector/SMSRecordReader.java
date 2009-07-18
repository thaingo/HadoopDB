package edu.yale.cs.hadoopdb.sms.connector;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import edu.yale.cs.hadoopdb.connector.AbstractDBRecordReader;
import edu.yale.cs.hadoopdb.sms.connector.SMSInputFormat;
import edu.yale.cs.hadoopdb.sms.connector.SMSInputSplit;
import edu.yale.cs.hadoopdb.sms.connector.SMSRecordReader;
import edu.yale.cs.hadoopdb.util.ParseSchema;

/**
 * SMSRecordReaders extends AbstractDBRecordReader and specializes the
 * value class to Text (in contrast to DBRecordReader that allows
 * arbitrary value classes). Hive expects the value to be set of field values
 * delimited by a special character {@link ParseSchema}.
 */
public class SMSRecordReader extends AbstractDBRecordReader implements RecordReader<LongWritable, Text> {

	public static final Log LOG = LogFactory.getLog(SMSRecordReader.class
			.getName());
	
	private ParseSchema parser;
	private JobConf conf;
	private SMSInputSplit split;

	/**
	 * Each relation is associated with a SQL query and schema in 
	 * the job configuration. This is retrieved and a {@link ParseSchema} object
	 * is created to retrieve the required fields from the result set
	 * and serialize them into a delimited string.
	 */
	public SMSRecordReader(SMSInputSplit split, JobConf conf) throws SQLException {
		
		this.split = split;
		this.conf = conf;
		parser = new ParseSchema(conf
				.get(SMSInputFormat.DB_QUERY_SCHEMA_PREFIX + "_"
						+ split.getRelation()));
		
		setupDB(split, conf);
	}
	

	@Override
	protected String getSqlQuery() {
		return conf.get(SMSInputFormat.DB_SQL_QUERY_PREFIX + "_" + split.getRelation());
	}


	@Override
	public Text createValue() {
		return new Text();
	}

	/**
	 * Retrieves each row from the result set, serializes it 
	 * using {@link ParseSchema} and increments the number of rows
	 * read in.
	 * @return false if no more rows exist.
	 */
	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		try {
			if (!results.next())
				return false;
			key.set(pos);
			value.set(parseResults());
			pos++;
		} catch (SQLException e) {
			throw new IOException(e);
		}
		return true;
	}

	private String parseResults() throws SQLException {
		return parser.serializeRow(results);
	}

}