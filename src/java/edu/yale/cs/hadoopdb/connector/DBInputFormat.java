package edu.yale.cs.hadoopdb.connector;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.yale.cs.hadoopdb.catalog.Catalog;

/**
 * Base DBInputFormat class. Extensions required to specialize value class.
 * @param <T>
 */
public abstract class DBInputFormat<T extends DBWritable> implements
		InputFormat<LongWritable, T>, JobConfigurable {
	
	protected DBConfiguration dbConf;

	/**
	 * Method necessary for JobConfigurable interface.
	 * We allow different extensions to utilize different information
	 * from the Hadoop JobConf object.
	 */
	@Override
	public void configure(JobConf conf) {		
		dbConf = new DBConfiguration();
	}
	
	/**
	 * Returns DBRecordReader for a given split.
	 */
	@Override
	public RecordReader<LongWritable, T> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {		
		try {
			return new DBRecordReader<T>(dbConf, (DBInputSplit) split, job);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Retrieves the location of chunks for a given
	 * relation. Then, it creates as many splits as the number of chunks. Each split is assigned
	 * a chunk (which holds connection and location information).
	 */
	@Override
	public InputSplit[] getSplits(JobConf conf, int numSplits)
			throws IOException {

		Catalog.getInstance(conf).setSplitLocationStructure(dbConf, conf.get(DBConst.DB_RELATION_ID));
		Collection<DBChunk> chunks = dbConf.getChunks();
		InputSplit[] splits = new InputSplit[chunks.size()];

		int i = 0;
		for (DBChunk chunk : chunks) {
			DBInputSplit split = new DBInputSplit();
			split.setChunk(chunk);
			split.setRelation(dbConf.getRelation());

			splits[i] = split;
			i++;
		}

		return splits;
	}

}
