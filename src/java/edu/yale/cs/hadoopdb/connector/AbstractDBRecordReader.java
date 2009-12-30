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
package edu.yale.cs.hadoopdb.connector;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;

/**
 * Base DBRecordReader class. Several extensions of this class are possible to
 * allow different constructors and SQL query specification through abstract
 * method getSqlQuery(). Class implements all methods needed by a Hadoop's
 * RecordReader interface except for next().
 * 
 */
public abstract class AbstractDBRecordReader {

	public static final Log LOG = LogFactory
			.getLog(AbstractDBRecordReader.class.getName());
	/**
	 * Maximum number of connection trials
	 */
	public static final int MAX_CONNECTION_TRIALS = 10;

	protected Connection connection;
	protected ResultSet results;
	protected Statement statement;

	protected long pos = 0;

	protected long startTime = 0;
	protected long connTime = 0;
	protected long queryTime = 0;

	/**
	 * Helper method to retrieve local host name or null if not possible
	 */
	private static String getLocatHostAddres() {
		try {
			return InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			return null;
		}
	}

	/**
	 * Abstract method definition. All extensions need to provide a sql query
	 * necessary for retrieving rows from the database.
	 * 
	 * @return String standard SQL query
	 */
	protected abstract String getSqlQuery();

	/**
	 * Method sets up a connection to a database and provides query optimization
	 * parameters. Then it executes the query.
	 */
	protected void setupDB(DBInputSplit split, JobConf conf)
			throws SQLException {

		try {
			startTime = System.currentTimeMillis();
			connection = getConnection(split);
			// Optimization options including specifying forward direction,
			// read-only cursor
			// and a default fetch size to prevent db cache overloading.
			statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			connection.setAutoCommit(false);
			statement.setFetchDirection(ResultSet.FETCH_FORWARD);
			statement.setFetchSize(conf.getInt(DBConst.DB_FETCH_SIZE,
					DBConst.SQL_DEFAULT_FETCH_SIZE));

			connTime = System.currentTimeMillis();
			LOG.info(getSqlQuery());
			results = statement.executeQuery(getSqlQuery());
			queryTime = System.currentTimeMillis();

		} catch (SQLException e) {

			try {
				if (results != null)
					results.close();
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException ex) {
				LOG.info(ex, ex);
			}

			throw e;
		}
	}

	/**
	 * Connects to a database of a particular chunk (specified within the
	 * split). If a particular host fails during connection, it is avoided and
	 * another host is found. The method fails after a set number of maximum
	 * connection trials.
	 */
	protected Connection getConnection(DBInputSplit dbSplit) {

		boolean connected = false;
		DBChunkHost avoid_host = null;
		int connect_tries = 0;
		Connection connection = null;

		String localHostAddr = getLocatHostAddres();
		DBChunk chunk = dbSplit.getChunk();
		DBChunkHost chunk_host = null;

		while (!connected) {
			if (!chunk.getLocations().contains(localHostAddr)) {
				LOG.info("Data locality failed for " + localHostAddr);
				chunk_host = chunk.getAnyHost(avoid_host);
			} else {
				if (avoid_host != chunk.getHost(localHostAddr))
					chunk_host = chunk.getHost(localHostAddr);
			}
			LOG.info("Task from " + localHostAddr + " is connecting to chunk "
					+ chunk.getId() + " on host " + chunk_host.getHost()
					+ " with db url " + chunk_host.getUrl());

			try {
				Class.forName(chunk_host.getDriver());
				connection = DriverManager.getConnection(chunk_host.getUrl(),
						chunk_host.getUser(), chunk_host.getPassword());
				connected = true;
			} catch (Exception e) {
				if (connect_tries < MAX_CONNECTION_TRIALS) {
					connect_tries++;
					avoid_host = chunk_host;
					chunk_host = null;
				} else
					throw new RuntimeException(e);
			}
		}
		return connection;
	}

	/**
	 * After query execution is complete, the database connection is closed
	 * cleanly.
	 */
	public void close() throws IOException {
		try {
			results.close();
			statement.close();
			connection.close();
			long endTime = System.currentTimeMillis();
			LOG.info("DB times (ms): connection = " + (connTime - startTime)
					+ ", query execution = " + (queryTime - connTime)
					+ ", row retrieval  = " + (endTime - queryTime));
			LOG.info("Rows retrieved = " + getPos());

		} catch (SQLException e) {
			LOG.debug("Error while closing JDBC.", e);
			throw new IOException(e);
		}

	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	/**
	 * Returns the number of rows retrieved so far. This value is updated by
	 * record reader sub-classes.
	 */
	public long getPos() throws IOException {
		return pos;
	}

	/**
	 * Returns a float [0,1] indicating progress (currently, progress is always
	 * 0 as there is no easy way for progress estimation).
	 */
	public float getProgress() throws IOException {
		return 0;
	}

}
