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
