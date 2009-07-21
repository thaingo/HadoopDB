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
package edu.yale.cs.hadoopdb.sms.connector;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.yale.cs.hadoopdb.catalog.Catalog;
import edu.yale.cs.hadoopdb.connector.DBChunk;
import edu.yale.cs.hadoopdb.sms.connector.SMSConfiguration;
import edu.yale.cs.hadoopdb.sms.connector.SMSInputFormat;
import edu.yale.cs.hadoopdb.sms.connector.SMSInputSplit;
import edu.yale.cs.hadoopdb.sms.connector.SMSRecordReader;

/**
 * SMSInputFormat extends FileInputFormat to allow access to path
 * information set by Hive and needed to recreate Map operators.
 *
 */
public class SMSInputFormat extends FileInputFormat<LongWritable, Text>
		implements JobConfigurable {


	public static final String DB_QUERY_SCHEMA_PREFIX = "hadoopdb.query.schema";
	public static final String DB_SQL_QUERY_PREFIX = "hadoopdb.sql.query";

	protected JobConf conf;

	public static final Log LOG = LogFactory.getLog(SMSInputFormat.class
			.getName());

	public Map<String, SMSConfiguration> rel_DBConf = new HashMap<String, SMSConfiguration>();
	public String relation = "";

	@Override
	public void configure(JobConf conf) {
		this.conf = conf;
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {
		try {
			return new SMSRecordReader((SMSInputSplit) split, conf);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Retrieves path information from FileInputFormat super class and then
	 * relation from path using the job configuration. Obtains the chunk locations
	 * from the HadoopDB catalog for the given relation and then creates a split for each chunk
	 * of a relation. The splits are provided with path, chunk and relation.
	 */
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		// This is why we need FileInputFormat!!
		Path p = getInputPaths(job)[0];
		String p_alias = p.toUri().getPath();

		LOG.debug("Processing splits for: " + p_alias);
		String relation = job.get(p_alias).toLowerCase();

		if (!rel_DBConf.containsKey(relation)) {
			LOG.debug("Generating split structure for relation: " + relation);
			SMSConfiguration smsConf = new SMSConfiguration();
			Catalog.getInstance(job).setSplitLocationStructure(smsConf, relation);
			rel_DBConf.put(relation, smsConf);
		}

		SMSConfiguration DBConf = rel_DBConf.get(relation);
		Collection<DBChunk> chunks = DBConf.getChunks();

		InputSplit[] splits = new InputSplit[chunks.size()];
		int i = 0;

		for (DBChunk chunk : chunks) {
			SMSInputSplit split = new SMSInputSplit();
			split.setPath(p);
			split.setRelation(relation);
			split.setChunk(chunk);
			splits[i] = split;
			i++;
		}
		return splits;
	}

}
