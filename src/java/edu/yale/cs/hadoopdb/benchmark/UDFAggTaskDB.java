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
package edu.yale.cs.hadoopdb.benchmark;



import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.connector.DBConst;
import edu.yale.cs.hadoopdb.connector.DBWritable;
import edu.yale.cs.hadoopdb.exec.DBJobBase;
import edu.yale.cs.hadoopdb.util.HDFSUtil;

/**
 * HadoopDB's implementation of UDF Aggregation Task
 * http://database.cs.brown.edu/projects/mapreduce-vs-dbms/
 */
public class UDFAggTaskDB extends DBJobBase {

	public static final String URL_PATTERN_STR = "<a href=\"http://([\\w./\\d]+\\.html)\">link</a>";

	protected static final Pattern URL_PATTERN = Pattern.compile(
			URL_PATTERN_STR, Pattern.CASE_INSENSITIVE);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new UDFAggTaskDB(), args);
		System.exit(res);
	}

	@Override
	protected JobConf configureJob(String... args) throws IOException {

		JobConf conf = new JobConf(this.getClass());
		conf.setJobName("udf_agg_db");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(Query4Map.class);
		conf.setCombinerClass(LongSumReducer.class);
		conf.setReducerClass(LongSumReducer.class);
		conf.setOutputFormat(TextOutputFormat.class);

		if (args.length < 1) {
			throw new RuntimeException("Incorrect arguments provided for "
					+ this.getClass());
		}

		// OUTPUT properties
		Path outputPath = new Path(args[0]);
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.set(DBConst.DB_RELATION_ID, "Documents");
		conf.set(DBConst.DB_RECORD_READER, DocumentRecord.class.getName());
		conf.set(DBConst.DB_SQL_QUERY, "SELECT url, contents FROM Documents;");

		return conf;

	}

	public static class Query4Map extends MapReduceBase implements
			Mapper<LongWritable, DocumentRecord, Text, LongWritable> {

		Text outputUrl = new Text();
		LongWritable outputPageRank1 = new LongWritable(1);

		public void map(LongWritable key, DocumentRecord value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {

			String input = value.getContents();
			Matcher m = URL_PATTERN.matcher(input);

			while (m.find()) {
				String url = input.substring(m.start(1), m.end(1));

				outputUrl.set(url);
				output.collect(outputUrl, outputPageRank1);
			}
		}
	}

	@Override
	protected int printUsage() {
		System.out.println("<output_dir>");
		return -1;
	}

	static class DocumentRecord implements DBWritable {
		private String url;
		private String contents;

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public String getContents() {
			return contents;
		}

		public void setContents(String contents) {
			this.contents = contents;
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.url = resultSet.getString("url");
			this.contents = resultSet.getString("contents");
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			throw new UnsupportedOperationException("No write() impl.");
		}
	}

}
