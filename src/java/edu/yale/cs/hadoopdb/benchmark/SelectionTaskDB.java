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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.connector.DBConst;
import edu.yale.cs.hadoopdb.connector.DBWritable;
import edu.yale.cs.hadoopdb.exec.DBJobBase;
import edu.yale.cs.hadoopdb.util.HDFSUtil;

/**
 * HadoopDB's implementation of Selection Task
 * http://database.cs.brown.edu/projects/mapreduce-vs-dbms/
 */
public class SelectionTaskDB extends DBJobBase {

	public static final String PAGE_RANK_VALUE_PARAM = "page.rank.value";
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SelectionTaskDB(), args);
		System.exit(res);
	}	

	@Override
	protected JobConf configureJob(String... args) throws Exception {
		
		JobConf conf = new JobConf(this.getClass());
		conf.setJobName("selection_db");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(Map.class);
		conf.setNumReduceTasks(0);

		if (args.length < 2) {
			throw new RuntimeException("Incorrect arguments provided for "
					+ this.getClass());
		}

		conf.set(PAGE_RANK_VALUE_PARAM, args[0]);

		// OUTPUT properties
		Path outputPath = new Path(args[1]);
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
				
		conf.set(DBConst.DB_RELATION_ID, "Rankings");
		conf.set(DBConst.DB_RECORD_READER, RankingsRecord.class.getName());
		conf.set(DBConst.DB_SQL_QUERY,
				"SELECT pageURL, pageRank FROM Rankings " +
				"WHERE pageRank > " + conf.get(PAGE_RANK_VALUE_PARAM) + ";");		
		
		return conf;
	}

	@Override
	protected int printUsage() {
		System.out.println("<page_rank_value> <output_dir>");
		return -1;
	}

	static class Map extends MapReduceBase implements
			Mapper<LongWritable, RankingsRecord, Text, IntWritable> {

		protected Text outputKey = new Text();
		protected IntWritable outputValue = new IntWritable();		

		public void map(LongWritable key, RankingsRecord value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
				
			outputKey.set(value.getPageURL());
			outputValue.set(value.getPageRank());
			output.collect(outputKey, outputValue);

		}	
	}

	static class RankingsRecord implements DBWritable {
		private String pageURL;
		private int pageRank;

		public String getPageURL() {
			return pageURL;
		}

		public int getPageRank() {
			return pageRank;
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.pageURL = resultSet.getString("pageURL");
			this.pageRank = resultSet.getInt("pageRank");
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			throw new UnsupportedOperationException("No write() impl.");
		}
	}

}
