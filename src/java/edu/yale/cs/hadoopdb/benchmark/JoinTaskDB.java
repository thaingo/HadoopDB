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
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.connector.DBConst;
import edu.yale.cs.hadoopdb.connector.DBWritable;
import edu.yale.cs.hadoopdb.exec.DBJobBase;
import edu.yale.cs.hadoopdb.util.BenchmarkUtils;
import edu.yale.cs.hadoopdb.util.HDFSUtil;

/**
 * HadoopDB's implementation of Join Task
 * http://database.cs.brown.edu/projects/mapreduce-vs-dbms/
 */
public class JoinTaskDB extends DBJobBase {
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new JoinTaskDB(), args);
		System.exit(res);
	}

	static class JoinRecord implements DBWritable {
		private String sourceIP;
		private double totalRevenue;
		private int sumPageRank;
		private int countPageRank;

		public String getSourceIP() {
			return sourceIP;
		}

		public int getSumPageRank() {
			return sumPageRank;
		}

		public double getTotalRevenue() {
			return totalRevenue;
		}

		public int getCountPageRank() {
			return countPageRank;
		}
		
		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.sourceIP = resultSet.getString("sourceIP");
			this.sumPageRank = resultSet.getInt("sumPageRank");
			this.countPageRank = resultSet.getInt("countPageRank");
			this.totalRevenue = resultSet.getDouble("totalRevenue");
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			throw new UnsupportedOperationException("Unimplemented method write() called");
		}
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, JoinRecord, Text, Text> {
		
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		@Override
		public void map(LongWritable dummy, JoinRecord record, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			outputKey.set(record.getSourceIP());
			outputValue.set(record.getTotalRevenue() + BenchmarkUtils.DELIMITER + record.getSumPageRank() + BenchmarkUtils.DELIMITER + record.getCountPageRank());
			output.collect(outputKey, outputValue);
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private OutputCollector<Text, Text> foutput;
		private double maxRevenue = Double.MIN_VALUE;
		private String sourceIP;
		private double avgPageRank;
		@Override
		public void reduce(Text key, Iterator<Text> fields, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int totalPageRank = 0, countPageRank = 0;
			double totalrev = 0;
			while (fields.hasNext()) {
				String[] values = BenchmarkUtils.DELIMITER_PATTERN.split(fields.next().toString());
				totalrev += Double.parseDouble(values[0]);
				totalPageRank += Integer.parseInt(values[1]);
				countPageRank += Integer.parseInt(values[2]);
			}
			foutput = output;
			if(totalrev > maxRevenue){
				maxRevenue = totalrev;
				sourceIP = key.toString();
				avgPageRank = (double)totalPageRank / (double)countPageRank;
			}
		}
		
		@Override
		public void close() throws IOException {
			foutput.collect(new Text(sourceIP), new Text(maxRevenue + BenchmarkUtils.DELIMITER + avgPageRank));
			super.close();
		}
	}
	
	
	@Override
	protected JobConf configureJob(String... args) throws Exception {
		JobConf conf = new JobConf(JoinTaskDB.class);
		conf.setJobName("join_db");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(1);  // Because we look for 1 TOP value

		// join arguments
		conf.setOutputFormat(TextOutputFormat.class);
		for (int i = 0; i < args.length; ++i) {
			if ("-date_l".equals(args[i]))
				conf.set("date_l", args[++i]);
			else if ("-date_u".equals(args[i]))
				conf.set("date_u", args[++i]);
			else if("-output".equals(args[i]))
				conf.set("output", args[++i]);
		}

		// OUTPUT properties
		Path outputPath = new Path(conf.get("output"));
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);


		conf.set(DBConst.DB_RELATION_ID, "UserVisits");
		conf.set(DBConst.DB_RECORD_READER, JoinRecord.class.getName());
	
		String TABLE_R = "Rankings";
		String TABLE_UV = "UserVisits";

		conf.set(DBConst.DB_SQL_QUERY, 
					"SELECT sourceIP, SUM(pageRank) as sumPageRank, COUNT(pageRank) as countPageRank, SUM(adRevenue) as totalRevenue " +
					"FROM " + TABLE_R + " AS R, " + TABLE_UV + " AS UV " +
					"WHERE R.pageURL = UV.destURL " +
					"AND UV.visitDate BETWEEN '" + conf.get("date_l") + "' AND '" + conf.get("date_u") + "' " +
					"GROUP BY UV.sourceIP;");
		
		return conf;
	}

	@Override
	protected int printUsage() {
		System.out.println("dbjoin\n" +
				"-date_l \"<lower date in yyyy-mm-dd>\" \n" +
				"-date_u \"<upper date in yyyy-mm-dd>\" \n" +
				"-output \"<output file>\" \n");
		return -1;
	}

}
