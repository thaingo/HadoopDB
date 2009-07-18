package edu.yale.cs.hadoopdb.benchmark;



import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import edu.yale.cs.hadoopdb.util.HDFSUtil;


/**
 * HadoopDB's implementation of Small Aggregation Task
 * http://database.cs.brown.edu/projects/mapreduce-vs-dbms/
 */
public class AggTaskSmallDB extends DBJobBase {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AggTaskSmallDB(), args);
		System.exit(res);
	}

	@Override
	protected JobConf configureJob(String... args) throws Exception {

		JobConf conf = new JobConf(this.getClass());
		conf.setJobName("aggregation_db_small");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		if (args.length < 1) {
			throw new RuntimeException("Incorrect arguments provided for "
					+ this.getClass());
		}

		// OUTPUT properties
		Path outputPath = new Path(args[0]);
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.set(DBConst.DB_RELATION_ID, "UserVisits");
		conf.set(DBConst.DB_RECORD_READER, AggUserVisitsRecord.class.getName());
		conf
				.set(
						DBConst.DB_SQL_QUERY,
						"SELECT SUBSTRING(sourceIP, 1, 7) AS subSourceIP, SUM(adRevenue) AS sumAdRevenue FROM UserVisits GROUP BY subSourceIP;");

		return conf;
	}

	@Override
	protected int printUsage() {
		System.out.println("<output_dir>");
		return -1;
	}

	static class Map extends MapReduceBase implements
			Mapper<LongWritable, AggUserVisitsRecord, Text, DoubleWritable> {

		protected Text outputKey = new Text();
		protected DoubleWritable outputValue = new DoubleWritable();

		public void map(LongWritable key, AggUserVisitsRecord value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {

			outputKey.set(value.getSubSourceIP());
			outputValue.set(value.getSumAdRevenue());
			output.collect(outputKey, outputValue);

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		protected DoubleWritable outputValue = new DoubleWritable();

		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {

			double sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			outputValue.set(sum);
			output.collect(key, outputValue);
		}
	}

	static class AggUserVisitsRecord implements DBWritable {
		private String subSourceIP;
		private double sumAdRevenue;

		public String getSubSourceIP() {
			return subSourceIP;
		}

		public double getSumAdRevenue() {
			return sumAdRevenue;
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.subSourceIP = resultSet.getString("subSourceIP");
			this.sumAdRevenue = resultSet.getDouble("sumAdRevenue");
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			throw new UnsupportedOperationException("No write() impl.");
		}
	}

}
