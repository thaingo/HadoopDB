package edu.yale.cs.hadoopdb.benchmark;


import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.exec.HDFSJobBase;
import edu.yale.cs.hadoopdb.util.BenchmarkUtils;
import edu.yale.cs.hadoopdb.util.HDFSUtil;

/**
 * Adapted from Andy Pavlo's code 
 * http://database.cs.brown.edu/projects/mapreduce-vs-dbms/
 */
public class JoinTaskHDFS extends HDFSJobBase {

	public static final String DATE_FROM_PARAM = "visitDate.from";
	public static final String DATE_TO_PARAM = "visitDate.to";

	public static final int RANKINGS_FIELD_NUMBER = 3;
	public static final int USER_VISITS_FIELD_NUMBER = 9;

	public final static DateFormat dateParser = new SimpleDateFormat(
			"yyyy-MM-dd");

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new JoinTaskHDFS(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();

		if (args.length < 5) {
			throw new RuntimeException("Incorrect arguments provided for "
					+ this.getClass());
		}

		String dateFrom = args[0];
		String dateTo = args[1];
		String rankingsInputDir = args[2];
		String userVisitsInputDir = args[3];
		String outputDir = args[4];

		// output path (delete)
		Path outputPath = new Path(outputDir);
		HDFSUtil.deletePath(outputPath);

		// phase 1
		JobConf conf1 = new JobConf(this.getClass());
		conf1.setJobName("join_hdfs_phase1");
		Path p1Output = new Path(outputDir + "/phase1");
		FileOutputFormat.setOutputPath(conf1, p1Output);
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf1, new Path(rankingsInputDir),
				new Path(userVisitsInputDir));

		conf1.set(DATE_FROM_PARAM, dateFrom);
		conf1.set(DATE_TO_PARAM, dateTo);

		conf1.setMapperClass(Phase1Map.class);
		conf1.setReducerClass(Phase1Reduce.class);
		// conf1.setPartitionerClass(theClass)

		RunningJob job1 = JobClient.runJob(conf1);

		if (job1.isSuccessful()) {

			// phase 2

			JobConf conf2 = new JobConf(this.getClass());
			conf2.setJobName("join_hdfs_phase2");
			conf2.setInputFormat(KeyValueTextInputFormat.class);
			conf2.setOutputFormat(TextOutputFormat.class);

			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(Text.class);
			conf2.setMapperClass(IdentityMapper.class);
			conf2.setReducerClass(Phase2Reduce.class);

			Path p2Output = new Path(outputDir + "/phase2");
			FileOutputFormat.setOutputPath(conf2, p2Output);
			FileInputFormat.setInputPaths(conf2, p1Output);

			RunningJob job2 = JobClient.runJob(conf2);

			if (job2.isSuccessful()) {

				// phase 3

				JobConf conf3 = new JobConf(this.getClass());
				conf3.setJobName("join_hdfs_phase3");
				conf3.setNumReduceTasks(1);
				conf3.setInputFormat(KeyValueTextInputFormat.class);
				conf3.setOutputKeyClass(Text.class);
				conf3.setOutputValueClass(Text.class);
				conf3.setMapperClass(IdentityMapper.class);
				conf3.setReducerClass(Phase3Reduce.class);

				Path p3Output = new Path(outputDir + "/phase3");
				FileOutputFormat.setOutputPath(conf3, p3Output);
				FileInputFormat.setInputPaths(conf3, p2Output);

				RunningJob job3 = JobClient.runJob(conf3);

				if (!job3.isSuccessful()) {
					System.out.println("PHASE 3 FAILED!!!");
				}

			} else {
				System.out.println("PHASE 2 FAILED!!!");
			}

		} else {
			System.out.println("PHASE 1 FAILED!!!");
		}

		long endTime = System.currentTimeMillis();
		System.out.println("\nJOB TIME : " + (endTime - startTime) + " ms.\n");

		return 0;
	}

	@Override
	protected JobConf configureJob(String... args) throws IOException {
		return null;
	}

	@Override
	protected int printUsage() {
		System.out
				.println("<date_from> <date_to> <rankings_input_dir> <user_visits_input_dir> <output_dir>");
		return -1;
	}

	class Phase1Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		protected Date DATE_FROM = null;
		protected Date DATE_TO = null;

		public void configure(JobConf job) {
			super.configure(job);

			String dateFrom = job.get(JoinTaskHDFS.DATE_FROM_PARAM);
			String dateTo = job.get(JoinTaskHDFS.DATE_TO_PARAM);

			try {
				DATE_FROM = JoinTaskHDFS.dateParser.parse(dateFrom);
				DATE_TO = JoinTaskHDFS.dateParser.parse(dateTo);
			} catch (ParseException ex) {
				ex.printStackTrace();
				System.exit(1);
			}
		}

		protected Text outputKey = new Text();
		protected Text outputValue = new Text();
		protected StringBuilder newValue = new StringBuilder();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String fields[] = BenchmarkUtils.DELIMITER_PATTERN.split(value
					.toString());
			String newKey = null;
			newValue.setLength(0);

			// Rankings: pageRank | pageURL | avgDuration
			if (fields.length == JoinTaskHDFS.RANKINGS_FIELD_NUMBER) {
				newKey = fields[1];
				// pageRank
				newValue.append(fields[0]);

				// UserVisits: sourceIP | destURL | visitDate | adRevenue |
				// userAgent | countryCode | langCode | searchWord | duration
			} else if (fields.length == JoinTaskHDFS.USER_VISITS_FIELD_NUMBER) {
				try {
					Date date = JoinTaskHDFS.dateParser.parse(fields[2]);
					if (date.compareTo(DATE_FROM) >= 0
							&& date.compareTo(DATE_TO) <= 0) {
						newKey = fields[1];
						// sourceIP
						newValue.append(fields[0]);
						newValue.append(BenchmarkUtils.DELIMITER);
						// adRevenue
						newValue.append(fields[3]);
					}
				} catch (ParseException ex) {
					ex.printStackTrace();
					System.exit(1);
				}
			}

			if (newKey != null) {
				outputKey.set(newKey);
				outputValue.set(newValue.toString());
				output.collect(outputKey, outputValue);
			}
		}
	}

	class Phase1Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		protected Text outputKey = new Text();
		protected Text outputValue = new Text();
		protected StringBuilder newValue = new StringBuilder();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String pageRank = null;
			List<String[]> sourceIPadRevenue = new ArrayList<String[]>();

			while (values.hasNext()) {
				Text value = values.next();

				String valueStr = value.toString();

				int delimiterIndex = valueStr.indexOf(BenchmarkUtils.DELIMITER);

				// pageRank
				if (delimiterIndex == -1) {
					if (pageRank == null) {
						pageRank = valueStr;
					}
					// sourceIP | adRevenue
				} else {

					String fields[] = new String[2];
					// sourceIP
					fields[0] = valueStr.substring(0, delimiterIndex);
					// adRevenue
					fields[1] = valueStr.substring(delimiterIndex + 1);

					sourceIPadRevenue.add(fields);
				}
			}

			// Output record:
			// <sourceIP> -> (<pageRank>, <adRevenue>)
			for (String fields[] : sourceIPadRevenue) {

				outputKey.set(fields[0]); // sourceIP
				newValue.setLength(0);
				newValue.append(pageRank);
				newValue.append(BenchmarkUtils.DELIMITER);
				newValue.append(fields[1]); // adRevenue
				outputValue.set(newValue.toString());
				output.collect(outputKey, outputValue);
			}
		}
	}

	class Phase2Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		protected OutputCollector<Text, Text> output = null;

		protected Double max_total_adRevenue = null;
		protected Text max_key = null;
		protected Text max_val = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if (this.output == null)
				this.output = output;

			double total_adRevenue = 0.0d;
			long counter = 0l;
			long total_pageRank = 0l;

			while (values.hasNext()) {
				Text value = values.next();
				String fields[] = BenchmarkUtils.DELIMITER_PATTERN.split(value
						.toString());

				// <sourceIP> -> (<pageRank>, <adRevenue>)
				if (fields.length == 2) {
					total_pageRank += Long.parseLong(fields[0]);
					total_adRevenue += Double.parseDouble(fields[1]);
					counter++;
				}
			}

			if (this.max_total_adRevenue == null
					|| total_adRevenue > this.max_total_adRevenue) {
				this.max_total_adRevenue = total_adRevenue;
				long average_pageRank = total_pageRank / (long) counter;
				this.max_val.set(total_adRevenue + BenchmarkUtils.DELIMITER
						+ average_pageRank);
				this.max_key = key;
			}
		}

		@Override
		public void close() throws IOException {
			// Max record:
			// <sourceIP> -> (<max total adRevenue> | <average pageRank>)
			if (this.max_total_adRevenue != null) {
				this.output.collect(this.max_key, this.max_val);
			}
			super.close();
		}

	}

	class Phase3Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		protected OutputCollector<Text, Text> output = null;

		protected Double max_total_adRevenue = null;
		protected Text max_key = null;
		protected Text max_val = null;

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if (this.output == null)
				this.output = output;

			while (values.hasNext()) {
				Text value = values.next();

				String fields[] = BenchmarkUtils.DELIMITER_PATTERN.split(value
						.toString());
				// <key> -> (<total_adRevenue> | <average_pageRank>)
				if (fields.length == 2) {
					double total_adRevenue = Double.parseDouble(fields[0]);
					if (this.max_total_adRevenue == null
							|| total_adRevenue > this.max_total_adRevenue) {
						this.max_total_adRevenue = total_adRevenue;
						this.max_key = key;
						this.max_val = value;
					}
				}
			}
		}

		@Override
		public void close() throws IOException {

			// Max record
			if (this.max_total_adRevenue != null) {
				this.output.collect(this.max_key, this.max_val);
			}
			super.close();
		}
	}

}
