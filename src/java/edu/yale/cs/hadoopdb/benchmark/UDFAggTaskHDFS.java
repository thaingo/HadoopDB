package edu.yale.cs.hadoopdb.benchmark;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.exec.HDFSJobBase;
import edu.yale.cs.hadoopdb.util.HDFSUtil;

/**
 * Adapted from Andy Pavlo's code 
 * http://database.cs.brown.edu/projects/mapreduce-vs-dbms/
 */
public class UDFAggTaskHDFS extends HDFSJobBase {

	public static final String URL_PATTERN_STR = "<a href=\"http://([\\w./\\d]+\\.html)\">link</a>";

	protected static final Pattern URL_PATTERN = Pattern.compile(
			URL_PATTERN_STR, Pattern.CASE_INSENSITIVE);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new UDFAggTaskHDFS(), args);
		System.exit(res);
	}

	@Override
	protected JobConf configureJob(String... args) throws IOException {

		JobConf conf = new JobConf(this.getClass());
		conf.setJobName("udf_agg_hdfs");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(LongSumReducer.class);
		conf.setReducerClass(LongSumReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		if (args.length < 2) {
			throw new RuntimeException("Incorrect arguments provided for "
					+ this.getClass());
		}

		FileInputFormat.setInputPaths(conf, new Path(args[0]));

		// OUTPUT properties
		Path outputPath = new Path(args[1]);
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		return conf;

	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {

		Text outputUrl = new Text();
		LongWritable outputPageRank1 = new LongWritable(1);

		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {

			String input = value.toString();
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
		System.out.println("<input_dir> <output_dir>");
		return -1;
	}

}
