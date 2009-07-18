package edu.yale.cs.hadoopdb.benchmark;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.exec.HDFSJobBase;
import edu.yale.cs.hadoopdb.util.BenchmarkUtils;
import edu.yale.cs.hadoopdb.util.HDFSUtil;

/**
 * Adapted from Andy Pavlo's code 
 * http://database.cs.brown.edu/projects/mapreduce-vs-dbms/
 */
public class SelectionTaskHDFS extends HDFSJobBase {

	public static final String PAGE_RANK_VALUE_PARAM = "page.rank.value";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SelectionTaskHDFS(), args);
		System.exit(res);
	}

	@Override
	protected JobConf configureJob(String... args) throws IOException {

		JobConf conf = new JobConf(getConf(), this.getClass());
		conf.setJobName("selection_hdfs");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setNumReduceTasks(0);
	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		if (args.length < 3) {
			throw new RuntimeException("Incorrect arguments provided for "
					+ this.getClass());
		}
		
		conf.set(PAGE_RANK_VALUE_PARAM, args[0]);
		FileInputFormat.setInputPaths(conf, new Path(args[1]));

		// OUTPUT properties
		Path outputPath = new Path(args[2]);
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		return conf;

	}

	static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		protected int PAGE_RANK_VALUE;

		public void configure(JobConf job) {
			super.configure(job);
	
			String pageRank = job.get(PAGE_RANK_VALUE_PARAM);
			PAGE_RANK_VALUE = Integer.parseInt(pageRank);
		}
		
		protected Text outputKey = new Text();
		protected IntWritable outputValue = new IntWritable();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			// Rankings: pageRank | pageURL | avgDuration
			String fields[] = BenchmarkUtils.DELIMITER_PATTERN.split(value.toString());
			
			int pageRank = Integer.valueOf(fields[0]);
			if (pageRank > PAGE_RANK_VALUE) {
				outputKey.set(fields[1]);
				outputValue.set(pageRank);
				output.collect(outputKey, outputValue);
			}
		}
	}

	@Override
	protected int printUsage() {
		System.out.println("<page_rank_value> <input_dir> <output_dir>");
		return -1;
	}

}
