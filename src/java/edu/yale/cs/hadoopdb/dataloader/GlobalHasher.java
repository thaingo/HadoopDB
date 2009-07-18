package edu.yale.cs.hadoopdb.dataloader;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.exec.HDFSJobBase;
import edu.yale.cs.hadoopdb.util.HDFSUtil;

/**
 * Hash-partitions files stored in HDFS into the specified number of partitions.
 * Each line of text is assumed to be a record with fields delimited with character
 * given as a param. The fields on which record is hashed is expected to be an index
 * (0 = the first field in a record).
 */
public class GlobalHasher extends HDFSJobBase {

	public static final String DELIMITER_PARAM = "globalhasher.delimiter";
	public static final String HASH_FIELD_POS_PARAM = "globalhasher.hash_field_pos";
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new GlobalHasher(), args);
		System.exit(res);
	}

	@Override
	protected JobConf configureJob(String... args) throws Exception {

		JobConf conf = new JobConf(getConf(), this.getClass());
		conf.setJobName("GlobalHasher");

		conf.setMapOutputKeyClass(UnsortableInt.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(GlobalHasher.Map.class);
		conf.setReducerClass(GlobalHasher.Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		if (args.length < 5) {
			throw new RuntimeException("Incorrect arguments provided for "
					+ this.getClass());
		}

		FileInputFormat.setInputPaths(conf, new Path(args[0]));

		// OUTPUT properties
		Path outputPath = new Path(args[1]);
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		
		int partNo = Integer.parseInt(args[2]);
		conf.setNumReduceTasks(partNo);
		
		conf.set(DELIMITER_PARAM, args[3]);
		
		int hashFieldPos = Integer.parseInt(args[4]);
		conf.setInt(HASH_FIELD_POS_PARAM, hashFieldPos);
		


		return conf;
	}

	static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, UnsortableInt, Text> {

		protected UnsortableInt outputKey = new UnsortableInt();
		
		protected String delimiter;
		protected int hashFieldPos;
		protected Pattern delimiterPattern;
		
		public void configure(JobConf job) {
			super.configure(job);
			delimiter = job.get(DELIMITER_PARAM);
			hashFieldPos = job.getInt(HASH_FIELD_POS_PARAM, 0);
			delimiterPattern = Pattern.compile("\\" + delimiter);
		}
		
		
		public void map(LongWritable key, Text value,
				OutputCollector<UnsortableInt, Text> output, Reporter reporter)
				throws IOException {

			String fields[] = delimiterPattern.split(value
					.toString());

			String newKey = fields[hashFieldPos];			
			int hash = hash(newKey);

			outputKey.set(hash);
			output.collect(outputKey, value);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<UnsortableInt, Text, NullWritable, Text> {

		private NullWritable nullKey = NullWritable.get();
		
		public void reduce(UnsortableInt key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			
			
			while (values.hasNext()) {
				output.collect(nullKey, values.next());
			}
		}
				
	}

	@Override
	protected int printUsage() {
		
		System.out.println("<input_dir> <output_dir> <# of partitions> <delimiter> <hash_field_pos>");
		return -1;
	}
	
	/**
	 * @author Kamil
	 *
	 * Avoids sorting in MR
	 */
	
	@SuppressWarnings("unchecked")
	private static class UnsortableInt implements WritableComparable {
		  private int value;

		  public UnsortableInt() {}

		  public UnsortableInt(int value) { set(value); }

		  public void set(int value) { this.value = value; }

		  public int get() { return value; }

		  public void readFields(DataInput in) throws IOException {
		    value = in.readInt();
		  }

		  public void write(DataOutput out) throws IOException {
		    out.writeInt(value);
		  }

		  public boolean equals(Object o) {
		    if (!(o instanceof UnsortableInt))
		      return false;
		    return true;
		  }

		  public int hashCode() {
		    return value;
		  }

		  public int compareTo(Object o) {
		    return 0;
		  }

		  public String toString() {
		    return Integer.toString(value);
		  }
 
		  public static class Comparator extends WritableComparator {
		    public Comparator() {
		      super(UnsortableInt.class);
		    }

		    public int compare(byte[] b1, int s1, int l1,
		                       byte[] b2, int s2, int l2) {
		      return 0;
		    }
		  }

		  static {
		    WritableComparator.define(UnsortableInt.class, new Comparator());
		  }
	}
	
	private static int hash(String s) {
		return Integer.rotateLeft(s.hashCode(), 13);
	}
}
