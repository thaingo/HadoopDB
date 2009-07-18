package edu.yale.cs.hadoopdb.benchmark;


import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
import org.apache.hadoop.util.ToolRunner;

import edu.yale.cs.hadoopdb.connector.DBConst;
import edu.yale.cs.hadoopdb.connector.DBWritable;
import edu.yale.cs.hadoopdb.exec.DBJobBase;
import edu.yale.cs.hadoopdb.util.HDFSUtil;

/**
 * HadoopDB's implementation of Grep Task
 * http://database.cs.brown.edu/projects/mapreduce-vs-dbms/
 */
public class GrepTaskDB extends DBJobBase {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GrepTaskDB(), args);
		System.exit(res);
	}

	static class DocumentsRecord implements DBWritable {
		private String key;
		private String field;

		public String getKey() {
			return key;
		}

		public String getField() {
			return field;
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.key = resultSet.getString("key1");
			this.field = resultSet.getString("field");
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			throw new UnsupportedOperationException("No write() impl.");		
		}
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, DocumentsRecord, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		public void map(LongWritable dummy, DocumentsRecord record, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			outputKey.set(record.key);
			outputValue.set(record.field);
			output.collect(outputKey, outputValue);
		}
	}

	@Override
	protected JobConf configureJob(String... args) throws IOException {

		JobConf conf = new JobConf(GrepTaskDB.class);
		conf.setJobName("grep_db_job");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setNumReduceTasks(0);
		

		// GREP arguments
		conf.setOutputFormat(TextOutputFormat.class);
		for (int i = 0; i < args.length; ++i) {
			if ("-pattern".equals(args[i]))
				conf.set("pattern", args[++i]);
			else if("-output".equals(args[i]))
				conf.set("output", args[++i]);
		}

		// OUTPUT properties

		Path outputPath = new Path(conf.get("output"));
		System.out.println(conf.get("output"));
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		// DB properties
		conf.set(DBConst.DB_RELATION_ID, "grep");
		conf.set(DBConst.DB_RECORD_READER, DocumentsRecord.class.getName());
		conf.set(DBConst.DB_SQL_QUERY, "SELECT key1, field FROM grep WHERE field LIKE '%" + conf.get("pattern") + "%';");
		
		return conf;
		

	}

	@Override
	protected int printUsage() {
		System.out.println("grep \n-pattern <pattern> \n-output <output file>");
		return -1;
	}

}
