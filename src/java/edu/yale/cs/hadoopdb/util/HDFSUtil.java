package edu.yale.cs.hadoopdb.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS utils 
 */
public class HDFSUtil {
	
	/**
	 * Deletes HDFS path if it exists. 
	 */
	public static void deletePath(Path path) throws IOException {
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);
		
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		
	}

}
