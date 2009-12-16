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
	 * Check whether HDFS path exists. 
	 */
	public static boolean existsPath(Path path) throws IOException {
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);
		
		return fs.exists(path);
		
	}
	
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
