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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * HDFS utils
 */
public class HDFSUtil {

	public static final Log LOG = LogFactory.getLog(HDFSUtil.class.getName());
	
	/**
	 * Returns default filesystem
	 */
	public static FileSystem getFS() throws IOException {
			Configuration fconf = new Configuration();
			FileSystem fs = FileSystem.get(fconf);
			return fs;
	}
	
	/**
	 * Check whether HDFS path exists.
	 */
	public static boolean existsPath(Path path) throws IOException {
		return getFS().exists(path);
	}

	/**
	 * Check whether HDFS path is a file.
	 */
	public static boolean isFile(Path path) throws IOException {
		return getFS().isFile(path);
	}

	/**
	 * Deletes HDFS path if it exists.
	 */
	public static void deletePath(Path path) throws IOException {
		FileSystem fs = getFS();

		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	/**
	 * Reads a text file into a set of integer (each value in a separate line)
	 */
	public static void readIntegerSet(Path path, Set<Integer> set)
			throws IOException {

		FSDataInputStream in = getFS().open(path);

		LineReader lineReader = new LineReader(new BufferedInputStream(in));

		Text line = new Text();
		while (lineReader.readLine(line) > 0) {
			String s = line.toString().trim();
			int value = Integer.parseInt(s);
			set.add(value);
		}
		in.close();
	}
}
