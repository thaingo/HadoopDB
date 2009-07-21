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
package edu.yale.cs.hadoopdb.dataloader;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.regex.Pattern;

/**
 * Hash-partitions a local file into the specified number of partitions.
 * Each line of text is assumed to be a record with fields delimited with character
 * given as a param. The fields on which record is hashed is expected to be an index
 * (0 = the first field in a record).
 */
public class LocalHasher {

	public static int COUNT_WINDOW = 1000000;
	public static final String NEW_LINE = System.getProperty("line.separator"); 

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		if(args.length < 4) {
			System.out.println("Usage: <filename> <#parts> <delimiter> <hash_field_pos>");
			return;
		}
		
		long startMillis = System.currentTimeMillis();
		
		String filename = args[0];
		int partNo = Integer.parseInt(args[1]);
		String delimiter = args[2];
		int hashFieldPos = Integer.parseInt(args[3]);
		
		System.out.print("Starting with file " + filename + "... ");
				
		int rows = process(filename, partNo, delimiter, hashFieldPos);
				

		long endMillis = System.currentTimeMillis();
		System.out.println("DONE in " + (endMillis - startMillis) + " #rows = "
				+ rows);

	}

	private static int process(String filename, int partNo, String delimiter, int hashFieldPos) throws Exception {

		Pattern delimiterPattern = Pattern.compile("\\" + delimiter);
		
		int counter = 0;

		
		BufferedWriter[] out = new BufferedWriter[partNo];
		for (int i = 0; i < partNo; i++) {
			out[i] = new BufferedWriter(new FileWriter(i + "-" + filename));
		}
		
		File file = new File(filename);
		FileReader reader = new FileReader(file);
		BufferedReader in = new BufferedReader(reader);
		String line;

		try {

			while ((line = in.readLine()) != null) {

				if(counter % COUNT_WINDOW == 0) System.out.print("*");
				
				// for each line in file
				String fields[] = delimiterPattern.split(line);

				String key = fields[hashFieldPos];

				int hash = hash(key);

				counter++;

				int hash_mod = hash % partNo;				
				if(hash_mod < 0) hash_mod += partNo;
				
				// write to an appropriate file
				out[hash_mod].write(line);
				out[hash_mod].write(NEW_LINE);				

			}
		} finally {
			in.close();
			for (int i = 0; i < partNo; i++) out[i].close();
		}
				
		return counter;

	}
	
	private static int hash(String s) {
		return Integer.reverse(s.hashCode());
	}	
	
}
