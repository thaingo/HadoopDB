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
package edu.yale.cs.hadoopdb.catalog;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;

import org.apache.hadoop.hive.ql.parse.HiveParser.propertiesList_return;

import edu.yale.cs.hadoopdb.catalog.xml.Configuration;
import edu.yale.cs.hadoopdb.catalog.xml.Node;
import edu.yale.cs.hadoopdb.catalog.xml.ObjectFactory;
import edu.yale.cs.hadoopdb.catalog.xml.Partition;
import edu.yale.cs.hadoopdb.catalog.xml.Relation;


/**
 * This class randomly replicates chunked and unchunked databases across a cluster. 
 * Since ensuring random replication is equivalent to a perfect matching problem, we don't
 * implement a nice algorithm that guarantees you always get a perfect random matching.
 * This algorithm is greedy. It will stall infinitely if it can't find a replicate. At that
 * point, you might get lucky in another try.
 * 
 * The class will generate shell scripts for POSTGRES databases only. Copying the
 * scripts to the appropriate nodes and executing them in parallel is not handled by
 * this code. Instead use the example python scripts to move scripts and execute scripts in parallel.
 *
 */
public class SimpleRandomReplicationFactorTwo extends SimpleCatalogGenerator {

	private Map<Integer, List<Integer>> buckets = new HashMap<Integer, List<Integer>>();
	private Random gen;
	
	private static final String SSH_KEY = "ssh_key";
	private static final String REPLICATION_SCRIPT_PREFIX = "replication_script_prefix";
	private static final String DUMP_FILE_U_PREFIX = "dump_file_u_prefix";
	private static final String DUMP_FILE_C_PREFIX = "dump_file_c_prefix";
	private static final String DUMP_SCRIPT_PREFIX = "dump_script_prefix";
	
	private boolean dump = true;
	
	private int chunks_per_node;
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		SimpleRandomReplicationFactorTwo rcat = new SimpleRandomReplicationFactorTwo();
		rcat.parseArguments(args);
		rcat.generateBuckets();
		
		try {
			rcat.replicate();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (JAXBException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	protected void parseArguments(String[] args) throws FileNotFoundException, IOException{
		for(int i = 0; i < args.length; i++){
			if(args[i].equals("-dump")){
				dump = new Boolean(args[++i]);
			}
		}
		super.parseArguments(args);
		chunks_per_node = new Integer(properties.getProperty(CHUNKS_PER_NODE));
	}
	
	private void generateBuckets(){
		gen = new Random();
		for(int i = 0; i < nodes.size(); i++){
			ArrayList<Integer> parts = new ArrayList<Integer>();
			for(int x = 0; x < chunks_per_node; x++)
				parts.add(i*chunks_per_node + x);
			buckets.put(i, parts);
		}
	}
	
	private void generatePostgresDumpScrips() throws IOException{
		int node_counter = 0;
		if(dump){
			for(String node : nodes){
				File dump_file = new File(properties.getProperty(DUMP_SCRIPT_PREFIX) + node + ".sh");
				BufferedWriter bw = new BufferedWriter(new FileWriter(dump_file));
				int start_index = node_counter*chunks_per_node;
				for(int index = start_index; index < start_index + chunks_per_node; index ++) {
					bw.append("pg_dump -U postgres -c " + properties.getProperty(CHUNKED_DB_PREFIX) + index 
							+ " > " + properties.getProperty(DUMP_FILE_C_PREFIX) + index + ".sql\n");
				}
				bw.append("pg_dump -U postgres -c " + properties.getProperty(UNCHUNKED_DB_PREFIX) + node_counter 
						+ " > " + properties.getProperty(DUMP_FILE_U_PREFIX) + node_counter + ".sql\n");
				bw.flush();
				bw.close();
			}
			node_counter++;
		}
	}
	
	public void replicate() throws JAXBException, PropertyException, FileNotFoundException, IOException {
		generatePostgresDumpScrips();
	
		JAXBContext jc = JAXBContext.newInstance("edu.yale.cs.hadoopdb.catalog.xml");
		Marshaller marshaller = jc.createMarshaller();
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,new Boolean(true));
		
		ObjectFactory factory = new ObjectFactory();
		Configuration configuration = factory.createConfiguration();
		
		int node_counter = 0;

		int rand_offset = gen.nextInt(nodes.size() - 1);
		for(String node : nodes){
			File replication_script_file = new File(properties.getProperty(REPLICATION_SCRIPT_PREFIX)  + node + ".sh");
			BufferedWriter bw = new BufferedWriter(new FileWriter(replication_script_file));
			
			
			Node n = factory.createNode();
			n.setDriver(properties.getProperty(DRIVER));
			n.setPassword(properties.getProperty(PASSWORD));
			n.setUsername(properties.getProperty(USERNAME));
			n.setLocation(node);

			//Unchunked relation replication
			int replica_node_id = (node_counter + rand_offset)%nodes.size();
			for(String relation : relations_unchunked){
				Relation r = factory.createRelation();
				r.setId(relation);
				
				//Original
				Partition p = factory.createPartition();
				p.setId(new Integer(node_counter).toString());
				p.setUrl(properties.getProperty(URL_PREFIX) + node + ":" + properties.getProperty(PORT) 
						+ "/" + properties.getProperty(UNCHUNKED_DB_PREFIX) + node_counter);
				r.getPartitions().add(p);
				
				//Replica
				Partition p_r = factory.createPartition();
				p_r.setId(new Integer(replica_node_id).toString());
				p_r.setUrl(properties.getProperty(URL_PREFIX) + node + ":" + properties.getProperty(PORT) 
						+ "/r_" + properties.getProperty(UNCHUNKED_DB_PREFIX) + replica_node_id);
				r.getPartitions().add(p_r);
				
				n.getRelations().add(r);
			}
			//Update script file
			bw.append("scp -i " + properties.getProperty(SSH_KEY) + " " + nodes.get(replica_node_id) + ":" 
					+ properties.getProperty(DUMP_FILE_U_PREFIX) + replica_node_id + ".sql " 
					+ properties.getProperty(DUMP_FILE_U_PREFIX) + replica_node_id + ".sql\n");
			bw.append("psql -U postgres -c 'CREATE DATABASE r_" + properties.getProperty(UNCHUNKED_DB_PREFIX) + replica_node_id 
					+ " WITH OWNER " + properties.getProperty(USERNAME) + ";'\n");
			bw.append("psql -U " +  properties.getProperty(USERNAME) + " -d r_" 
					+ properties.getProperty(UNCHUNKED_DB_PREFIX) + replica_node_id 
					+ " -f " + properties.getProperty(DUMP_FILE_U_PREFIX) + replica_node_id + ".sql\n");
			
			//Chunked Relation Replication
			int[] replica_chunk_id = new int[chunks_per_node];
			for(int i = 0; i < chunks_per_node; i++){
				replica_chunk_id[i] = getNumber(node_counter, (node_counter == (nodes.size() - 1)));
			}
			for(String relation : relations_chunked){
				Relation r = factory.createRelation();
				r.setId(relation);
				
				int start_index = node_counter*chunks_per_node;
				for(int index = start_index; index < start_index + chunks_per_node; index ++) {
					//Original
					Partition p = factory.createPartition();
					p.setId(new Integer(index).toString());
					p.setUrl(properties.getProperty(URL_PREFIX) + node + ":" + properties.getProperty(PORT) 
							+ "/" + properties.getProperty(CHUNKED_DB_PREFIX) + index);
					r.getPartitions().add(p);
					
					//Replica
					int chunk_id = replica_chunk_id[index - start_index];
					replica_node_id = chunk_id/20;
					Partition p_r = factory.createPartition();
					p_r.setId(new Integer(chunk_id).toString());
					p_r.setUrl(properties.getProperty(URL_PREFIX) + node + ":" + properties.getProperty(PORT) 
							+ "/r_" + properties.getProperty(CHUNKED_DB_PREFIX)+ chunk_id);
					r.getPartitions().add(p_r);
					
					//Update script file
					bw.append("scp -i " + properties.getProperty(SSH_KEY) + " " + nodes.get(replica_node_id) + ":" 
							+ properties.getProperty(DUMP_FILE_C_PREFIX) + chunk_id + ".sql " 
							+ properties.getProperty(DUMP_FILE_C_PREFIX) + chunk_id + ".sql\n");
					bw.append("psql -U postgres -c 'CREATE DATABASE r_" +  properties.getProperty(CHUNKED_DB_PREFIX) + chunk_id 
							+ " WITH OWNER " + properties.getProperty(USERNAME) + ";'\n");
					bw.append("psql -U " +  properties.getProperty(USERNAME) + " -d r_" +  properties.getProperty(CHUNKED_DB_PREFIX) + chunk_id 
							+ " -f " + properties.getProperty(DUMP_FILE_C_PREFIX) + chunk_id + ".sql\n");
				}
				n.getRelations().add(r);
			}
			
			node_counter++;
			configuration.getNodes().add(n);
			bw.flush();
			bw.close();
		}
		JAXBElement<Configuration> jaxb = factory.createDBClusterConfiguration(configuration);
		marshaller.marshal(jaxb, new FileOutputStream(new File(properties.getProperty(CATALOG_FILE))));
	}
	
	private int getNumber(int avoid_bucket, boolean last_node){
		int empty_count = 0;
		for(List<Integer> bucket : buckets.values()){
			if(bucket.size() == 0)
				empty_count++;
		}
		if(empty_count == buckets.size()  ||
				(empty_count == (buckets.size() - 1) && buckets.get(avoid_bucket).size() > 0)){
			throw new RuntimeException("Cannot satisfy random replication request, please run again!");
		}
		int b_r = gen.nextInt(buckets.size());
		while(b_r == avoid_bucket ||
				(b_r != (nodes.size() - 1) && buckets.get(b_r).size() <= 2 && !last_node) || 
				(buckets.get(b_r).size() == 0))
			b_r = gen.nextInt(buckets.size());
		int r = gen.nextInt(buckets.get(b_r).size());
		return buckets.get(b_r).remove(r);
	}

}
