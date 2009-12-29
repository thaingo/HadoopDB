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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import edu.yale.cs.hadoopdb.catalog.xml.ConfigurationMapping;
import edu.yale.cs.hadoopdb.catalog.xml.Node;
import edu.yale.cs.hadoopdb.connector.DBChunk;
import edu.yale.cs.hadoopdb.connector.DBChunkHost;
import edu.yale.cs.hadoopdb.connector.DBConst;

/**
 * HadoopDB Catalog: Simple XML file based implementation. No support for mid-job updates
 * except by recreating the XML catalog file and re-executing the job. 
 */
public class Catalog {

	public static final Log LOG = LogFactory.getLog(Catalog.class.getName());
	
	
	private static Catalog singleton;

	public static Catalog getInstance(JobConf job) {
		if (singleton == null)
			singleton = new Catalog(job);
		return singleton;
	}

	private ConfigurationMapping xmlConfig;
	private boolean replication = false;

	private Catalog(JobConf job) {

		try {
			FileSystem fs = FileSystem.get(job);
			Path config_file = new Path(job.get(DBConst.DB_CONFIG_FILE));
			xmlConfig = ConfigurationMapping.getInstance(fs.open(config_file));
			replication = job.getBoolean(DBConst.DB_REPLICATION, false);
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
		} catch (JAXBException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}
	
	/**
	 * For a given relation, it populates the configuration object with the different
	 * chunks associated with the relation. Each chunk contains connection and location
	 * information.
	 *  
	 */
	public void setSplitLocationStructure(BaseDBConfiguration dbConf, String relation) {
		
		dbConf.setRelation(relation);
		for(DBChunk chunk : getSplitLocationStructure(relation)) {
			dbConf.addChunk(chunk);
		}
	}	

	/**
	 * For a given relation, it returns a collection of chunks associated with the relation. 
	 * Each chunk contains connection and location information.
	 */
	public Collection<DBChunk> getSplitLocationStructure(String relation) {
		
		Collection<DBChunk> list = new ArrayList<DBChunk>();

		Map<String, List<Node>> chunkHostMap = xmlConfig
				.getPartitionsForRelation(relation);
		if(chunkHostMap == null) {
			throw new RuntimeException("Relation '" + relation + "' is not defined in the catalog.");
		}
		Set<Node> usedNodes = new HashSet<Node>();
		for (String chunk_id : chunkHostMap.keySet()) {
			DBChunk chunk = new DBChunk(chunk_id);
			if (replication) {
				for (Node node : chunkHostMap.get(chunk_id)) {
					chunk.addHost(new DBChunkHost(node.getLocation(),
							xmlConfig.getPartitionForNodeRelation(node,
									relation, chunk_id).getUrl(), node
									.getUsername(), node.getPassword(), node
									.getDriver()));
				}
			} else {
				for (Node node : chunkHostMap.get(chunk_id)) {
					if (usedNodes.contains(node))
						;
					else {
						usedNodes.add(node);
						chunk.addHost(new DBChunkHost(node.getLocation(),
								xmlConfig.getPartitionForNodeRelation(node,
										relation, chunk_id).getUrl(), node
										.getUsername(), node.getPassword(),
								node.getDriver()));
						break;

					}
				}
				if (chunk.getHosts().isEmpty()) {
					Node node = chunkHostMap.get(chunk_id).get(0);
					chunk.addHost(new DBChunkHost(node.getLocation(),
							xmlConfig.getPartitionForNodeRelation(node,
									relation, chunk_id).getUrl(), node
									.getUsername(), node.getPassword(), node
									.getDriver()));
				}
			}
			list.add(chunk);
		}
		
		return list;
	}	
	
}
