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

import java.util.Collection;
import java.util.HashMap;

import edu.yale.cs.hadoopdb.connector.DBChunk;

/**
 * BaseDBConfiguration re-maps the Catalog object created by ConfigurationMapping: 
 * Instead of mapping nodes to relations to chunks (partitions), it maps chunks to 
 * hosts (nodes) for a given relation.
  */
public class BaseDBConfiguration{
	
	protected HashMap<String, DBChunk> chunks = new HashMap<String, DBChunk>();
	protected String relation;

	public BaseDBConfiguration() {
		super();
	}
	/**
	 * Set relation for bookkeeping purpose, not used internally
	 * @param relation the relation's identifier
	 */
	public void setRelation(String relation){
	  this.relation = relation;
	}
	/**
   * Get relation (Relation maintained for bookkeeping, not used internally)
   * @return relation the relation's identifier
   */
	public String getRelation(){
	  return this.relation;
	}

  /**
   * Returns a DBChunk Object for a given chunk id
   * @param id a chunk of a particular relation
   * @return DBChunk chunk for a given id
   */
	public DBChunk getChunk(String id) {
		return chunks.get(id);
	}
  /**
   * Maps DBChunk Object to its partition id
   * @param chunk
   */
	public void addChunk(DBChunk chunk) {
		chunks.put(chunk.getId(), chunk);
	}

  /**
   * Returns a list of DBChunk Objects for a given relation
   * @return Collection<DBChunk> chunks
   */
	public Collection<DBChunk> getChunks() {
		return chunks.values();
	}
}
