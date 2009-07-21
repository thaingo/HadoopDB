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
package edu.yale.cs.hadoopdb.connector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

/**
 * DBInputSplit links each Map to a DB Chunk. Splits
 * serialize their DB connection information so that when
 * instantiated at Map nodes, they can connect to their
 * respective DB Chunks.
 */
public class DBInputSplit implements InputSplit {

	public static final Log LOG = LogFactory.getLog(DBInputSplit.class
			.getName());
	
	protected String[] locations;
	protected DBChunk chunk;
	protected String relation;
	

	public DBChunk getChunk() {
		return chunk;
	}

	/**
	 * Sets a DBChunk and updates split locations
	 */
	public void setChunk(DBChunk chunk) {
		this.chunk = chunk;
		setLocations();
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}
	
	/**
	 * This method is called by readFields or setChunk on split 
	 * instantiation or creation. A chunk could be stored
	 * on one or more locations. The locations array is therefore populated
	 * with the different host locations of the split's chunk.
	 */
	private void setLocations() {
		Collection<DBChunkHost> hosts = chunk.getHosts();
		locations = new String[hosts.size()];
		int j = 0;
		for (DBChunkHost node : hosts) {
			locations[j] = node.getHost();
			j++;
		}
	}
	
	/**
	 * Returns 1 now... could in the future return
	 * information about the size of the Chunk (e.g. number of rows)
	 */
	@Override
	public long getLength() throws IOException {
		return 1;
	}

	/**
	 * Returns locations (host addresses) of the chunk's hosts.
	 */
	@Override
	public String[] getLocations() throws IOException {
		return locations;
	}
	
	/**
	 * Deserializes relation and DBChunk object. Then creates the list of locations from the
	 * DBChunk object.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		relation = Text.readString(in);
		setChunk(deserializeChunk(in));
	}

	/**
	 * Serializes the relation and Chunk object.
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, relation);
		serializeChunk(chunk, out);
	}

	/**
	 * Serializes DBChunk 
	 */
	private void serializeChunk(DBChunk chunk, DataOutput out)
			throws IOException {
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream();
		ObjectOutputStream object_stream = new ObjectOutputStream(
				byte_stream);
		object_stream.writeObject(chunk);
		object_stream.close();

		byte[] buf = byte_stream.toByteArray();
		BytesWritable bw = new BytesWritable(buf);
		bw.write(out);
	}

	/**
	 * Deserializes DBChunk 
	 */
	private DBChunk deserializeChunk(DataInput in) throws IOException {
		BytesWritable br = new BytesWritable();
		br.readFields(in);
		byte[] buf = br.getBytes();
		ObjectInputStream byte_stream = new ObjectInputStream(
				new ByteArrayInputStream(buf));
		DBChunk chunk = null;
		try {
			chunk = (DBChunk) byte_stream.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
		return chunk;
	}
		
}