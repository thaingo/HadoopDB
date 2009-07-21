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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each relation chunk, this object stores connection and location
 * information in a collection of DBChunkHost identified by their location or
 * host address. This class and all included classes are serialized by
 * DBInputSplit
 */
public class DBChunk implements Serializable {

	private static final long serialVersionUID = 2154832951481581295L;
	private static Random R = new Random(System.currentTimeMillis());

	private String id;
	private HashMap<String, DBChunkHost> locations = new HashMap<String, DBChunkHost>();

	public static final Log LOG = LogFactory.getLog(DBChunk.class.getName());

	public DBChunk(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void addHost(DBChunkHost host) {
		locations.put(host.getHost(), host);
	}

	public DBChunkHost getHost(String host) {
		return locations.get(host);
	}

	/**
	 * Randomly chooses a host from the set of hosts containing the chunk.
	 */
	public DBChunkHost getAnyHost() {
		int size = locations.keySet().size();
		int skip = R.nextInt(size);

		Iterator<String> it = locations.keySet().iterator();
		for (int i = 0; i < skip; i++) {
			it.next();
		}

		String host = it.next();

		return getHost(host);
	}

	/**
	 * Randomly chooses a host from the set of hosts avoiding if possible the
	 * given host
	 */
	public DBChunkHost getAnyHost(DBChunkHost avoid_host) {

		if (avoid_host == null) {
			return getAnyHost();
		}

		List<DBChunkHost> nds = new ArrayList<DBChunkHost>();
		nds.addAll(this.locations.values());
		nds.remove(avoid_host);

		if (nds.size() == 0) {
			LOG.warn("Request to avoid host " + avoid_host + " unsatisfiable -"
					+ "- only one host for chunk " + this.getId());
			return getAnyHost();
		}
		return nds.get(R.nextInt(nds.size()));
	}

	public Collection<DBChunkHost> getHosts() {
		return locations.values();
	}

	public Collection<String> getLocations() {
		return locations.keySet();
	}

	public String toString() {
		return this.id;
	}

}
