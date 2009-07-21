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

/**
 * DBChunkHost stores connection information to particular chunk within
 * a particular host. This class could be extended in the future to
 * include other optional connection parameters.
 *
 */
public class DBChunkHost implements Serializable {

	private static final long serialVersionUID = -6635222373182598967L;
	
	
	private String address;
	private String url;
	private String user;
	private String password;
	private String driver;
		
	public DBChunkHost(String address, String url,
			String user, String password, String driver) {
		super();
		this.address = address;
		this.password = password;
		this.url = url;
		this.user = user;
		this.driver = driver;
	}
	
	public String getHost() {
		return address;
	}
	public void setHost(String host) {
		this.address = host;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}
		
}

