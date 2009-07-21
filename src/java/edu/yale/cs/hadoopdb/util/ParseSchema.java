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


import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * This class accepts a schema represented as "field_name_1 field_type_1, ..."
 * and parses the result set according to the given schema. This avoids
 * other methods from having complete schema awareness. It also provides
 * helper methods to serialize values from the result set into a string.
 * Other helper methods include generating a create table with the given schema SQL 
 * and inserting a row into the table after deserializing the values from a string. 
 */
public class ParseSchema {

	private static final String SPACE_REGEX = "(\\s)+";
	private static final String DELIMITER_REGEX = "\\|";
	public static final String DELIMITER = "|";
	
	String schema;
	int cols;
	ArrayList<String> indexLabelMap;
	ArrayList<String> indexTypeMap;

	String insert_query;

	public ParseSchema(String schema) {
		this.schema = schema;
		String[] descriptors = schema.split(",");
		indexLabelMap = new ArrayList<String>();
		indexTypeMap = new ArrayList<String>();
		for (String descriptor : descriptors) {
			String[] labelType = descriptor.trim().split(SPACE_REGEX);
			indexLabelMap.add(labelType[0].toLowerCase());
			indexTypeMap.add(labelType[1].toLowerCase());
		}
		cols = indexLabelMap.size();
	}

	public String getAsString(String label, ResultSet rs) throws SQLException {
		return getAsString(indexLabelMap.indexOf(label) + 1, rs);
	}

	public String getAsString(int index, ResultSet rs) throws SQLException {
		String sqlType = indexTypeMap.get(index - 1).toLowerCase();
		if (sqlType.contains("varchar")) {
			return rs.getString(index);
		} else if (sqlType.contains("int")) {
			return new Integer(rs.getInt(index)).toString();
		} else if (sqlType.contains("float")) {
			return new Float(rs.getFloat(index)).toString();
		} else if (sqlType.contains("double")) {
			return new Double(rs.getDouble(index)).toString();
		} else if (sqlType.contains("date")) {
			return rs.getDate(index).toString();
		} else if (sqlType.contains("text")) {
			return rs.getString(index);
		} else
			return rs.getObject(index).toString();
	}

	public void deserializeRow(String row, PreparedStatement ps)
			throws SQLException {
		String[] values = row.split(DELIMITER_REGEX);
		assert values.length == cols;
		for (int i = 0; i < cols; i++) {
			insert(i + 1, values[i].trim(), ps);
		}
	}

	public String serializeRow(ResultSet rs) throws SQLException {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < cols; i++) {
			buf.append(DELIMITER);
			buf.append(rs.getObject(indexLabelMap.get(i)));
		}
		return buf.substring(1, buf.length());
	}

	public Class<? extends Object> getType(String label) {
		return getType(indexLabelMap.indexOf(label) + 1);
	}

	public Class<? extends Object> getType(int index) {
		String sqlType = indexTypeMap.get(index - 1).toLowerCase();
		if (sqlType.contains("varchar")) {
			return String.class;
		} else if (sqlType.contains("int")) {
			return Integer.class;
		} else if (sqlType.contains("float")) {
			return Float.class;
		} else if (sqlType.contains("double")) {
			return Double.class;
		} else if (sqlType.contains("date")) {
			return Date.class;
		} else if (sqlType.contains("text")) {
			return String.class;
		} else
			return Object.class;
	}

	public void insert(int colId, String value, PreparedStatement ps)
			throws SQLException {
		String sqlType = indexTypeMap.get(colId - 1).toLowerCase();
		if (sqlType.contains("varchar")) {
			ps.setString(colId, value);
		} else if (sqlType.contains("int")) {
			ps.setInt(colId, Integer.parseInt(value));
		} else if (sqlType.contains("float")) {
			ps.setFloat(colId, Float.parseFloat(value));
		} else if (sqlType.contains("double")) {
			ps.setDouble(colId, Double.parseDouble(value));
		} else if (sqlType.contains("date")) {
			ps.setDate(colId, Date.valueOf(value));
		} else if (sqlType.contains("text")) {
			ps.setString(colId, value);
		}
	}

	private String getLabels() {
		String lab = indexLabelMap.toString();
		return lab.substring(1, lab.length() - 1);
	}

	public String getInsertRowQuery(String relation_id) {
		if (insert_query == null) {
			StringBuffer insert_sql = new StringBuffer("INSERT INTO "
					+ relation_id + "(" + getLabels() + ") VALUES(");
			for (int i = 0; i < cols - 1; i++) {
				insert_sql.append("?, ");
			}
			insert_sql.append("?);");
			insert_query = insert_sql.toString();
		}
		return insert_query;
	}

	public String getCreateTableQuery(String relation_id) {
		return "CREATE TABLE " + relation_id + "(" + this.schema + ");";
	}
}
