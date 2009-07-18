package edu.yale.cs.hadoopdb.sms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Maintains a SQL query information and related structures during Hive plan analysis
 * in SMS Planner
 */
public class SQLQuery {

	String sqlQuery;
	// fields in a base table
	HashMap<String, String> fields = new HashMap<String, String>();
	// columns to return (projected, transformed etc)
	List<String> columnList = new ArrayList<String>();
	HashMap<String, String> columnTypeMap = new HashMap<String, String>();
	// aliases to columns (optional)
	HashMap<String, String> columnAliasMap = new HashMap<String, String>();

	private String tableName;
	@SuppressWarnings("unused")
	private String tableAlias;

	public SQLQuery(String tableName, String tableAlias) {
		this.tableName = tableName;
		this.tableAlias = tableAlias;
	}

	public void putColumnAlias(String name, String alias) {
		columnAliasMap.put(name, alias);
	}

	public void addColumn(String name, String type) {
		columnList.add(name);
		columnTypeMap.put(name, type);
	}

	public void removeColumn(String name, String type) {
		columnList.remove(name);
		columnTypeMap.remove(name);
	}

	public void retainColumns(Collection<String> cols) {
		columnList.retainAll(cols);
		columnTypeMap.keySet().retainAll(cols);
	}

	public void putField(String name, String type) {
		fields.put(name, type);
	}

	public String getField(String name) {
		return fields.get(name);
	}

	public String getDBQuerySchema() {

		StringBuilder schema = new StringBuilder();

		for (String name : columnList) {
			String type = columnTypeMap.get(name);
			schema.append(", ").append(getColumnAlias(name)).append(" ")
					.append(type);
		}

		return schema.substring(1);
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

	// columns.types=int:string:int
	public String getDDLColumnTypes() {
		StringBuilder sb = new StringBuilder();
		for (String name : columnList) {
			String type = columnTypeMap.get(name);
			sb.append(":").append(type);
		}
		return sb.substring(1);
	}

	// serialization.ddl=struct rankings { i32 pagerank, string pageurl, i32
	// avgduration}
	public String getSerializationDDL() {
		StringBuilder sb = new StringBuilder();
		for (String name : columnList) {
			String type = typeDLL(columnTypeMap.get(name));
			sb.append(", ").append(type).append(" ").append(
					getColumnAlias(name));
		}
		sb.deleteCharAt(0);
		sb.insert(0, "struct " + tableName + " {");
		sb.append("}");
		return sb.toString();
	}

	// columns=pagerank,pageurl,avgduration
	public String getDDLColumns() {
		StringBuilder sb = new StringBuilder();
		for (String name : columnList) {
			sb.append(",").append(getColumnAlias(name));
		}
		return sb.substring(1);
	}

	private String typeDLL(String type) {
		if (type.equals("int"))
			return "i32";
		else
			return type;
	}

	// rowSchema = tableScanOp.getSchema().getSignature();
	public Vector<ColumnInfo> getTableRowSchema() {
		Vector<ColumnInfo> v = new Vector<ColumnInfo>();

		for (String name : columnList) {
			String type = columnTypeMap.get(name);

			ColumnInfo colInfo = new ColumnInfo();
			colInfo.setInternalName(getColumnAlias(name));
			TypeInfo t = TypeInfoFactory.getPrimitiveTypeInfo(type);
			colInfo.setType(t);

			v.add(colInfo);
		}

		return v;
	}

	public String getColumnAlias(String columnName) {
		String alias = columnAliasMap.get(columnName);
		if (alias != null)
			return alias;
		else
			return columnName;
	}

}
