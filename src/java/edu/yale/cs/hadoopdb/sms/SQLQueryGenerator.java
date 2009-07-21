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
package edu.yale.cs.hadoopdb.sms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFYear;

import edu.yale.cs.hadoopdb.sms.connector.SMSInputFormat;

/**
 * Performs Hive plan analysis and builds relevant SQL queries 
 */
public class SQLQueryGenerator {

	public static final Log LOG = LogFactory.getLog(SQLQueryGenerator.class
			.getName());

	private static final String HIVE_COLUMN_PREFIX = "_col";

	/**
	 * Performs Hive's plan analysis and SQL generation for all tables
	 */
	public static void process(HiveConf conf, QB qb, HashMap<String, Operator<? extends Serializable>> topOps) {
		
		for (String alias : qb.getMetaData().getAliasToTable().keySet()) {
			LOG.debug("Table : " + alias);
			Table tbl = qb.getMetaData().getTableForAlias(alias);

			if (tbl.getInputFormatClass().equals(SMSInputFormat.class)) {

				SQLQuery sqlStructure = SQLQueryGenerator.processTable(alias, tbl,
						(TableScanOperator) topOps.get(alias));
				conf.set(SMSInputFormat.DB_QUERY_SCHEMA_PREFIX + "_"
						+ tbl.getName(), sqlStructure.getDBQuerySchema());
				conf.set(SMSInputFormat.DB_SQL_QUERY_PREFIX + "_"
						+ tbl.getName(), sqlStructure.getSqlQuery());
				
				SQLQueryGenerator.hackMapredWorkSchema(sqlStructure, tbl);

				LOG.info(SMSInputFormat.DB_QUERY_SCHEMA_PREFIX + "_"
						+ tbl.getName() + "---"
						+ sqlStructure.getDBQuerySchema());
				LOG.info(SMSInputFormat.DB_SQL_QUERY_PREFIX + "_"
						+ tbl.getName() + "---" + sqlStructure.getSqlQuery());

			}
		}

	}

	/**
	 * Cheats Hive's internal table schema with that to be returned by DBMS 
	 */
	public static void hackMapredWorkSchema(SQLQuery sqlStructure, Table tbl) {

		Properties schema = tbl.getSchema();
		LOG.debug("MetaData Table properties " + tbl + " : "
				+ schema.toString());

		// hacking
		schema.setProperty("columns.types", sqlStructure.getDDLColumnTypes());
		schema.setProperty("serialization.ddl", sqlStructure
				.getSerializationDDL());
		schema.setProperty("columns", sqlStructure.getDDLColumns());

		LOG.debug("Hacked Table properties " + tbl + " : " + schema.toString());
	}

	/**
	 * Generates default table schema (as in table definition) 
	 */
	private static void generateDefaultSchema(SQLQuery sqlStructure, Table tbl) {

		for (FieldSchema field : tbl.getCols()) {
			sqlStructure.putField(field.getName(), field.getType());
			sqlStructure.addColumn(field.getName(), field.getType());
		}

		// schema.append(", ").append(" dummy int");

	}

	/**
	 * Returns alias for Hive expression as specified in the column renaming map 
	 */
	private static String getAlias(Map<String, exprNodeDesc> map,
			exprNodeDesc expr) {

		for (String key : map.keySet()) {
			if (map.get(key) == expr) {
				return key;
			}
		}
		return null;
	}
	
	/**
	 * Returns internal column id with prefix 
	 */
	private static String prefixColumnIndex(int i) {
		return HIVE_COLUMN_PREFIX + String.valueOf(i);
	}

	/**
	 * Builds SQL query for a given Hive's table 
	 */
	@SuppressWarnings("unchecked")
	public static SQLQuery processTable(String alias, Table tbl,
			TableScanOperator tableScanOp) {

		List<Operator<? extends Serializable>> opsToRemove = new ArrayList<Operator<? extends Serializable>>();

		SQLQuery sqlStructure = new SQLQuery(tbl.getName(), alias);

		generateDefaultSchema(sqlStructure, tbl);
		LOG.debug("Default schema: " + sqlStructure.getDBQuerySchema());

		StringBuilder sqlWhere = new StringBuilder();
		StringBuilder sqlGroupBy = new StringBuilder();

		StringBuilder sb = new StringBuilder();
		Vector<ColumnInfo> rowSchema = tableScanOp.getSchema().getSignature();
		for (ColumnInfo col : rowSchema) {
			sb.append(", ").append(col.getInternalName()).append(" ").append(
					col.getType().getTypeName());
		}

		LOG.debug("Table row schema : " + sb.substring(1));

		List<Operator<? extends Serializable>> children = tableScanOp
				.getChildOperators();

		HashMap<String, String> columnsMapping = new HashMap<String, String>();

		Operator<? extends Serializable> o = children.get(0);
		boolean done = false;
		Map<String, exprNodeDesc> emptyMap = new HashMap<String, exprNodeDesc>();

		// analyze operators for SQL push down
		while (!done) {
			LOG.debug("OP : " + o.getName() + " - " + o.getClass().getName());

			if (o.getColumnExprMap() != null) {
				LOG.debug("Column renaming map: " + o.getColumnExprMap()
						+ " with keys: " + o.getColumnExprMap().keySet());
			}

			Map<String, exprNodeDesc> exprAliasMap = o.getColumnExprMap();
			if (exprAliasMap == null) {
				exprAliasMap = emptyMap;
			}
			
			// handle SELECT (projection, column order)
			if (o instanceof SelectOperator) {
				processSelect((SelectOperator) o, sqlStructure, columnsMapping, exprAliasMap);				

				opsToRemove.add(o);
				o = o.getChildOperators().get(0);
			}
			// handle FILTER (WHERE conditions)
			else if (o instanceof FilterOperator) {
				processFilter((FilterOperator) o, sqlStructure, columnsMapping, sqlWhere);

				opsToRemove.add(o);
				o = o.getChildOperators().get(0);

			}
			// handle GROUP BY (aggregations)
			else if (o instanceof GroupByOperator) {
				
				processGroupBy((GroupByOperator) o, sqlStructure, columnsMapping, exprAliasMap, sqlGroupBy);

				opsToRemove.add(o);
				o = o.getChildOperators().get(0);

			} else {
				done = true;
			}
		}

		sqlStructure.sqlQuery = buildSQLQuery(sqlStructure, alias, tbl, sqlWhere, sqlGroupBy);

		LOG.info(sqlStructure.sqlQuery);

		// removing operators!
		for (Operator op : opsToRemove) {
			removeOperator(op);
		}

		// hack rowSchema in TableScanOp
		tableScanOp.getSchema().setSignature(sqlStructure.getTableRowSchema());

		return sqlStructure;
	}
	
	/**
	 * Builds actual SQL query to execute against DBMS
	 */
	private static String buildSQLQuery(SQLQuery sqlStructure, String alias, Table tbl, StringBuilder sqlWhere, StringBuilder sqlGroupBy) {
		// final SQL query
		StringBuilder sqlSelect = new StringBuilder();
		StringBuilder sqlFrom = new StringBuilder();

		for (String name : sqlStructure.columnList) {
			String columnAlias = sqlStructure.columnAliasMap.get(name);
			sqlSelect.append(", ").append(name);
			if (columnAlias != null) {
				sqlSelect.append(" AS ").append(columnAlias);
			}
		}

		sqlSelect.deleteCharAt(0);

		sqlFrom.append(tbl.getName()).append(" AS ").append(alias);

		StringBuilder sqlQuery = new StringBuilder();
		sqlQuery.append("SELECT ").append(sqlSelect).append(" ");
		sqlQuery.append("FROM ").append(sqlFrom).append(" ");
		if (sqlWhere.length() > 0) {
			sqlQuery.append(" WHERE ").append(sqlWhere).append(" ");
		}
		if (sqlGroupBy.length() > 0) {
			sqlGroupBy.deleteCharAt(0);
			sqlQuery.append(" GROUP BY ").append(sqlGroupBy).append(" ");
		}

		sqlQuery.append(";");
		
		return sqlQuery.toString();
	}

	/**
	 * Handles SelectOperator 
	 */
	private static void processSelect(SelectOperator op, SQLQuery sqlStructure, HashMap<String, String> columnsMapping, Map<String, exprNodeDesc> exprAliasMap) {
		LOG.debug(" Select");

		int i = 0;
		List<String> orderedColumnList = new ArrayList<String>();
		for (exprNodeDesc expr : op.getConf().getColList()) {
			if (expr instanceof exprNodeColumnDesc) {
				String colIndexPrefixed = prefixColumnIndex(i);
				exprNodeColumnDesc col = (exprNodeColumnDesc) expr;
				if (!col.getColumn().equals(colIndexPrefixed)) {
					columnsMapping.put(colIndexPrefixed, col
							.getColumn());
				}
				orderedColumnList.add(col.getColumn());
				String exprAlias = getAlias(exprAliasMap, expr);
				if (exprAlias != null) {
					sqlStructure.putColumnAlias(col.getColumn(),
							exprAlias);
				}

			}
			i++;
		}

		if (orderedColumnList.size() > 0) {
			sqlStructure.retainColumns(orderedColumnList);
			sqlStructure.columnList = orderedColumnList;
			LOG.debug("Adjusted SQL schema is "
					+ sqlStructure.getDBQuerySchema());
		}
		
	}
	
	/**
	 * Handles FilterOperator 
	 */	
	private static void processFilter(FilterOperator op, SQLQuery sqlStructure, HashMap<String, String> columnsMapping, StringBuilder sqlWhere) {
		
		LOG.debug(" Filter");

		exprNodeDesc expr = op.getConf().getPredicate();
		LOG.debug(" expr : " + expr.getClass().getName() + " "
				+ expr.getExprString());
		if (expr instanceof exprNodeFuncDesc) {
			exprNodeFuncDesc func = (exprNodeFuncDesc) expr;

			String condition = funcSQL(columnsMapping, func);

			if (sqlWhere.length() > 0)
				sqlWhere.append(" AND ");

			sqlWhere.append(condition);
		}
		
	}
	
	/**
	 * Handles GroupByOperator 
	 */		
	private static void processGroupBy(GroupByOperator op, SQLQuery sqlStructure, HashMap<String, String> columnsMapping, Map<String, exprNodeDesc> exprAliasMap, StringBuilder sqlGroupBy) {
		 
		LOG.debug(" GroupBy");
		StringBuilder sb = new StringBuilder();

		groupByDesc conf = op.getConf();

		LOG.debug("Output columns: " + conf.getOutputColumnNames());

		List<String> columns = new ArrayList<String>();
		columns.addAll(conf.getOutputColumnNames());

		sb.setLength(0);
		if (conf.getKeys() != null) {

			List<String> groupKeys = new ArrayList<String>();

			sb.append(" - KEYS: ");
			for (exprNodeDesc e : conf.getKeys()) {
				sb.append(e.getClass().getName()).append(" - ");
				sb.append(e.getTypeString() + "=" + e.getExprString());

				String exprAlias = getAlias(exprAliasMap, e);

				// group by columns
				if (e instanceof exprNodeColumnDesc) {
					exprNodeColumnDesc col = (exprNodeColumnDesc) e;

					String colStr = columnsMapping.get(col.getColumn());
					if (colStr == null) {
						colStr = col.getColumn();
					}

					if (exprAlias != null) {
						sqlStructure.putColumnAlias(colStr, exprAlias);
						columns.remove(exprAlias);
					}

					groupKeys.add(colStr);
					sqlGroupBy.append(", ").append(colStr);

					sb.append(" - col: ").append(colStr);

				}
				// group by functions eg. substr(0, 1, 7)

				if (e instanceof exprNodeFuncDesc) {

					exprNodeFuncDesc func = (exprNodeFuncDesc) e;

					String funcSQL = funcSQL(columnsMapping, func);
					String colStr = getColumnFromExpr(func,
							columnsMapping);

					Class<? extends UDF> udfClass = func.getUDFClass();
					FunctionInfo funcInfo = FunctionRegistry
							.getInfo(udfClass);
					String funcName = funcInfo.getDisplayName();
					String funcAlias = funcName + "_" + colStr;
					if (exprAlias != null) {
						funcAlias = exprAlias;
						columns.remove(exprAlias);
					}

					sqlStructure.addColumn(funcSQL, "string");
					sqlStructure.putColumnAlias(funcSQL, funcAlias);

					groupKeys.add(funcSQL);
					sqlGroupBy.append(", ").append(funcAlias);

					sb.append(" - func: ").append(funcSQL).append(
							" AS ").append(funcAlias);

				}

				sb.append("\n");

				sqlStructure.retainColumns(groupKeys);

			}
		}

		if (conf.getAggregators() != null) {
			sb.append(" - AGGREGATORS: ");
			for (aggregationDesc a : conf.getAggregators()) {

				Class<? extends UDAFEvaluator> aggClass = a
						.getAggregationClass();

				FunctionInfo aggInfo = FunctionRegistry
						.getInfo(aggClass);

				sb.append("UDFAggClass : ").append(aggClass.getName())
						.append(" ");
				sb.append(aggInfo.getDisplayName() + " with ");
				sb.append("params {");
				for (exprNodeDesc expr : a.getParameters()) {
					sb.append(", ").append(expr.getClass().getName())
							.append(" - ").append(expr.getExprString());
				}
				sb.append("} ");
				sb.append(a.getExprString() + ", ");

				String aggName = aggInfo.getDisplayName();
				String colStr = getColumnFromExpr(a.getParameters()
						.get(0), columnsMapping);

				String aggSQL = aggName + "(" + colStr + ")";
				String aggAlias = aggName + "_" + colStr;
				if (!columns.isEmpty()) {
					aggAlias = columns.get(0);
					columns.remove(0);
				}

				sqlStructure.addColumn(aggSQL, "double");
				sqlStructure.putColumnAlias(aggSQL, aggAlias);

			}
		}

		LOG.debug(sb.toString());
		LOG.debug("Adjusted SQL schema is "
				+ sqlStructure.getDBQuerySchema());
		
	}
	
	/**
	 * Takes the first column from expression
	 */
	private static String getColumnFromExpr(exprNodeDesc expr,
			HashMap<String, String> columnsMapping) {

		exprNodeDesc e = expr;

		if (e instanceof exprNodeColumnDesc) {
			exprNodeColumnDesc col = (exprNodeColumnDesc) e;

			String colStr = columnsMapping.get(col.getColumn());
			if (colStr == null) {
				colStr = col.getColumn();
			}

			return colStr;
		}
		else if (e instanceof exprNodeFuncDesc) {
			exprNodeFuncDesc func = (exprNodeFuncDesc) e;
			for (exprNodeDesc c : func.getChildExprs()) {
				if (c instanceof exprNodeColumnDesc) {
					exprNodeColumnDesc col = (exprNodeColumnDesc) c;

					String colStr = columnsMapping.get(col.getColumn());
					if (colStr == null) {
						colStr = col.getColumn();
					}

					return colStr;
				}
			}
		}

		return null;
	}

	/**
	 * Removes operators from Hive's plan
	 */
	@SuppressWarnings("unchecked")
	private static void removeOperator(Operator op) {

		LOG.debug("Removing OP : " + op.getName() + " - "
				+ op.getClass().getName());

		List<Operator<? extends Serializable>> parents = op
				.getParentOperators();
		List<Operator<? extends Serializable>> children = op
				.getChildOperators();

		for (Operator parent : parents) {
			parent.getChildOperators().remove(op);
			parent.getChildOperators().addAll(children);
		}

		for (Operator child : children) {
			child.getParentOperators().remove(op);
			child.getParentOperators().addAll(parents);
		}

	}

	/**
	 * Handles predicates including nested ones
	 */
	private static String funcSQL(HashMap<String, String> columnsMapping,
			exprNodeFuncDesc func) {

		StringBuilder sb = new StringBuilder();

		Class<? extends UDF> udfClass = func.getUDFClass();
		FunctionInfo funcInfo = FunctionRegistry.getInfo(udfClass);
		LOG.debug(" udfClass " + udfClass.getName() + " "
				+ funcInfo.getDisplayName());

		List<String> params = new ArrayList<String>();

		for (exprNodeDesc funcParam : func.getChildExprs()) {

			LOG.debug(" funcParam : " + funcParam.getClass().getName() + " "
					+ funcParam.getExprString());

			if (funcParam instanceof exprNodeColumnDesc) {
				exprNodeColumnDesc col = (exprNodeColumnDesc) funcParam;

				String colStr = columnsMapping.get(col.getColumn());
				if (colStr == null) {
					colStr = col.getColumn();
				}
				params.add(colStr);
			}
			else if (funcParam instanceof exprNodeConstantDesc) {
				exprNodeConstantDesc cons = (exprNodeConstantDesc) funcParam;
				params.add(cons.getExprString());
			}
			else if (funcParam instanceof exprNodeFuncDesc) {
				// recursion
				params
						.add(funcSQL(columnsMapping,
								(exprNodeFuncDesc) funcParam));
			}
		}

		if (funcInfo.isOperator()) {

			switch (funcInfo.getOpType()) {
			case PREFIX:
				sb.append("( ").append(funcInfo.getDisplayName()).append(" ");
				sb.append(params.get(0)).append(") ");
				break;
			case INFIX:
				sb.append(" (").append(params.get(0)).append(" ");
				sb.append(funcInfo.getDisplayName()).append(" ");
				sb.append(params.get(1)).append(") ");
				break;
			case POSTFIX:
				sb.append(" (").append(params.get(0)).append(" ");
				sb.append(funcInfo.getDisplayName()).append(") ");
				break;
			default:
				break;
			}
		} else {
			LOG.debug("Not an operator - must handle specially!");

			if (udfClass == UDFSubstr.class) {
				sb.append(funcInfo.getDisplayName());
				sb.append("(").append(params.get(0)).append(",");
				sb.append(params.get(1)).append(",");
				sb.append(params.get(2)).append(") ");
			}
			else if(udfClass == UDFYear.class) {
				sb.append(funcInfo.getDisplayName());
				sb.append("(").append(params.get(0));
				sb.append(") ");
			}
		}

		return sb.toString();
	}

}
