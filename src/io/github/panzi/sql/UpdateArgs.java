package io.github.panzi.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateArgs {
	final String tablename;
	final QueryBuilder builder;
	final Map<String, Object> args;

	public UpdateArgs(QueryBuilder builder, String tablename) {
		this.tablename = tablename;
		this.builder = builder;
		this.args    = new HashMap<String, Object>();
	}
	
	public UpdateArgs set(String column, Object value) {
		args.put(column, value);
		return this;
	}

	public void update(Map<String, Object> values) throws SQLException {
		args.putAll(values);
		update();
	}

	public void insert(Map<String, Object> values) throws SQLException {
		args.putAll(values);
		insert();
	}

	public int update() throws SQLException {
		return builder.update(tablename, args);
	}

	public int insert() throws SQLException {
		return builder.insert(tablename, args);
	}

	public String toUpdateSQL(List<Object> outputArgs) {
		return builder.toUpdateSQL(tablename, args, outputArgs);
	}

	public String toUpdateSQL() {
		return toUpdateSQL(new ArrayList<Object>());
	}

	public String toInsertSQL(List<Object> outputArgs) {
		return builder.toInsertSQL(tablename, args, outputArgs);
	}

	public String toInsertSQL() {
		return toInsertSQL(new ArrayList<Object>());
	}
}
