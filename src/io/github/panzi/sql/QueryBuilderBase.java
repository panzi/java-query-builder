package io.github.panzi.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.github.panzi.sql.config.Config;

public abstract class QueryBuilderBase<T extends QueryBuilderBase<?>> {
	protected final Connection con;
	protected final Config config;
	protected final QueryFragment[] where;

	public QueryBuilderBase(Config config) {
		this.con = null;
		this.config = config;
		this.where = null;
	}

	public QueryBuilderBase(Connection con) throws SQLException {
		this.con = con;
		this.config = Config.getConfig(con);
		this.where = null;
	}
	
	protected QueryBuilderBase(QueryBuilderBase<?> other) {
		this.con = other.con;
		this.config = other.config;
		this.where = other.where;
	}
	
	protected QueryBuilderBase(QueryBuilderBase<?> other, QueryFragment[] where) {
		this.con = other.con;
		this.config = other.config;
		this.where = where;
	}

	public NamedArgs<T> where(String query) {
		return new NamedArgs<T>(query, this);
	}

	protected abstract T where(QueryFragment[] where);

	protected T where(QueryFragment fragment) {
		QueryFragment[] newWhere;
		if (where != null) {
			newWhere = new QueryFragment[where.length + 1];
			System.arraycopy(where, 0, newWhere, 0, where.length);
			newWhere[where.length] = fragment;
		} else {
			newWhere = new QueryFragment[] { fragment };
		}
		return where(newWhere);
	}

	public T where(String query, Map<String, Object> args) {
		return where(new NamedQueryFragment(query, args));
	}

	public T where(String query, Object... args) {
		return where(new PositionalQueryFragment(query, args));
	}

	public T whereIs(String column, Object value) {
		String query;
		Object[] args;
		if (value == null) {
			query = "? IS NULL";
			args = new Object[] { new ColumnName(column) };
		} else {
			query = "? = ?";
			args = new Object[] { new ColumnName(column), value };
		}
		return where(new PositionalQueryFragment(query, args));
	}

	abstract public String toSQL(List<Object> args);

	public String toSQL() {
		return toSQL(new ArrayList<Object>());
	}

	public PreparedStatement prepare() throws SQLException {
		List<Object> args = new ArrayList<>();
		return Util.prepare(con, toSQL(args), args);
	}
}
