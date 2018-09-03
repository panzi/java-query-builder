package io.github.panzi.sql;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.github.panzi.sql.annotations.Field;
import io.github.panzi.sql.annotations.Mapping;
import io.github.panzi.sql.config.Config;
import io.github.panzi.sql.internal.ColumnName;
import io.github.panzi.sql.internal.Name;
import io.github.panzi.sql.internal.Util;

public class QueryBuilder extends QueryBuilderBase<QueryBuilder> {
	public static QueryBuilder query(Connection con) throws SQLException {
		return new QueryBuilder(con);
	}

	public static Name name(String name) {
		return new Name(name);
	}

	public static ColumnName column(String name) {
		return new ColumnName(name);
	}

	public static Order asc(String column) {
		return new Order(column, true);
	}

	public static Order desc(String column) {
		return new Order(column, false);
	}

	public QueryBuilder(Config config) {
		super(config);
	}

	public QueryBuilder(Connection con) throws SQLException {
		super(con);
	}
	
	protected QueryBuilder(QueryBuilder builder, QueryFragment[] where) {
		super(builder, where);
	}

	public SelectBuilder<?> select() {
		return new SelectBuilder<>(this, null, null, null, null, null, -1, -1);
	}

	public SelectBuilder<?> select(String select) {
		return new SelectBuilder<>(this, select, null, null, null, null, -1, -1);
	}

	public SelectBuilder<?> columns(String... columns) {
		return select().columns(columns);
	}

	public SelectBuilder<?> from(String tablename) {
		return new SelectBuilder<>(this, null, tablename, null, null, null, -1, -1);
	}

	public<T> SelectBuilder<T> from(Class<T> cls) {
		return new SelectBuilder<>(this, null, null, cls, null, null, -1, -1);
	}

	public<T> SelectBuilder<T> from(Class<T> cls, String tablename) {
		return new SelectBuilder<>(this, null, tablename, cls, null, null, -1, -1);
	}
	
	public SelectBuilder<?> include(String... include) {
		return new SelectBuilder<>(this, null, null, null, include, null, -1, -1);
	}

	public SelectBuilder<?> order(Order... order) {
		return select().order(order);
	}

	public SelectBuilder<?> order(String... order) {
		return select().order(order);
	}

	public SelectBuilder<?> orderSQL(String order) {
		return new SelectBuilder<>(this, null, null, null, null, order, -1, -1);
	}

	@Override
	protected QueryBuilder where(QueryFragment[] where) {
		return new QueryBuilder(this, where);
	}

	public UpdateArgs into(Class<?> cls) {
		return new UpdateArgs(this, Util.getTableName(cls));
	}

	public UpdateArgs into(String tablename) {
		return new UpdateArgs(this, tablename);
	}

	public int update(Object object) throws SQLException {
		return update(Util.getTableName(object.getClass()), object);
	}

	public String toUpdateSQL(Class<?> cls, Map<String, Object> values) {
		String tablename = Util.getTableName(cls);
		List<Object> outputArgs = new ArrayList<>();
		return toUpdateSQL(tablename, values, outputArgs);
	}

	public String toInsertSQL(Class<?> cls, Map<String, Object> values) {
		String tablename = Util.getTableName(cls);
		List<Object> outputArgs = new ArrayList<>();
		return toInsertSQL(tablename, values, outputArgs);
	}

	public String toUpdateSQL(Object object) {
		Map<String, Object> values = getValues(object);
		Object id = values.remove("id");
		return whereIs("id", id).toUpdateSQL(object.getClass(), values);
	}

	public String toInsertSQL(Object object) {
		String tablename = Util.getTableName(object.getClass());
		Map<String, Object> values = getValues(object);
		List<Object> outputArgs = new ArrayList<>();
		return toInsertSQL(tablename, values, outputArgs);
	}

	protected Map<String, Object> getValues(Object object) {
		Map<String, Object> values = new HashMap<>();
		Class<?> cls = object.getClass();
		Map<String, Field> fieldDefs = Util.getFields(cls);
		boolean onlyDeclared = Util.getOnlyDeclared(cls);

		for (Method method : cls.getMethods()) {
			String name = method.getName();
			if (
					method.getParameterTypes().length == 0 &&
					!method.isVarArgs() &&
					method.getDeclaringClass() != Object.class &&
					name.length() > 3 &&
					name.startsWith("get") &&
					Character.isUpperCase(name.charAt(3))) {
				String javaName = Character.toLowerCase(name.charAt(3)) + name.substring(4);
				Field field = fieldDefs.get(javaName);
				String columnName = "";
				Mapping mapping = Mapping.VALUE;
				if (field != null) {
					columnName = field.columnName();
					mapping = field.mapping();
					
					if (mapping != Mapping.BELONGS_TO) {
						continue;
					}
				} else if (onlyDeclared) {
					continue;
				}

				if (values.containsKey(columnName)) {
					continue;
				}

				if (columnName.length() == 0) {
					columnName = Util.toSnakeCase(javaName);
					if (mapping == Mapping.BELONGS_TO) {
						columnName += "_id";
					}
				}

				Object value;
				try {
					value = method.invoke(object);
				} catch (IllegalAccessException | IllegalArgumentException e) {
					// didn't work, hope there's another getter or field with a compatible type
					continue;
				} catch (InvocationTargetException e) {
					throw new RuntimeException(e);
				}

				if (value != null && mapping == Mapping.BELONGS_TO) {
					value = Util.getId(value);
				}

				values.put(columnName, value);
			}
		}

		for (java.lang.reflect.Field objField : cls.getFields()) {
			if ((objField.getModifiers() & Modifier.TRANSIENT) == 0) {
				String javaName = objField.getName();
				Field field = fieldDefs.get(javaName);
				String columnName = "";
				Mapping mapping = Mapping.VALUE;
				if (field != null) {
					columnName = field.columnName();
					mapping = field.mapping();
					
					if (mapping != Mapping.BELONGS_TO) {
						continue;
					}
				} else if (onlyDeclared) {
					continue;
				}

				if (values.containsKey(columnName)) {
					continue;
				}

				if (columnName.length() == 0) {
					columnName = Util.toSnakeCase(javaName);
					if (mapping == Mapping.BELONGS_TO) {
						columnName += "_id";
					}
				}

				Object value;
				try {
					value = objField.get(object);
				} catch (IllegalAccessException | IllegalArgumentException e) {
					// didn't work, hope there's another field with a compatible type
					continue;
				}

				if (value != null && mapping == Mapping.BELONGS_TO) {
					value = Util.getId(value);
				}

				values.put(columnName, value);
			}
		}

		return values;
	}

	public int update(String tablename, Object object) throws SQLException {
		Map<String, Object> values = getValues(object);
		Object id = values.remove("id");
		return whereIs("id", id).update(tablename, values);
	}

	public int update(Class<?> cls, Map<String, Object> values) throws SQLException {
		return update(Util.getTableName(cls), values);
	}

	public int update(String tablename, Map<String, Object> values) throws SQLException {
		List<Object> args = new ArrayList<>();
		String sql = toUpdateSQL(tablename, values, args);

		try (PreparedStatement stmt = Util.prepare(con, sql, args)) {
			return stmt.executeUpdate();
		}
	}

	public int insert(Class<?> cls, Map<String, Object> values) throws SQLException {
		return insert(Util.getTableName(cls), values);
	}

	public int insert(Object object) throws SQLException {
		return insert(Util.getTableName(object.getClass()), object);
	}

	public int insert(String tablename, Object object) throws SQLException {
		return insert(tablename, getValues(object));
	}

	public String toUpdateSQL(String tablename, Map<String, Object> values, List<Object> outputArgs) {
		if (values.isEmpty()) {
			throw new IllegalArgumentException("no UPDATE values supplied");
		}
		StringBuilder buf = new StringBuilder();

		buf.append("UPDATE ");
		config.escapeName(tablename, buf);
		buf.append(" SET ");
		
		boolean first = true;
		for (Entry<String, Object> entry : values.entrySet()) {
			if (first) {
				first = false;
			} else {
				buf.append(", ");
			}
			config.escapeName(entry.getKey(), buf);
			buf.append(" = ");
			QueryFragment.addArg(config, tablename, buf, outputArgs, entry.getValue(), false);
		}

		if (where != null && where.length > 0) {
			buf.append(" WHERE (");
			first = true;
			for (QueryFragment fragment : where) {
				if (first) {
					first = false;
				} else {
					buf.append(") AND (");
				}
				fragment.generate(config, tablename, buf, outputArgs);
			}
			buf.append(')');
		}

		return buf.toString();
	}

	// TODO: return new ID
	public String toInsertSQL(String tablename, Map<String, Object> values, List<Object> outputArgs) {
		if (values.isEmpty()) {
			throw new IllegalArgumentException("no INSERT values supplied");
		}

		if (where != null && where.length > 0) {
			throw new IllegalArgumentException("INSERT has no WHERE clause");
		}

		StringBuilder buf = new StringBuilder();
		
		buf.append("INSERT INTO ");
		config.escapeName(tablename, buf);
		buf.append(" (");
		
		boolean first = true;
		for (String column : values.keySet()) {
			if (first) {
				first = false;
			} else {
				buf.append(", ");
			}
			config.escapeName(column, buf);
		}
		
		buf.append(") VALUES (");
		first = true;
		for (Object value : values.values()) {
			if (first) {
				first = false;
			} else {
				buf.append(", ");
			}
			QueryFragment.addArg(config, tablename, buf, outputArgs, value, false);
		}
		buf.append(")");
		
		return buf.toString();
	}

	public int insert(String tablename, Map<String, Object> values) throws SQLException {
		List<Object> args = new ArrayList<>();
		String sql = toInsertSQL(tablename, values, args);

		try (PreparedStatement stmt = Util.prepare(con, sql, args)) {
			return stmt.executeUpdate();
		}
	}

	public<T> T first(Class<T> cls) throws SQLException {
		return from(cls).first();
	}
	
	public<T> List<T> all(Class<T> cls) throws SQLException {
		return from(cls).all();
	}
	
	public ResultSet execute() throws SQLException {
		return select().execute();
	}

	public String toSQL(List<Object> args) {
		return select().toSQL(args);
	}
}
