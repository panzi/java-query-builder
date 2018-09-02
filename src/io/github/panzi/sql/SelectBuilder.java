package io.github.panzi.sql;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.github.panzi.sql.config.Config;

public class SelectBuilder<T> extends QueryBuilderBase<SelectBuilder<T>> {
	private final String tablename;
	private final Class<T> cls;
	private final String select;
	private final String[] include;
	private final String order;
	private final long offset;
	private final long limit;

	public SelectBuilder(Config config) {
		super(config);
		tablename = null;
		cls = null;
		select = null;
		include = null;
		order = null;
		offset = -1;
		limit = -1;
	}

	public SelectBuilder(Connection con) throws SQLException {
		super(con);
		tablename = null;
		cls = null;
		select = null;
		include = null;
		order = null;
		offset = -1;
		limit = -1;
	}

	public SelectBuilder(SelectBuilder<T> builder, QueryFragment[] where) {
		super(builder, where);

		this.tablename = builder.tablename;
		this.cls = builder.cls;
		this.select = builder.select;
		this.include = builder.include;
		this.order = builder.order;
		this.offset = builder.offset;
		this.limit = builder.limit;
	}

	public SelectBuilder(QueryBuilderBase<?> builder, String select, String tablename, Class<T> cls, String[] include, String order, long offset, long limit) {
		super(builder);

		this.select = select;
		this.tablename = tablename;
		this.cls = cls;
		this.include = include;
		this.order = order;
		this.offset = offset;
		this.limit = limit;
	}

	public SelectBuilder<T> select(String select) {
		if (select == null) {
			return new SelectBuilder<T>(this, null, tablename, cls, include, order, offset, limit);
		}

		select = select.trim();

		if (select.length() == 0 || this.select == null) {
			return new SelectBuilder<T>(this, this.select, tablename, cls, include, order, offset, limit);
		}

		return new SelectBuilder<T>(this, this.select + ", " + select, tablename, cls, include, order, offset, limit);
	}
	
	private String getTableName() {
		String tablename = this.tablename;
		if (tablename == null && cls != null) {
			tablename = Util.getTableName(cls);
		}
		if (tablename == null) {
			throw new IllegalArgumentException("table name is not defined");
		}
		return tablename;
	}

	public SelectBuilder<T> columns(String... columns) {
		String tablename = getTableName();
		StringBuilder buf = new StringBuilder();

		if (columns == null) {
			if (select != null) {
				buf.append(select);
				buf.append(", ");
			}
			config.escapeName(tablename, buf);
			buf.append(".*");
		} else {
			boolean first = true;
			if (select != null) {
				buf.append(select);
				if (columns.length > 0) {
					buf.append(", ");
				}
				first = false;
			}
			for (String column : columns) {
				if (first) {
					first = false;
				} else {
					buf.append(", ");
				}

				config.escapeName(tablename, buf);
				if (column == null) {
					buf.append(".*");
				} else {
					buf.append('.');
					config.escapeName(column, buf);
				}
			}
		}
		return new SelectBuilder<T>(this, buf.toString(), tablename, cls, include, order, offset, limit);
	}

	public SelectBuilder<T> include(String... include) {
		String[] newInclude;
		if (this.include != null) {
			newInclude = new String[this.include.length + include.length];
			System.arraycopy(this.include, 0, newInclude, 0, this.include.length);
			System.arraycopy(include, 0, newInclude, this.include.length, include.length);
		} else {
			newInclude = include;
		}
		
		return new SelectBuilder<T>(this, select, tablename, cls, newInclude, order, offset, limit);
	}

	public SelectBuilder<T> from(String tablename) {
		return new SelectBuilder<T>(this, select, tablename, cls, include, order, offset, limit);
	}

	public<NewT> SelectBuilder<NewT> from(Class<NewT> cls) {
		return new SelectBuilder<NewT>(this, select, tablename, cls, include, order, offset, limit);
	}

	@Override
	protected SelectBuilder<T> where(QueryFragment[] where) {
		return new SelectBuilder<T>(this, where);
	}

	public SelectBuilder<T> orderSQL(String order) {
		return new SelectBuilder<T>(this, select, tablename, cls, include, order, offset, limit);
	}
	
	public SelectBuilder<T> order(String... order) {
		String tablename = getTableName();
		StringBuilder buf = new StringBuilder();
		boolean first = true;
		for (String column : order) {
			if (first) {
				first = false;
			} else {
				buf.append(", ");
			}
			config.escapeName(tablename, buf);
			buf.append('.');
			config.escapeName(column, buf);
		}
		return orderSQL(buf.toString());
	}
	
	public SelectBuilder<T> order(Order... order) {
		StringBuilder buf = new StringBuilder();
		boolean first = true;
		for (Order item : order) {
			if (first) {
				first = false;
			} else {
				buf.append(", ");
			}
			config.escapeName(tablename, buf);
			buf.append('.');
			config.escapeName(item.getColumn(), buf);
			buf.append(item.isAscending() ? " ASC" : " DESC");
		}
		return orderSQL(buf.toString());
	}
	
	public SelectBuilder<T> offset(long offset) {
		return new SelectBuilder<T>(this, select, tablename, cls, include, order, offset, limit);
	}

	public SelectBuilder<T> limit(long limit) {
		return new SelectBuilder<T>(this, select, tablename, cls, include, order, offset, limit);
	}

	public T find(Object id) throws SQLException {
		T object = whereIs("id", id).first();
		if (object == null) {
			throw new RecordNotFoundException("could not find " + cls.getSimpleName() + " with ID=" + id);
		}
		return object;
	}

	public List<T> findAll(Object... ids) throws SQLException {
		return where("? in (?)", new ColumnName("id"), ids).all();
	}

	public ResultSet find(Object... ids) throws SQLException {
		return where("? in (?)", new ColumnName("id"), ids).execute();
	}

	public T first() throws SQLException {
		return first(cls);
	}

	@SuppressWarnings("unchecked")
	static private<T> T fetch(Class<T> cls, ResultSet rs, Set<String> columns) throws SQLException {
		try {
			if (cls == Byte.class || cls == byte.class) {
				return (T)(Byte)rs.getByte(1);
			} else if (cls == Short.class || cls == short.class) {
				return (T)(Short)rs.getShort(1);
			} else if (cls == Integer.class || cls == int.class) {
				return (T)(Integer)rs.getInt(1);
			} else if (cls == Long.class || cls == long.class) {
				return (T)(Long)rs.getLong(1);
			} else if (cls == Float.class || cls == float.class) {
				return (T)(Float)rs.getFloat(1);
			} else if (cls == Double.class || cls == double.class) {
				return (T)(Double)rs.getDouble(1);
			} else if (cls == Boolean.class || cls == boolean.class) {
				return (T)(Boolean)rs.getBoolean(1);
			} else if (cls == Character.class || cls == char.class) {
				return (T)(Character)rs.getString(1).charAt(0);
			} else if (cls == Object.class) {
				return (T)rs.getObject(1);
			} else if (cls == String.class) {
				return (T)rs.getString(1);
			} else if (cls == java.util.Date.class || cls == java.util.Date.class) {
				return (T)rs.getDate(1);
			} else if (cls == Blob.class) {
				return (T)rs.getBlob(1);
			} else if (cls == BigDecimal.class) {
				return (T)rs.getBigDecimal(1);
			} else if (cls.isArray()) {
				if (cls == byte[].class) {
					byte[] array = new byte[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getByte(index + 1);
					}
					return (T)array;
				} else if (cls == short[].class) {
					short[] array = new short[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getShort(index + 1);
					}
					return (T)array;
				} else if (cls == int[].class) {
					int[] array = new int[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getInt(index + 1);
					}
					return (T)array;
				} else if (cls == long[].class) {
					long[] array = new long[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getLong(index + 1);
					}
					return (T)array;
				} else if (cls == float[].class) {
					float[] array = new float[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getFloat(index + 1);
					}
					return (T)array;
				} else if (cls == double[].class) {
					double[] array = new double[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getDouble(index + 1);
					}
					return (T)array;
				} else if (cls == String[].class) {
					String[] array = new String[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getString(index + 1);
					}
					return (T)array;
				} else if (cls == char[].class) {
					char[] array = new char[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getString(index + 1).charAt(0);
					}
					return (T)array;
				} else if (cls == java.util.Date[].class) {
					java.util.Date[] array = new java.util.Date[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getDate(index + 1);
					}
					return (T)array;
				} else if (cls == java.sql.Date[].class) {
					java.sql.Date[] array = new java.sql.Date[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getDate(index + 1);
					}
					return (T)array;
				} else if (cls == Object[].class) {
					Object[] array = new Object[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getObject(index + 1);
					}
					return (T)array;
				} else if (cls == Byte[].class) {
					Byte[] array = new Byte[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getByte(index + 1);
					}
					return (T)array;
				} else if (cls == Short[].class) {
					Short[] array = new Short[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getShort(index + 1);
					}
					return (T)array;
				} else if (cls == Integer[].class) {
					Integer[] array = new Integer[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getInt(index + 1);
					}
					return (T)array;
				} else if (cls == Long[].class) {
					Long[] array = new Long[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getLong(index + 1);
					}
					return (T)array;
				} else if (cls == Float[].class) {
					Float[] array = new Float[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getFloat(index + 1);
					}
					return (T)array;
				} else if (cls == Double[].class) {
					Double[] array = new Double[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getDouble(index + 1);
					}
					return (T)array;
				} else if (cls == Character[].class) {
					Character[] array = new Character[columns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = rs.getString(index + 1).charAt(0);
					}
					return (T)array;
				}
				throw new IllegalArgumentException("unhandeled type: " + cls.getName());
			} else if (cls.isPrimitive()) {
				throw new IllegalArgumentException("unhandeled type: " + cls.getName());
			}

			T object = cls.newInstance();
			load(object, rs, columns);
			return object;
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
	}

	public<NewT> NewT first(Class<NewT> cls) throws SQLException {
		try (ResultSet rs = execute()) {
			if (rs.next()) {
				Set<String> columns = getColumns(rs);
				return fetch(cls, rs, columns);
			}
			return null;
		}
	}

	public void load(T object) throws SQLException {
		if (object == null) {
			throw new IllegalArgumentException("object may not be null");
		}

		Class<?> cls = this.cls;
		if (cls == null) {
			cls = object.getClass();
		}

		try (ResultSet rs = execute()) {
			if (rs.next()) {
				Set<String> columns = getColumns(rs);
				load(object, rs, columns);
				return;
			}
		}
		throw new RecordNotFoundException();
	}

	public List<T> all() throws SQLException {
		List<T> result = new ArrayList<>();
		try (ResultSet rs = execute()) {
			Set<String> columns = getColumns(rs);
			while (rs.next()) {
				result.add(fetch(cls, rs, columns));
			}
		}
		return result;
	}
	
	private static Set<String> getColumns(ResultSet rs) throws SQLException {
		Set<String> columns = new HashSet<>();
		ResultSetMetaData meta = rs.getMetaData();
		for (int i = 1, n = meta.getColumnCount(); i <= n; ++ i) {
			columns.add(meta.getColumnName(i));
		}
		
		return columns;
	}
	
	private static void load(Object object, ResultSet data, Set<String> columns) throws SQLException {
		Class<?> cls = object.getClass();
		while (cls != null && cls != Object.class) {
			for (Field field : cls.getDeclaredFields()) {
				if ((field.getModifiers() & Modifier.TRANSIENT) == 0) {
					String name = Util.toSnakeCase(field.getName());
					if (columns.contains(name)) {
						Object value = data.getObject(name);
						if (value != null || !field.getType().isPrimitive()) {
							try {
								field.set(object, value);
							} catch (IllegalAccessException e) {
								throw new RuntimeException(e);
							}
						}
					}
				}
			}
			cls = cls.getSuperclass();
		}
	}

	public ResultSet execute() throws SQLException {
		return prepare().executeQuery();
	}

	public String toSQL(List<Object> args) {
		String tablename = getTableName();

		StringBuilder buf = new StringBuilder();
		buf.append("SELECT ");
		if (select == null) {
			config.escapeName(tablename, buf);
			buf.append(".*");
		} else {
			buf.append(select);
		}
		buf.append(" FROM ");
		config.escapeName(tablename, buf);

		// TODO: include

		if (where != null && where.length > 0) {
			buf.append(" WHERE (");
			boolean first = true;
			for (QueryFragment fragment : where) {
				if (first) {
					first = false;
				} else {
					buf.append(") AND (");
				}
				fragment.generate(config, tablename, buf, args);
			}
			buf.append(')');
		}

		String order = this.order;
		if (order != null && (order = order.trim()).length() > 0) {
			buf.append(" ORDER BY ");
			buf.append(order);
		}

		if (offset >= 0) {
			buf.append(" OFFSET ");
			buf.append(offset);
		}

		if (limit >= 0) {
			buf.append(" LIMIT ");
			buf.append(limit);
		}

		return buf.toString();
	}
}
