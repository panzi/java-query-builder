package io.github.panzi.sql;

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
import io.github.panzi.sql.internal.ColumnName;
import io.github.panzi.sql.internal.LoadContext;
import io.github.panzi.sql.internal.Util;

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

	public<NewT> SelectBuilder<NewT> from(Class<NewT> cls, String tablename) {
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
	private<Type> Type fetch(Class<Type> cls, LoadContext ctx) throws SQLException {
		try {
			if (cls == Byte.class || cls == byte.class) {
				return (Type)(Byte)ctx.data.getByte(1);
			} else if (cls == Short.class || cls == short.class) {
				return (Type)(Short)ctx.data.getShort(1);
			} else if (cls == Integer.class || cls == int.class) {
				return (Type)(Integer)ctx.data.getInt(1);
			} else if (cls == Long.class || cls == long.class) {
				return (Type)(Long)ctx.data.getLong(1);
			} else if (cls == Float.class || cls == float.class) {
				return (Type)(Float)ctx.data.getFloat(1);
			} else if (cls == Double.class || cls == double.class) {
				return (Type)(Double)ctx.data.getDouble(1);
			} else if (cls == Boolean.class || cls == boolean.class) {
				return (Type)(Boolean)ctx.data.getBoolean(1);
			} else if (cls == Character.class || cls == char.class) {
				return (Type)(Character)ctx.data.getString(1).charAt(0);
			} else if (cls == Object.class) {
				return cls.cast(ctx.data.getObject(1));
			} else if (cls == String.class) {
				return cls.cast(ctx.data.getString(1));
			} else if (cls == java.util.Date.class || cls == java.util.Date.class) {
				return cls.cast(ctx.data.getDate(1));
			} else if (cls == Blob.class) {
				return cls.cast(ctx.data.getBlob(1));
			} else if (cls == BigDecimal.class) {
				return cls.cast(ctx.data.getBigDecimal(1));
			} else if (cls.isArray()) {
				if (cls == byte[].class) {
					return cls.cast(ctx.data.getBytes(1));
				} else if (cls == short[].class) {
					short[] array = new short[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getShort(index + 1);
					}
					return cls.cast(array);
				} else if (cls == int[].class) {
					int[] array = new int[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getInt(index + 1);
					}
					return cls.cast(array);
				} else if (cls == long[].class) {
					long[] array = new long[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getLong(index + 1);
					}
					return cls.cast(array);
				} else if (cls == float[].class) {
					float[] array = new float[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getFloat(index + 1);
					}
					return cls.cast(array);
				} else if (cls == double[].class) {
					double[] array = new double[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getDouble(index + 1);
					}
					return cls.cast(array);
				} else if (cls == String[].class) {
					String[] array = new String[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getString(index + 1);
					}
					return cls.cast(array);
				} else if (cls == char[].class) {
					return cls.cast(ctx.data.getString(1).toCharArray());
				} else if (cls == java.util.Date[].class) {
					java.util.Date[] array = new java.util.Date[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getDate(index + 1);
					}
					return cls.cast(array);
				} else if (cls == java.sql.Date[].class) {
					java.sql.Date[] array = new java.sql.Date[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getDate(index + 1);
					}
					return cls.cast(array);
				} else if (cls == Object[].class) {
					Object[] array = new Object[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getObject(index + 1);
					}
					return cls.cast(array);
				} else if (cls == Byte[].class) {
					Byte[] array = new Byte[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getByte(index + 1);
					}
					return cls.cast(array);
				} else if (cls == Short[].class) {
					Short[] array = new Short[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getShort(index + 1);
					}
					return cls.cast(array);
				} else if (cls == Integer[].class) {
					Integer[] array = new Integer[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getInt(index + 1);
					}
					return cls.cast(array);
				} else if (cls == Long[].class) {
					Long[] array = new Long[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getLong(index + 1);
					}
					return cls.cast(array);
				} else if (cls == Float[].class) {
					Float[] array = new Float[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getFloat(index + 1);
					}
					return cls.cast(array);
				} else if (cls == Double[].class) {
					Double[] array = new Double[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getDouble(index + 1);
					}
					return cls.cast(array);
				} else if (cls == Character[].class) {
					Character[] array = new Character[ctx.availableColumns.size()];
					for (int index = 0; index < array.length; ++ index) {
						array[index] = ctx.data.getString(index + 1).charAt(0);
					}
					return cls.cast(array);
				}
				throw new IllegalArgumentException("unhandeled type: " + cls.getName());
			} else if (cls.isPrimitive()) {
				throw new IllegalArgumentException("unhandeled type: " + cls.getName());
			}

			Type object = cls.newInstance();
			ctx.load(object);
			return object;
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
	}
	
	private Set<String> getInclude() {
		Set<String> include = new HashSet<>();
		if (this.include != null) {
			for (String field : this.include) {
				include.add(field);
			}
		}
		return include;
	}

	public<NewT> NewT first(Class<NewT> cls) throws SQLException {
		try (ResultSet rs = execute()) {
			if (rs.next()) {
				Set<String> columns = getColumns(rs);
				Set<String> include = getInclude();
				LoadContext ctx = new LoadContext(con, rs, columns, include);
				return fetch(cls, ctx);
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
				Set<String> include = getInclude();
				LoadContext ctx = new LoadContext(con, rs, columns, include);
				ctx.load(object);
				return;
			}
		}
		throw new RecordNotFoundException();
	}

	public List<T> all() throws SQLException {
		List<T> result = new ArrayList<>();
		try (ResultSet rs = execute()) {
			Set<String> columns = getColumns(rs);
			Set<String> include = getInclude();
			LoadContext ctx = new LoadContext(con, rs, columns, include);
			while (rs.next()) {
				result.add(fetch(cls, ctx));
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
