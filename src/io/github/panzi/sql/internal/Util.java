package io.github.panzi.sql.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.panzi.sql.annotations.Field;
import io.github.panzi.sql.annotations.Meta;

public abstract class Util {
	private Util() {}
	
	private static final Map<Class<?>, Map<String, Field>> FIELD_DEFS = new HashMap<>();

	public static boolean getOnlyDeclared(Class<?> cls) {
		Class<?> type = cls;
		while (type != null) {
			Meta meta = cls.getAnnotation(Meta.class);
			if (meta != null) {
				return meta.onlyDeclared();
			}
			type = type.getSuperclass();
		}
		return false;
	}

	public static String getTableName(Class<?> cls) {
		Class<?> type = cls;
		while (type != null) {
			Meta meta = cls.getAnnotation(Meta.class);
			String name;
			if (meta != null && (name = meta.tableName()).length() > 0) {
				return name;
			}
			type = type.getSuperclass();
		}

		if (cls.isArray() || cls.isAnonymousClass() || cls.isPrimitive()) {
			throw new IllegalArgumentException("cannot derive table name from " + cls.getName());
		}
		String name = cls.getSimpleName();
		StringBuilder buf = new StringBuilder();
		toSnakeCase(name, buf);
		buf.append(name.endsWith("s") ? "es" : "s");
		return buf.toString();
	}
	
	public static Map<String, Field> getFields(Class<?> cls) {
		synchronized (FIELD_DEFS) {
			Map<String, Field> fields = FIELD_DEFS.get(cls);
			
			if (fields != null) {
				return fields;
			}

			fields = new HashMap<>();
			FIELD_DEFS.put(cls, fields);

			while (cls != null) {
				Meta meta = cls.getAnnotation(Meta.class);
				if (meta != null) {
					for (Field field : meta.fields()) {
						if (!fields.containsKey(field.name())) {
							fields.put(field.name(), field);
						}
					}
				}
				cls = cls.getSuperclass();
			}

			return fields;
		}
	}

	public static String getForeignKey(Class<?> cls) {
		if (cls.isArray() || cls.isAnonymousClass() || cls.isPrimitive()) {
			throw new IllegalArgumentException("cannot derive foreign key name from " + cls.getName());
		}
		String name = cls.getSimpleName();
		StringBuilder buf = new StringBuilder();
		toSnakeCase(name, buf);
		buf.append("_id");
		return buf.toString();
	}
	
	public static String toSnakeCase(String str) {
		StringBuilder buf = new StringBuilder();
		toSnakeCase(str, buf);
		return buf.toString();
	}

	public static void toSnakeCase(String str, StringBuilder output) {
		int prev = 0;
		int len = str.length();
		for (int index = 0; index < len; ++ index) {
			char ch = str.charAt(index);
			if (Character.isUpperCase(ch)) {
				if (index > 0) {
					output.append(str, prev, index);
					output.append('_');
				}
				output.append(Character.toLowerCase(ch));
				prev = index + 1;
			}
		}
		output.append(str, prev, len);
	}

	public static String toCamelCase(String str) {
		StringBuilder buf = new StringBuilder();
		toCamelCase(str, buf);
		return buf.toString();
	}

	public static void toCamelCase(String str, StringBuilder output) {
		int prev = 0;
		int len = str.length();
		for (int index = 0; index < len; ++ index) {
			char ch = str.charAt(index);
			if (ch == '_') {
				++ index;
				if (index < len) {
					output.append(Character.toUpperCase(str.charAt(index)));
				}
				prev = index + 1;
			}
		}
		output.append(str, prev, len);
	}

	public static PreparedStatement prepare(Connection con, String sql, List<Object> args) throws SQLException {
		PreparedStatement stmt = con.prepareStatement(sql);
		try {
			for (int index = 0; index < args.size(); ++ index) {
				Object arg = args.get(index);
				stmt.setObject(index + 1, arg instanceof Calendar ? ((Calendar) arg).getTime() : arg);
			}
			return stmt;
		} catch (SQLException e) {
			stmt.close();
			throw e;
		} catch (RuntimeException e) {
			stmt.close();
			throw e;
		}
	}
	
	public static Object getId(Object object) {
		Class<?> cls = object.getClass();
		try {
			Method idMethod;
			try {
				idMethod = cls.getMethod("getId");
			} catch (NoSuchMethodException e) {
				java.lang.reflect.Field idField = cls.getField("id");
				return idField.get(object);
			}
			return idMethod.invoke(object);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | NoSuchFieldException e) {
			throw new RuntimeException(e);
		}
	}
}
