package io.github.panzi.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

public abstract class Util {
	private Util() {}
	
	public static String getTableName(Class<?> cls) {
		if (cls.isArray() || cls.isAnonymousClass() || cls.isPrimitive()) {
			throw new IllegalArgumentException("cannot derive table name from " + cls.getName());
		}
		String name = cls.getSimpleName();
		StringBuilder buf = new StringBuilder();
		toSnakeCase(name, buf);
		buf.append(name.endsWith("s") ? "es" : "s");
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
}
