package io.github.panzi.sql.config;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class Config {
	public static final Config SQL99 = new SQL99Config();
	public static final Config MYSQL = new MySQLConfig();
	public static final Config DB2   = new DB2Config();
	public static final Config POSTGRE_SQL          = new PostgreSQLConfig();
	public static final Config MICROSOFT_SQL_SERVER = new MicrosoftSQLServerConfig();
	
	/**
	 * SQL99 table/column name quoting.
	 * 
	 * @param name
	 * @param output
	 */
	public abstract void escapeName(String name, StringBuilder output);

	public abstract void arrayPattern(int length, StringBuilder output);

	public static Config getConfig(Connection con) throws SQLException {
		String dbname = con.getMetaData().getDatabaseProductName();
		return getConfig(dbname);
	}

	public static Config getConfig(String databaseProductName) throws SQLException {
		if (databaseProductName.equals("MySQL")) {
			return MYSQL;
		} else if (databaseProductName.equals("PostgreSQL")) {
			return POSTGRE_SQL;
		} else if (databaseProductName.equals("Microsoft SQL Server")) {
			return MICROSOFT_SQL_SERVER;
		} else if (databaseProductName.equals("SQLite")) {
			return SQL99;
		} else if (databaseProductName.equals("Oracle")) {
			return SQL99;
		} else if (databaseProductName.startsWith("DB2/")) {
			return DB2;
		}
		
		throw new SQLException("unknown database product name: " + databaseProductName);
	}
}
