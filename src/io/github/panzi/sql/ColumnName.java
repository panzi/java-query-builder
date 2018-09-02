package io.github.panzi.sql;

import io.github.panzi.sql.config.Config;

public class ColumnName extends Name {
	public ColumnName(String name) {
		super(name);
	}

	public void generate(Config config, String tablename, StringBuilder output) {
		config.escapeName(tablename, output);
		output.append('.');
		config.escapeName(toString(), output);
	}
}
