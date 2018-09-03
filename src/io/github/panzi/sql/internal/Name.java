package io.github.panzi.sql.internal;

import io.github.panzi.sql.config.Config;

public class Name {
	private final String name;

	public Name(String name) {
		this.name = name;
	}

	public String toString() {
		return name;
	}

	public void generate(Config config, String tablename, StringBuilder output) {
		config.escapeName(name, output);
	}
}