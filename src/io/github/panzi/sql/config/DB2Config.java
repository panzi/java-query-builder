package io.github.panzi.sql.config;

public class DB2Config extends SQL99Config {
	@Override
	public void arrayPattern(int length, StringBuilder output) {
		output.append("ARRAY[");
		if (length > 0) {
			output.append('?');
			for (int i = 1; i < length; ++ i) {
				output.append(", ?");
			}
		}
		output.append(']');
	}
}
