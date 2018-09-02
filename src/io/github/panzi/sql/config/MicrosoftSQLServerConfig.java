package io.github.panzi.sql.config;

public class MicrosoftSQLServerConfig extends Config {
	@Override
	public void escapeName(String name, StringBuilder output) {
		output.append('[');
		int prev = 0;
		for (;;) {
			int index = name.indexOf(']', prev);
			if (index == -1) {
				break;
			}
			output.append(name, prev, index);
			output.append("]]");
			prev = index + 1;
		}
		output.append(name, prev, name.length());
		output.append(']');
	}

	public void arrayPattern(int length, StringBuilder output) {
		throw new UnsupportedOperationException("arrays are not supported by Microsoft SQL Server");
	}
}
