package io.github.panzi.sql;

import java.util.List;
import java.util.Map;

import io.github.panzi.sql.config.Config;

public class NamedQueryFragment extends QueryFragment {
	private final Map<String, Object> args;
	
	public NamedQueryFragment(String query, Map<String, Object> args) {
		super(query);
		this.args = args;
	}

	@Override
	void generate(Config conf, String tablename, StringBuilder output, List<Object> outputArgs) {
		final int len = query.length();
		int prev = 0;
		for (;;) {
			final int sqlIndex = query.indexOf(':', prev);
			if (sqlIndex == -1) {
				break;
			}
			output.append(query, prev, sqlIndex);
			int nameBegin = sqlIndex + 1;
			int nameEnd = nameBegin;
			while (nameEnd < len) {
				char ch = query.charAt(nameEnd);
				if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
					++ nameEnd;
				} else {
					break;
				}
			}
			prev = nameEnd;
			String name = query.substring(nameBegin, nameEnd);
			Object arg = args.get(name);
			addArg(conf, tablename, output, outputArgs, arg, true);
		}
		output.append(query, prev, len);
	}
}
