package io.github.panzi.sql;

import java.util.List;

import io.github.panzi.sql.config.Config;

public class PositionalQueryFragment extends QueryFragment {
	private final Object[] args;
	
	public PositionalQueryFragment(String query, Object[] args) {
		super(query);
		this.args = args;
	}

	@Override
	void generate(Config conf, String tablename, StringBuilder output, List<Object> outputArgs) {
		int prev = 0;
		int argIndex = 0;
		for (;;) {
			final int sqlIndex = query.indexOf('?', prev);
			if (sqlIndex == -1) {
				break;
			}
			output.append(query, prev, sqlIndex);
			prev = sqlIndex + 1;
			Object arg = args[argIndex ++];
			addArg(conf, tablename, output, outputArgs, arg, true);
		}
		output.append(query, prev, query.length());
	}
}
