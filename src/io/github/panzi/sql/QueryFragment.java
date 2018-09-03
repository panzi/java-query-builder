package io.github.panzi.sql;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;

import io.github.panzi.sql.config.Config;
import io.github.panzi.sql.internal.Name;

public abstract class QueryFragment {
	protected final String query;
	
	public QueryFragment(String query) {
		this.query = query;
	}
	
	abstract void generate(Config conf, String tablename, StringBuilder output, List<Object> outputArgs);

	public static void addArg(Config conf, String tablename, StringBuilder output, List<Object> outputArgs, Object arg, boolean arrayAsInClause) {
		if (arg != null) {
			if (arg.getClass().isArray()) {
				int arrayLen = Array.getLength(arg);
				if (arrayAsInClause) {
					if (arrayLen > 0) {
						output.append('?');
						outputArgs.add(Array.get(arg, 0));

						for (int i = 1; i < arrayLen; ++ i) {
							output.append(", ?");
							outputArgs.add(Array.get(arg, i));
						}
					}
				} else {
					conf.arrayPattern(arrayLen, output);
					for (int i = 0; i < arrayLen; ++ i) {
						outputArgs.add(Array.get(arg, i));
					}
				}
			} else if (arg instanceof Collection<?>) {
				Collection<?> coll = (Collection<?>) arg;
				if (arrayAsInClause) {
					boolean first = true;
					for (Object item : coll) {
						if (first) {
							output.append('?');
						} else {
							output.append(", ?");
							first = false;
						}
						outputArgs.add(item);
					}
				} else {
					conf.arrayPattern(coll.size(), output);
					outputArgs.addAll(coll);
				}
			} else if (arg instanceof Name) {
				((Name) arg).generate(conf, tablename, output);
			} else {
				output.append('?');
				outputArgs.add(arg);
			}
		} else {
			output.append('?');
			outputArgs.add(arg);
		}
		
	}
}
