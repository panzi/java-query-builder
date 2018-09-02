package io.github.panzi.sql;

import java.util.HashMap;
import java.util.Map;

public class NamedArgs<T extends QueryBuilderBase<?>> {
	private final String query;
	private final QueryBuilderBase<T> builder;
	private final Map<String, Object> map = new HashMap<>();
	
	public NamedArgs(String query, QueryBuilderBase<T> builder) {
		this.query = query;
		this.builder = builder;
	}
	
	public NamedArgs<T> set(String key, Object value) {
		map.put(key, value);
		return this;
	}

	public T done() {
		return builder.where(query, map);
	}
}
