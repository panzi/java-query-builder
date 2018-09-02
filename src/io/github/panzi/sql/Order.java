package io.github.panzi.sql;

public class Order {
	private final String column;
	private final boolean asc;
	
	public Order(String column, boolean asc) {
		this.column = column;
		this.asc = asc;
	}
	
	public String getColumn() {
		return column;
	}
	
	public boolean isAscending() {
		return asc;
	}
}
