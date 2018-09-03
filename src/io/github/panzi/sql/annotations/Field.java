package io.github.panzi.sql.annotations;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;

@Retention(RUNTIME)
public @interface Field {
	String name();
	Mapping mapping() default Mapping.VALUE;
	Class<?> type() default void.class;
	String columnName() default "";
	String tableName() default "";
}
