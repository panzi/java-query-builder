package io.github.panzi.sql.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(TYPE)
public @interface Meta {
	String tableName() default "";
	boolean onlyDeclared() default false;
	Field[] fields() default {};
}
