package io.github.panzi.sql.internal;

import static io.github.panzi.sql.annotations.Mapping.BELONGS_TO;
import static io.github.panzi.sql.annotations.Mapping.HAS_MANY;
import static io.github.panzi.sql.annotations.Mapping.HAS_ONE;
import static io.github.panzi.sql.annotations.Mapping.VALUE;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.panzi.sql.QueryBuilder;
import io.github.panzi.sql.annotations.Field;
import io.github.panzi.sql.annotations.Mapping;

public class LoadContext {
	public Connection con;
	public Object object;
	public ResultSet data;
	public Set<String> availableColumns;
	public Set<String> include;
	public Object id = null;
	public boolean gotId = false;
	public Class<?> cls;
	public Map<String, Field> fieldDefs;
	public boolean onlyDeclared;

	public LoadContext(Connection con, ResultSet data, Set<String> availableColumns, Set<String> include) {
		this.con = con;
		this.data = data;
		this.availableColumns = availableColumns;
		this.include = include;
	}

	public Object getId() throws SQLException {
		if (availableColumns.contains("id")) {
			return data.getObject("id");
		} else {
			// get id from the object
			Class<?> cls = object.getClass();
			try {
				try {
					Method idMethod = cls.getMethod("getId");
					return idMethod.invoke(object);
				} catch (NoSuchMethodException e) {
					java.lang.reflect.Field idField = cls.getField("id");
					return idField.get(object);
				}
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchFieldException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public Object loadValue(String javaName, Class<?> declType, AnnotatedElement member) throws SQLException {
		Object value;
		Class<?> argType = void.class;
		Field field = fieldDefs.get(javaName);
		
		if (field == null && onlyDeclared) {
			return null;
		}

		String columnName = "";
		String tableName = "";
		Mapping mapping = VALUE;
		Class<?> type = void.class;
		
		if (field != null) {
			columnName = field.columnName();
			tableName = field.tableName();
			mapping = field.mapping();
			type = field.type();
		}

		if (mapping == HAS_ONE) {
			argType = type;

			if (!include.contains(javaName)) {
				return null;
			}

			if (columnName.equals("")) {
				columnName = Util.getForeignKey(cls);
			}

			if (argType == void.class) {
				argType = declType;
			}

			if (!gotId) {
				id = getId();
				gotId = true;
			}

			if (id == null) {
				return null;
			} else {
				value = QueryBuilder.query(con).from(argType, tableName).whereIs(columnName, id).first();
			}
		} else if (mapping == HAS_MANY) {
			Class<?> itemType = type;
			Class<?> containerType = argType = declType;

			if (!include.contains(javaName)) {
				return null;
			}

			if (itemType == void.class) {
				if (argType.isArray()) {
					itemType = argType.getComponentType();
				} else {
					throw new RuntimeException("Could not determine the component type of @HasMany relaion. You need to set the type field.");
				}
			}

			if (tableName.equals("")) {
				tableName = Util.getTableName(itemType);
			}

			if (columnName.equals("")) {
				columnName = Util.getForeignKey(cls);
			}

			if (containerType == List.class || containerType == Collection.class) {
				containerType = ArrayList.class;
			}

			if (!gotId) {
				id = getId();
				gotId = true;
			}

			if (id == null) {
				if (containerType.isArray()) {
					value = Array.newInstance(itemType, 0);
				} else {
					value = new ArrayList<>();
				}
			} else {
				List<?> values = QueryBuilder.query(con).from(itemType, tableName).whereIs(columnName, id).all();

				if (containerType.isArray()) {
					value = Array.newInstance(itemType, values.size());
					for (int index = 0; index < values.size(); ++ index) {
						Array.set(value, index, values.get(index));
					}
				} else {
					value = values;
				}
			}
		} else if (mapping == BELONGS_TO) {
			argType = type;

			if (!include.contains(javaName)) {
				return null;
			}

			if (columnName.equals("")) {
				columnName = Util.toSnakeCase(javaName) + "_id";
			}

			if (!availableColumns.contains(columnName)) {
				return null;
			}

			if (argType == void.class) {
				argType = declType;
			}

			if (tableName.equals("")) {
				tableName = Util.getTableName(argType);
			}

			Object otherId = data.getObject(columnName);
			value = QueryBuilder.query(con).from(argType, tableName).find(otherId);
		} else {
			String sqlName = columnName;
			argType = type;

			if (sqlName.length() == 0) {
				sqlName = Util.toSnakeCase(javaName);
			}

			if (!availableColumns.contains(sqlName)) {
				return null;
			}

			if (argType == void.class) {
				argType = declType;
			}

			value = data.getObject(sqlName);
		}

		try {
			return argType.cast(value);
		} catch (ClassCastException e) {
			// didn't work, hope there's another setter with a compatible type
			return null;
		}
	}
	
	public void load(Object object) throws SQLException {
		Set<String> loaded = new HashSet<>();
		this.object = object;
		this.cls = object.getClass();
		this.fieldDefs = Util.getFields(cls);
		this.onlyDeclared = Util.getOnlyDeclared(cls);
		this.id = null;
		this.gotId = false;

		for (Method method : cls.getMethods()) {
			Class<?>[] argTypes;
			String name = method.getName();
			if (
					(argTypes = method.getParameterTypes()).length == 1 &&
					!method.isVarArgs() &&
					method.getDeclaringClass() != Object.class &&
					name.length() > 3 &&
					name.startsWith("set") &&
					Character.isUpperCase(name.charAt(3))) {
				String javaName = Character.toLowerCase(name.charAt(3)) + name.substring(4);

				if (loaded.contains(javaName)) {
					continue;
				}

				Object value = loadValue(javaName, argTypes[0], method);

				if (value != null) {
					try {
						method.invoke(object, value);
					} catch (IllegalAccessException | IllegalArgumentException e) {
						// didn't work, hope there's another setter with a compatible type
						continue;
					} catch (InvocationTargetException e) {
						throw new RuntimeException(e);
					}
					loaded.add(javaName);
				}
			}
		}

		for (java.lang.reflect.Field field : cls.getFields()) {
			if ((field.getModifiers() & Modifier.TRANSIENT) == 0) {
				String javaName = field.getName();

				if (loaded.contains(javaName)) {
					continue;
				}

				Object value = loadValue(javaName, field.getType(), field);

				if (value != null) {
					try {
						field.set(object, value);
					} catch (IllegalAccessException e) {
						throw new RuntimeException(e);
					}
					loaded.add(javaName);
				}
			}
		}
		
	}
}