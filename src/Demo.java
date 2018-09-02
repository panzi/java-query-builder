import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.github.panzi.sql.QueryBuilder;
import io.github.panzi.sql.SelectBuilder;

import static io.github.panzi.sql.QueryBuilder.query;

public class Demo {
	public static void main(String[] args) throws SQLException, IOException {
		Properties props = new Properties();
		props.load(Demo.class.getClassLoader().getResourceAsStream("config.properties"));
		
		String url = props.getProperty("url");
		Connection con = DriverManager.getConnection(url, props);
		
		QueryBuilder query = query(con);

		SelectBuilder<User> q = query.
				from(User.class).
				columns("id", "screenname").
				whereIs("screenname", "Mathias Panzenböck").
				where("id > ?", 2).
				limit(3).
				order("id");
		System.out.println(q.toSQL());
		
		try (ResultSet rs = q.execute()) {
			while (rs.next()) {
				System.out.printf("%5d %s\n", rs.getLong("id"), rs.getString("screenname"));
			}
		}
		
		System.out.println(q.all());
		
		User user = new User();
		user.id = 17595;
		user.screenname = "Changed Screenname";
		user.bla = "ignored";

		System.out.println(query.toUpdateSQL(user));
		System.out.println(query.toInsertSQL(user));
		query.update(user);

		System.out.println(query.from(User.class).find(user.id));
		
		query.whereIs("id", user.id).into(User.class).set("screenname", "More Changes").update();
		System.out.println(query.from(User.class).find(user.id));

		Map<String, Object> values = new HashMap<>();
		values.put("screenname", "Mathias Panzenböck");

		System.out.println(query.whereIs("id", user.id).toUpdateSQL(User.class, values));
		query.whereIs("id", user.id).update(User.class, values);

		System.out.println(query.from(User.class).findAll(user.id));
		
		q = query.from(User.class).whereIs("screenname", "Mathias Panzenböck").order("id");
		System.out.println("USER: " + q.columns("id").first());
		System.out.println("ID: " + q.columns("id").first(long.class));
		System.out.println(q.columns("screenname").toSQL());
		System.out.println("SCREENNAME: " + q.columns("screenname").first(String.class));
		System.out.println(join(q.first(Object[].class)));
		System.out.println(join(q.first(String[].class)));
	}
	
	static private<T> String join(T[] array) {
		StringBuilder buf = new StringBuilder();
		buf.append('[');
		if (array.length > 0) {
			buf.append(array[0]);
			for (int i = 1; i < array.length; ++ i) {
				buf.append(", ");
				buf.append(array[i]);
			}
		}
		buf.append(']');
		return buf.toString();
	}
}
