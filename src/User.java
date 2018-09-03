import java.util.List;

import io.github.panzi.sql.annotations.Field;
import io.github.panzi.sql.annotations.Meta;
import static io.github.panzi.sql.annotations.Mapping.*;

@Meta(
	fields = {
		@Field(name = "bla", mapping = IGNORE),
		@Field(name = "topics", mapping = HAS_MANY, columnName = "created_by_id", type = Topic.class)
	}
)
public class User {
	private long id;
	private String screenname;
	private List<Topic> topics;
	private transient String bla;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getScreenname() {
		return screenname;
	}

	public void setScreenname(String screenname) {
		this.screenname = screenname;
	}
	
	public List<Topic> getTopics() {
		return topics;
	}
	
	public void setTopics(List<Topic> topics) {
		this.topics = topics;
	}

	public String getBla() {
		return bla;
	}

	public void setBla(String bla) {
		this.bla = bla;
	}

	@Override
	public String toString() {
		String ts;
		if (topics == null) {
			ts = "null";
		} else {
			StringBuilder buf = new StringBuilder();
			buf.append('[');
			boolean first = true;
			for (Topic topic : topics) {
				if (first) {
					first = false;
				} else {
					buf.append(", ");
				}
				buf.append(topic.getName());
			}
			buf.append(']');
			ts = buf.toString();
		}
		return String.format("{id: %d, screenname: %s, bla: %s, topics: %s}", id, screenname, bla, ts);
	}
}
