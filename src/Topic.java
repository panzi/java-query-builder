import static io.github.panzi.sql.annotations.Mapping.BELONGS_TO;

import io.github.panzi.sql.annotations.Field;
import io.github.panzi.sql.annotations.Meta;

@Meta(
	fields = {
		@Field(name = "owner", mapping = BELONGS_TO, columnName = "created_by_id")
	}
)
public class Topic {
	private long id;
	private String name;
	private User owner;

	public Topic() {
	}
	
	public User getOwner() {
		return owner;
	}
	
	public void setOwner(User owner) {
		this.owner = owner;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
