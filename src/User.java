
public class User {
	public long id;
	public String screenname;
	public transient String bla;
	
	@Override
	public String toString() {
		return String.format("{id: %d, screenname: %s, bla: %s}", id, screenname, bla);
	}
}
