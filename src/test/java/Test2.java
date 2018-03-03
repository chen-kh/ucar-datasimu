import net.sf.json.JSONObject;

public class Test2 {
	public static void main(String[] args) {
		JSONObject jo = new JSONObject();
		jo.put("a", "a");
		jo.getString("b");
	}
}
