package single;

import java.util.HashMap;
import java.util.Map;

public class Pref {

	private String uid;
	private Map<String,Double> map = new HashMap<String,Double>();
	
	public Pref (String uid, Map<String,Double> map) {
		this.uid=uid;
		this.map=map;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public Map<String, Double> getMap() {
		return map;
	}

	public void setMap(Map<String, Double> map) {
		this.map = map;
	}
	
}
