package buaa.act.ucar.datasimu.hbase;

import java.util.List;
import java.util.SortedMap;

public class KeyValue {
	private String key;
	private SortedMap<Long, List<String>> value;

	public KeyValue(String key, SortedMap<Long, List<String>> value) {
		this.setKey(key);
		this.setValue(value);
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public SortedMap<Long, List<String>> getValue() {
		return value;
	}

	public void setValue(SortedMap<Long, List<String>> value) {
		this.value = value;
	}
}
