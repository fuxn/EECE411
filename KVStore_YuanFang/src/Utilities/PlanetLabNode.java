package Utilities;

import java.util.SortedMap;
import java.util.TreeMap;

public class PlanetLabNode {

	private SortedMap<Integer, Object> values = new TreeMap<Integer, Object>();

	public void put(Object key, Object value) {
		this.values.put(key.hashCode(), value);
	}

	public Object get(Object key) {
		return this.values.get(key.hashCode());
	}

	public void remove(Object key) {
		this.values.remove(key.hashCode());
	}

}
