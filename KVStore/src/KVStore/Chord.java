package KVStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class Chord {

	private SortedMap<Integer, String> chord = new TreeMap<Integer, String>();
	private List<String> indexs = new ArrayList<String>();

	public Chord(Collection<String> nodes) {
		System.out.println("chord initiated ");
		for (String node : nodes) {
			this.chord.put(node.hashCode(), node);
			this.indexs.add(node);
		}
		for (Integer key : this.chord.keySet()) {
			System.out.println("chord contains : " + this.chord.get(key));
		}
	}

	public String getNodeByIndex(int index) {
		if (this.indexs.size() <= index)
			return this.indexs.get(index);

		else
			return this.indexs.get(0);
	}

	public SortedMap<Integer, String> getChord() {
		return this.chord;
	}

	public void join(String hostName) {
		this.chord.put(hostName.hashCode(), hostName);
		for (Integer key : this.chord.keySet()) {
			System.out.println("chord contains : " + this.chord.get(key));
		}

	}

	public void leave(String hostName) {
		if (this.chord.containsKey(hostName.hashCode()))
			this.chord.remove(hostName.hashCode());

		for (Integer key : this.chord.keySet()) {
			System.out.println("chord contains : " + this.chord.get(key));
		}
		System.out.println("left : " + hostName);
	}
}
