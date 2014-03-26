package KVStore;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class Chord {

	private SortedMap<Integer, String> chord = new TreeMap<Integer, String>();
	private List<String> indexs = new ArrayList<String>();

	public Chord(List<String> nodes) {
		this.indexs = nodes;
		for (String node : nodes) {
			this.chord.put(node.hashCode(), node);
		}
		for (Integer key : this.chord.keySet()) {
			System.out.println("chord contains : " + this.chord.get(key));
		}
	}

	public void partition(List<String> nodes) {
		Integer hashSpace = (int) Math.pow(2, 32);
		Integer nodePartitionSpace = hashSpace / nodes.size();

		int minHash = -(int) Math.pow(2, 31);
		int maxHash = (int) Math.pow(2, 31) - 1;

		for (String node : nodes) {
			minHash = minHash * nodes.indexOf(node) + 1;
			for (int i = minHash; i < minHash + nodePartitionSpace; i++) {
				this.chord.put(i, node);
			}
		}

		System.out.println("chord hashspace : " + maxHash + " contains: "
				+ this.chord.get(maxHash));
	}

	public String getNodeByIndex(int index) {
		if (this.indexs.size() > index)
			return this.indexs.get(index);

		else
			return this.indexs.get(0);
	}

	public SortedMap<Integer, String> getChord() {
		return this.chord;
	}

	public void join(String hostName) {
		this.chord.put(hostName.hashCode(), hostName);
		this.indexs.add(hostName.trim());

		for (Integer key : this.chord.keySet()) {
			System.out.println("chord contains : " + this.chord.get(key));
		}
	}

	public void leave(Integer hostNameHashCode) {
		if (this.chord.containsKey(hostNameHashCode)) {
			this.indexs.remove(this.chord.get(hostNameHashCode).trim());
			this.chord.remove(hostNameHashCode);
		}

		for (Integer key : this.chord.keySet()) {
			System.out.println("chord contains : " + this.chord.get(key));
		}

		for (String host : this.indexs) {
			System.out.println(host);
		}
	}
}