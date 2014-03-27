package KVStore;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class Chord {

	private SortedMap<Integer, String> chord = new TreeMap<Integer, String>();
	private List<String> participantingNodes = new ArrayList<String>();

	public Chord(List<String> nodes) {
		this.participantingNodes = nodes;
		for (String node : nodes) {
			this.chord.put(node.hashCode(), node);
		}
		for (Integer key : this.chord.keySet()) {
			System.out.println("chord contains : " + this.chord.get(key));
		}
	}

	public String getNodeByIndex(int index) {
		if (this.participantingNodes.size() > index)
			return this.participantingNodes.get(index);

		else
			return this.participantingNodes.get(0);
	}

	public List<String> getAllNodes() {
		return this.participantingNodes;
	}

	public SortedMap<Integer, String> getChord() {
		return this.chord;
	}

	public void leave(Integer hostNameHashCode) {
		if (this.chord.containsKey(hostNameHashCode)) {
			this.participantingNodes.remove(this.chord.get(hostNameHashCode).trim());
			this.chord.remove(hostNameHashCode);
		}
		System.out.println(participantingNodes);
	}

	/*
	 * public void join(String hostName) { this.chord.put(hostName.hashCode(),
	 * hostName); this.indexs.add(hostName.trim()); }
	 */

	/*
	 * public void partition(List<String> nodes) { Integer hashSpace = (int)
	 * Math.pow(2, 32); Integer nodePartitionSpace = hashSpace / nodes.size();
	 * 
	 * int minHash = -(int) Math.pow(2, 31); int maxHash = (int) Math.pow(2, 31)
	 * - 1;
	 * 
	 * for (String node : nodes) { minHash = minHash * nodes.indexOf(node) + 1;
	 * for (int i = minHash; i < minHash + nodePartitionSpace; i++) {
	 * this.chord.put(i, node); } }
	 * 
	 * System.out.println("chord hashspace : " + maxHash + " contains: " +
	 * this.chord.get(maxHash)); }
	 */
}