package KVStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Chord {

	private SortedMap<Integer, String> chord = new TreeMap<Integer, String>();
	private ArrayList<String> indexs = new ArrayList<String>();
	private Map<String, String> participatingNode = new HashMap<String, String>();

	public Chord(Collection<String> nodes) {
		for (String node : nodes) {
			this.chord.put(node.hashCode(), node);
		}
		for (Integer key : this.chord.keySet()) {
			System.out.println("chord contains : " + this.chord.get(key));
		}
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

	public Map<String, String> getParticipatingNode() {
		return this.participatingNode;
	}

	public void join(String ipAddress, String hostName) {
		this.participatingNode.put(ipAddress, hostName);

		for (String key : this.participatingNode.keySet()) {
			System.out.println("chord contains : "
					+ this.participatingNode.get(key) + " @ " + key);
		}
		this.indexs.add(ipAddress);
	}

	public void leave(String ipAddress) {
		if (this.participatingNode.containsKey(ipAddress))
			this.participatingNode.remove(ipAddress);

		for (String key : this.participatingNode.keySet()) {
			System.out.println("chord contains : "
					+ this.participatingNode.get(key) + " @ " + key);
		}
		this.indexs.remove(ipAddress);
	}
}